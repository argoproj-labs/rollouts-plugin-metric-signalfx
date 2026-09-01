package plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	"github.com/signalfx/signalflow-client-go/v2/signalflow/messages"
	log "github.com/sirupsen/logrus"
)

const (
	streamMargin = 10 * time.Second
	drainTimeout = 2 * time.Second
)

func newSignalFlowClient(config Config, logCtx log.Entry) (*signalflow.Client, error) {
	var streamParam signalflow.ClientParam
	if config.StreamURL != "" {
		streamParam = signalflow.StreamURL(config.StreamURL)
	} else {
		streamParam = signalflow.StreamURLForRealm(config.Realm)
	}

	client, err := signalflow.NewClient(
		streamParam,
		signalflow.AccessToken(config.AccessToken),
		signalflow.OnError(func(err error) {
			logCtx.WithError(err).Error("SignalFlow client error")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create SignalFlow client: %w", err)
	}

	return client, nil
}

func payloadValues(message *messages.DataMessage) ([]float64, error) {
	values := make([]float64, 0, len(message.Payloads))
	for _, payload := range message.Payloads {
		var value float64
		switch payload.Type {
		case messages.ValTypeDouble:
			value = payload.Float64()
		case messages.ValTypeLong:
			value = float64(payload.Int64())
		case messages.ValTypeInt:
			value = float64(payload.Int32())
		default:
			return nil, fmt.Errorf("unsupported SignalFlow value type %v", payload.Type)
		}

		if !isFinite(value) {
			return nil, fmt.Errorf("query returned a non-finite value")
		}

		values = append(values, value)
	}

	return values, nil
}

func collectSignalFlow(ctx context.Context, client *signalflow.Client, config Config, logCtx log.Entry) (float64, error) {
	streamDuration := time.Duration(config.Duration) * time.Second
	streamCtx, cancel := context.WithTimeout(ctx, streamDuration+streamMargin)
	defer cancel()

	aggregation, err := newAggregationState(config.Aggregator)
	if err != nil {
		return 0, err
	}

	comp, err := client.Execute(streamCtx, &signalflow.ExecuteRequest{
		Program: config.Query,
	})
	if err != nil {
		return 0, fmt.Errorf("could not execute SignalFlow program: %w", err)
	}

	stopTimer := time.NewTimer(streamDuration)
	defer stopTimer.Stop()

	process := func(message *messages.DataMessage) error {
		values, err := payloadValues(message)
		if err != nil {
			return err
		}
		for _, value := range values {
			if err := aggregation.add(value); err != nil {
				return err
			}
		}
		return nil
	}

	dataCh := comp.Data()

	timedOut := func() (float64, error) {
		if err := stopAndDrain(comp, nil, logCtx); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("stream completed before the configured window: %w", streamCtx.Err())
	}

loop:
	for {
		select {
		case <-streamCtx.Done():
			return timedOut()
		default:
		}

		select {
		case <-streamCtx.Done():
			return timedOut()
		case <-stopTimer.C:
			if err := stopAndDrain(comp, process, logCtx); err != nil {
				return 0, err
			}
			break loop
		case message, ok := <-dataCh:
			if !ok {
				if compErr := comp.Err(); compErr != nil {
					return 0, compErr
				}
				return 0, fmt.Errorf("SignalFlow stream closed before the configured window")
			}
			if err := process(message); err != nil {
				_ = stopAndDrain(comp, nil, logCtx)
				return 0, err
			}
		}
	}

	return aggregation.result()
}

func stopAndDrain(comp *signalflow.Computation, process func(*messages.DataMessage) error, logCtx log.Entry) error {
	stopCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	if err := comp.Stop(stopCtx); err != nil {
		logCtx.WithError(err).Info("failed to stop SignalFlow computation")
	}

	dataCh := comp.Data()
	for {
		select {
		case message, ok := <-dataCh:
			if !ok {
				return comp.Err()
			}
			if process != nil {
				if err := process(message); err != nil {
					startBackgroundDataDrain(dataCh)
					return err
				}
			}
		case <-stopCtx.Done():
			logCtx.Info("gave up draining SignalFlow data channel after stop")
			startBackgroundDataDrain(dataCh)
			return comp.Err()
		}
	}
}

func startBackgroundDataDrain(dataCh <-chan *messages.DataMessage) {
	go func() {
		for range dataCh {
		}
	}()
}

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
	comp, err := client.Execute(ctx, &signalflow.ExecuteRequest{
		Program: config.Query,
	})
	if err != nil {
		return 0, fmt.Errorf("could not execute SignalFlow program: %w", err)
	}

	streamDuration := time.Duration(config.Duration) * time.Second
	streamCtx, cancel := context.WithTimeout(ctx, streamDuration+streamMargin)
	defer cancel()

	stopTimer := time.NewTimer(streamDuration)
	defer stopTimer.Stop()

	var values []float64

	process := func(message *messages.DataMessage) error {
		converted, err := payloadValues(message)
		if err != nil {
			return err
		}
		values = append(values, converted...)
		return nil
	}

	dataCh := comp.Data()

	timedOut := func() (float64, error) {
		_ = stopAndDrain(comp, nil, logCtx)
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
				break loop
			}
			if err := process(message); err != nil {
				_ = stopAndDrain(comp, nil, logCtx)
				return 0, err
			}
		}
	}

	return aggregate(values, config.Aggregator)
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
		case <-stopCtx.Done():
			logCtx.Info("gave up draining SignalFlow data channel after stop")
			return nil
		default:
		}

		select {
		case message, ok := <-dataCh:
			if !ok {
				return nil
			}
			if process != nil {
				if err := process(message); err != nil {
					return err
				}
			}
		case <-stopCtx.Done():
			logCtx.Info("gave up draining SignalFlow data channel after stop")
			return nil
		}
	}
}

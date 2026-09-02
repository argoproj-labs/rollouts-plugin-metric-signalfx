package plugin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	"github.com/signalfx/signalflow-client-go/v2/signalflow/messages"
	log "github.com/sirupsen/logrus"
)

const (
	streamMargin             = 10 * time.Second
	drainTimeout             = 2 * time.Second
	maxDurationSeconds int64 = (1<<63 - 1 - int64(streamMargin)) / int64(time.Second)
)

var errComputationDrainTimeout = errors.New("timed out draining SignalFlow computation")

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
		if err := stopAndDrain(comp, nil, logCtx); err != nil && !errors.Is(err, errComputationDrainTimeout) {
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
					return 0, fmt.Errorf("SignalFlow stream closed before the configured window: %w", compErr)
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

	processErr := make(chan error, 1)
	drainErr := make(chan error, 1)
	go func() {
		drainErr <- drainComputation(comp, process, processErr)
	}()

	if err := comp.Stop(stopCtx); err != nil {
		logCtx.WithError(err).Info("failed to stop SignalFlow computation")
	}

	select {
	case err := <-processErr:
		return err
	case err := <-drainErr:
		return expectedStopDrainError(err)
	case <-stopCtx.Done():
		select {
		case err := <-processErr:
			return err
		case err := <-drainErr:
			return expectedStopDrainError(err)
		default:
		}
		logCtx.Info("gave up draining SignalFlow computation after stop")
		return fmt.Errorf("%w after %s", errComputationDrainTimeout, drainTimeout)
	}
}

func expectedStopDrainError(err error) error {
	var abortErr *signalflow.ChannelAbortError
	if errors.As(err, &abortErr) && abortErr.State == "STOPPED" {
		return nil
	}
	return err
}

func drainComputation(comp *signalflow.Computation, process func(*messages.DataMessage) error, processErr chan<- error) error {
	dataCh := comp.Data()
	infoCh := comp.Info()
	eventCh := comp.Events()
	expirationCh := comp.Expirations()
	var firstProcessErr error

	for dataCh != nil || infoCh != nil || eventCh != nil || expirationCh != nil {
		select {
		case message, ok := <-dataCh:
			if !ok {
				dataCh = nil
				continue
			}
			if process == nil || firstProcessErr != nil {
				continue
			}
			firstProcessErr = process(message)
			if firstProcessErr != nil {
				processErr <- firstProcessErr
			}
		case _, ok := <-infoCh:
			if !ok {
				infoCh = nil
			}
		case _, ok := <-eventCh:
			if !ok {
				eventCh = nil
			}
		case _, ok := <-expirationCh:
			if !ok {
				expirationCh = nil
			}
		}
	}

	if firstProcessErr != nil {
		return firstProcessErr
	}
	return comp.Err()
}

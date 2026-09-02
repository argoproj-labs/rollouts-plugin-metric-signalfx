// Copyright Splunk Inc.
// SPDX-License-Identifier: Apache-2.0

package signalflow

import (
	"strings"
	"testing"
	"time"

	"github.com/signalfx/signalflow-client-go/v2/signalflow/messages"
)

func TestComputationRejectsMalformedErrorPayload(t *testing.T) {
	channel := make(chan messages.Message)
	computation := newComputation(channel, "ch-1", &Client{})

	message, err := messages.ParseMessage([]byte(`{"type":"error","channel":"ch-1","error":null}`), true)
	if err != nil {
		t.Fatal(err)
	}
	channel <- message

	deadline := time.Now().Add(time.Second)
	for computation.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if computation.Err() == nil {
		t.Fatal("malformed error payload did not stop the computation")
	}

	close(channel)
	if !strings.Contains(computation.Err().Error(), "invalid SignalFlow error") {
		t.Fatalf("computation error = %v, want malformed error message", computation.Err())
	}
}

func TestComputationRejectsMalformedGroupByMissingProperty(t *testing.T) {
	channel := make(chan messages.Message, 1)
	computation := newComputation(channel, "ch-1", &Client{})

	message, err := messages.ParseMessage([]byte(`{"type":"message","channel":"ch-1","message":{"messageCode":"GROUPBY_MISSING_PROPERTY","contents":{"propertyNames":[42]}}}`), true)
	if err != nil {
		t.Fatal(err)
	}
	channel <- message

	deadline := time.Now().Add(time.Second)
	for computation.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if computation.Err() == nil {
		t.Fatal("malformed GROUPBY_MISSING_PROPERTY payload did not stop the computation")
	}
	if !strings.Contains(computation.Err().Error(), "invalid SignalFlow") {
		t.Fatalf("computation error = %v, want malformed protocol error", computation.Err())
	}
}

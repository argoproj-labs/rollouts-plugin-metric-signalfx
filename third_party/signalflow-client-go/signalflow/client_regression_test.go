// Copyright Splunk Inc.
// SPDX-License-Identifier: Apache-2.0

package signalflow

import (
	"context"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/signalfx/signalflow-client-go/v2/signalflow/messages"
)

func TestHandleMessageDoesNotRetainClientLockForUnknownChannel(t *testing.T) {
	client := &Client{channelsByName: make(map[string]chan messages.Message)}
	message := []byte(`{"type":"control-message","event":"JOB_PROGRESS","channel":"missing"}`)

	if err := client.handleMessage(message, websocket.TextMessage); err != nil {
		t.Fatalf("handleMessage() error = %v", err)
	}

	unlocked := make(chan struct{})
	go func() {
		client.Lock()
		client.Unlock()
		close(unlocked)
	}()

	select {
	case <-unlocked:
	case <-time.After(time.Second):
		client.Unlock()
		t.Fatal("handleMessage() retained the client lock")
	}
}

func TestDetachHonorsContextAfterMessageHandoff(t *testing.T) {
	client := &Client{outgoingTextMsgs: make(chan *outgoingMessage)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- client.Detach(ctx, &DetachRequest{Channel: "ch-1"})
	}()

	select {
	case <-client.outgoingTextMsgs:
	case <-time.After(time.Second):
		t.Fatal("Detach() did not hand off its message")
	}
	cancel()

	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("Detach() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Detach() did not observe cancellation after handoff")
	}
}

func TestExecuteRegistersChannelBeforeSendingRequest(t *testing.T) {
	client := &Client{
		channelsByName:   make(map[string]chan messages.Message),
		outgoingTextMsgs: make(chan *outgoingMessage),
	}

	go func() {
		message := <-client.outgoingTextMsgs
		if err := client.handleMessage([]byte(`{"type":"control-message","event":"JOB_START","channel":"ch-1","handle":"handle-1"}`), websocket.TextMessage); err != nil {
			t.Errorf("handleMessage() error = %v", err)
		}
		message.resultCh <- nil
	}()

	computation, err := client.Execute(context.Background(), &ExecuteRequest{Channel: "ch-1"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	handle, err := computation.Handle(ctx)
	if err != nil {
		t.Fatalf("Computation.Handle() error = %v", err)
	}
	if handle != "handle-1" {
		t.Fatalf("Computation.Handle() = %q, want handle-1", handle)
	}
}

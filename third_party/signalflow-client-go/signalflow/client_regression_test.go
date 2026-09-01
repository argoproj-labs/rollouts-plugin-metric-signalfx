// Copyright Splunk Inc.
// SPDX-License-Identifier: Apache-2.0

package signalflow

import (
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

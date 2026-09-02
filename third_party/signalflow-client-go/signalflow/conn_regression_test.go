// Copyright Splunk Inc.
// SPDX-License-Identifier: Apache-2.0

package signalflow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSConnStopsForwardingWhenContextIsCancelled(t *testing.T) {
	sent := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := (&websocket.Upgrader{}).Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if err := connection.WriteMessage(websocket.TextMessage, []byte(`{"type":"authenticated"}`)); err != nil {
			return
		}
		close(sent)
		<-request.Context().Done()
	}))
	defer server.Close()

	streamURL, err := url.Parse(strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &wsConn{
		StreamURL:          streamURL,
		OutgoingTextMsgs:   make(chan *outgoingMessage),
		IncomingTextMsgs:   make(chan []byte),
		IncomingBinaryMsgs: make(chan []byte),
		ConnectTimeout:     time.Second,
		ReadTimeout:        time.Minute,
		WriteTimeout:       time.Second,
	}
	done := make(chan struct{})
	go func() {
		conn.Run(ctx)
		close(done)
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("websocket server did not send a message")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("websocket connection did not stop after context cancellation")
	}
}

func TestWSConnCancellationInterruptsReconnectDelay(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	server.Close()

	streamURL, err := url.Parse(strings.Replace(server.URL, "http://", "ws://", 1))
	if err != nil {
		t.Fatal(err)
	}
	oldReconnectDelay := reconnectDelay
	reconnectDelay = 500 * time.Millisecond
	defer func() { reconnectDelay = oldReconnectDelay }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connectionError := make(chan struct{}, 1)
	conn := &wsConn{
		StreamURL:          streamURL,
		OutgoingTextMsgs:   make(chan *outgoingMessage),
		IncomingTextMsgs:   make(chan []byte),
		IncomingBinaryMsgs: make(chan []byte),
		ConnectTimeout:     time.Second,
		ReadTimeout:        time.Second,
		WriteTimeout:       time.Second,
		OnError: func(error) {
			connectionError <- struct{}{}
		},
	}
	done := make(chan struct{})
	go func() {
		conn.Run(ctx)
		close(done)
	}()

	select {
	case <-connectionError:
	case <-time.After(time.Second):
		t.Fatal("websocket connection error was not reported")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reconnect delay ignored context cancellation")
	}
}

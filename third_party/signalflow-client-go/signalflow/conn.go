// Copyright Splunk Inc.
// SPDX-License-Identifier: Apache-2.0

package signalflow

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/gorilla/websocket"
)

// How long to wait between connections in case of a bad connection.
var reconnectDelay = 5 * time.Second

type wsConn struct {
	StreamURL *url.URL

	OutgoingTextMsgs   chan *outgoingMessage
	IncomingTextMsgs   chan []byte
	IncomingBinaryMsgs chan []byte
	ConnectedCh        chan struct{}

	ConnectTimeout         time.Duration
	ReadTimeout            time.Duration
	WriteTimeout           time.Duration
	OnError                OnErrorFunc
	PostDisconnectCallback func()
	PostConnectMessage     func() []byte
}

type outgoingMessage struct {
	bytes    []byte
	resultCh chan error
}

// Run keeps the connection alive and puts all incoming messages into a channel
// as needed.
func (c *wsConn) Run(ctx context.Context) {
	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for {
		if conn != nil {
			conn.Close()
			select {
			case <-time.After(reconnectDelay):
			case <-ctx.Done():
				return
			}
		}
		// This will get run on before the first connection as well.
		if c.PostDisconnectCallback != nil {
			c.PostDisconnectCallback()
		}

		if ctx.Err() != nil {
			return
		}

		dialCtx, cancel := context.WithTimeout(ctx, c.ConnectTimeout)
		var err error
		conn, err = c.connect(dialCtx)
		cancel()
		if err != nil {
			c.sendErrIfWanted(fmt.Errorf("Error connecting to SignalFlow websocket: %w", err))
			continue
		}

		err = c.postConnect(conn)
		if err != nil {
			c.sendErrIfWanted(fmt.Errorf("Error setting up SignalFlow websocket: %w", err))
			continue
		}

		err = c.readAndWriteMessages(ctx, conn)
		if err == nil || ctx.Err() != nil {
			return
		}
		c.sendErrIfWanted(fmt.Errorf("Error in SignalFlow websocket: %w", err))
	}
}

type messageWithType struct {
	bytes   []byte
	msgType int
}

func (c *wsConn) readAndWriteMessages(ctx context.Context, conn *websocket.Conn) error {
	readMessageCh := make(chan messageWithType)
	readErrCh := make(chan error)

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	connectionDone := make(chan struct{})
	defer close(connectionDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-connectionDone:
		}
	}()

	go func() {
		for {
			bytes, typ, err := readNextMessage(conn, c.ReadTimeout)
			if err != nil {
				select {
				case readErrCh <- err:
				case <-readCtx.Done():
				}
				return
			}
			select {
			case readMessageCh <- messageWithType{
				bytes:   bytes,
				msgType: typ,
			}:
			case <-readCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-readMessageCh:
			if !ok {
				return nil
			}
			if msg.msgType == websocket.TextMessage {
				select {
				case c.IncomingTextMsgs <- msg.bytes:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				select {
				case c.IncomingBinaryMsgs <- msg.bytes:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		case err := <-readErrCh:
			return err
		case msg, ok := <-c.OutgoingTextMsgs:
			if !ok {
				return nil
			}
			err := c.writeMessage(conn, msg.bytes)
			select {
			case msg.resultCh <- err:
			case <-ctx.Done():
				return ctx.Err()
			}
			if err != nil {
				return err
			}
		}
	}
}

func (c *wsConn) sendErrIfWanted(err error) {
	if c.OnError != nil {
		c.OnError(err)
	}
}

func (c *wsConn) Close() {
	close(c.IncomingTextMsgs)
	close(c.IncomingBinaryMsgs)
}

func (c *wsConn) connect(ctx context.Context) (*websocket.Conn, error) {
	connectURL := *c.StreamURL
	connectURL.Path = path.Join(c.StreamURL.Path, "connect")
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, connectURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not connect Signalflow websocket: %w", err)
	}
	return conn, nil
}

func (c *wsConn) postConnect(conn *websocket.Conn) error {
	if c.PostConnectMessage != nil {
		msg := c.PostConnectMessage()
		if msg != nil {
			return c.writeMessage(conn, msg)
		}
	}
	return nil
}

func readNextMessage(conn *websocket.Conn, timeout time.Duration) (data []byte, msgType int, err error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, 0, fmt.Errorf("could not set read timeout in SignalFlow client: %w", err)
	}

	typ, bytes, err := conn.ReadMessage()
	if err != nil {
		return nil, 0, err
	}
	return bytes, typ, nil
}

func (c *wsConn) writeMessage(conn *websocket.Conn, msgBytes []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout))
	if err != nil {
		return fmt.Errorf("could not set write timeout for SignalFlow request: %w", err)
	}

	err = conn.WriteMessage(websocket.TextMessage, msgBytes)
	if err != nil {
		return err
	}
	return nil
}

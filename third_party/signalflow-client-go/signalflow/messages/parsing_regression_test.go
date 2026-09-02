// Copyright Splunk Inc.
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"encoding/binary"
	"testing"
)

func TestParseBinaryMessageRejectsMalformedFrames(t *testing.T) {
	fullChannel := make([]byte, 21)
	for index := 4; index < 20; index++ {
		fullChannel[index] = 'x'
	}

	shortPayload := make([]byte, 21)
	truncatedElement := make([]byte, 32)
	binary.BigEndian.PutUint32(truncatedElement[28:], 1)

	tests := []struct {
		name  string
		frame []byte
	}{
		{name: "channel without terminator", frame: fullChannel},
		{name: "short data header", frame: shortPayload},
		{name: "short data payload", frame: truncatedElement},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseMessage(test.frame, false); err == nil {
				t.Fatal("ParseMessage() returned nil error")
			}
		})
	}
}

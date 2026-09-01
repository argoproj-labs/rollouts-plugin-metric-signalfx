package plugin

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	"github.com/signalfx/signalflow-client-go/v2/signalflow/messages"
	"github.com/signalfx/signalfx-go/idtool"
	log "github.com/sirupsen/logrus"
)

func TestPayloadValues(t *testing.T) {
	message := &messages.DataMessage{Payloads: []messages.DataPayload{
		doublePayload(1.5),
		longPayload(2),
		intPayload(-3),
	}}

	got, err := payloadValues(message)
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{1.5, 2, -3}
	if len(got) != len(want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("values = %v, want %v", got, want)
		}
	}
}

func TestPayloadValuesRejectsUnsupportedAndNonFiniteValues(t *testing.T) {
	tests := []struct {
		name    string
		payload messages.DataPayload
	}{
		{name: "unsupported type", payload: messages.DataPayload{Type: messages.ValType(99)}},
		{name: "NaN", payload: doublePayload(math.NaN())},
		{name: "infinity", payload: doublePayload(math.Inf(1))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := payloadValues(&messages.DataMessage{Payloads: []messages.DataPayload{test.payload}})
			if err == nil {
				t.Fatal("payloadValues() returned nil error")
			}
		})
	}
}

func TestCollectSignalFlowAggregatesAndStopsFakeStream(t *testing.T) {
	const program = "data('demo').publish()"
	client, fake := newFakeClient(t, program, map[idtool.ID]float64{
		idtool.ID(1): 10,
		idtool.ID(2): 20,
		idtool.ID(3): 30,
	})
	defer closeFakeClient(client, fake)

	got, err := collectSignalFlow(context.Background(), client, Config{
		Query: program, Duration: 2, Aggregator: "max",
	}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if got != 30 {
		t.Fatalf("collectSignalFlow() = %v, want 30", got)
	}

	deadline := time.Now().Add(time.Second)
	for fake.RunningJobsForProgram(program) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if running := fake.RunningJobsForProgram(program); running != 0 {
		t.Fatalf("running jobs = %d, want 0", running)
	}
}

func TestCollectSignalFlowReturnsEmptyDataError(t *testing.T) {
	const program = "data('empty').publish()"
	client, fake := newFakeClient(t, program, nil)
	defer closeFakeClient(client, fake)

	_, err := collectSignalFlow(context.Background(), client, Config{
		Query: program, Duration: 1, Aggregator: "avg",
	}, testLogger())
	if err == nil || !strings.Contains(err.Error(), "query returned no data points") {
		t.Fatalf("error = %v, want empty-data error", err)
	}
}

func TestCollectSignalFlowReturnsComputationError(t *testing.T) {
	const program = "data('broken').publish()"
	server := newErrorSignalFlowServer()
	defer server.Close()

	client, err := signalflow.NewClient(
		signalflow.StreamURL(strings.Replace(server.URL, "http://", "ws://", 1)),
		signalflow.AccessToken("abcd"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = collectSignalFlow(context.Background(), client, Config{
		Query: program, Duration: 5, Aggregator: "avg",
	}, testLogger())
	var computationErr *signalflow.ComputationError
	if err == nil || !errors.As(err, &computationErr) || computationErr.Message != "synthetic SignalFlow failure" {
		t.Fatalf("error = %v, want execute error", err)
	}
}

func TestCollectSignalFlowReturnsAfterContextCancellation(t *testing.T) {
	const program = "data('slow').publish()"
	client, fake := newFakeClient(t, program, map[idtool.ID]float64{idtool.ID(1): 42})
	defer closeFakeClient(client, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := collectSignalFlow(ctx, client, Config{
		Query: program, Duration: 3600, Aggregator: "latest",
	}, testLogger())
	if err == nil || !strings.Contains(err.Error(), "stream completed before the configured window") {
		t.Fatalf("error = %v, want deadline error", err)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("collection returned after %v, want no more than 4s", elapsed)
	}
}

func newFakeClient(t *testing.T, program string, values map[idtool.ID]float64) (*signalflow.Client, *signalflow.FakeBackend) {
	t.Helper()
	fake := signalflow.NewRunningFakeBackend()
	client, err := fake.Client()
	if err != nil {
		fake.Stop()
		t.Fatal(err)
	}

	ids := make([]idtool.ID, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	fake.AddProgramTSIDs(program, ids)
	for id, value := range values {
		fake.SetTSIDFloatData(id, value)
	}
	return client, fake
}

func closeFakeClient(client *signalflow.Client, fake *signalflow.FakeBackend) {
	client.Close()
	fake.Stop()
}

func testLogger() log.Entry {
	logger := log.New()
	logger.SetOutput(io.Discard)
	return *logger.WithField("test", "signalflow")
}

func newErrorSignalFlowServer() *httptest.Server {
	upgrader := websocket.Upgrader{}
	return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()

		for {
			_, payload, err := connection.ReadMessage()
			if err != nil {
				return
			}
			var message struct {
				Type    string `json:"type"`
				Channel string `json:"channel"`
			}
			if err := json.Unmarshal(payload, &message); err != nil {
				return
			}
			switch message.Type {
			case "authenticate":
				_ = connection.WriteJSON(map[string]string{"type": "authenticated"})
			case "execute":
				_ = connection.WriteJSON(map[string]string{
					"type":    "error",
					"message": "synthetic SignalFlow failure",
					"channel": message.Channel,
				})
				return
			}
		}
	}))
}

func doublePayload(value float64) messages.DataPayload {
	payload := messages.DataPayload{Type: messages.ValTypeDouble}
	binary.BigEndian.PutUint64(payload.Val[:], math.Float64bits(value))
	return payload
}

func longPayload(value int64) messages.DataPayload {
	payload := messages.DataPayload{Type: messages.ValTypeLong}
	binary.BigEndian.PutUint64(payload.Val[:], uint64(value))
	return payload
}

func intPayload(value int32) messages.DataPayload {
	payload := messages.DataPayload{Type: messages.ValTypeInt}
	binary.BigEndian.PutUint32(payload.Val[:4], uint32(value))
	return payload
}

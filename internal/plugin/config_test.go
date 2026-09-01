package plugin

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

func TestParseConfigJSON(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		envToken string
		want     Config
		wantErr  string
	}{
		{
			name: "valid inline token",
			raw:  `{"query":"data('demo').publish()","realm":"us0","accessToken":"inline","duration":60,"aggregator":"latest"}`,
			want: Config{Query: "data('demo').publish()", Realm: "us0", AccessToken: "inline", Duration: 60, Aggregator: "latest"},
		},
		{
			name:     "environment token fallback",
			raw:      `{"query":"data('demo').publish()","realm":"us0","duration":60,"aggregator":"avg"}`,
			envToken: "from-env",
			want:     Config{Query: "data('demo').publish()", Realm: "us0", AccessToken: "from-env", Duration: 60, Aggregator: "avg"},
		},
		{
			name:     "inline token wins",
			raw:      `{"query":"data('demo').publish()","realm":"us0","accessToken":"inline","duration":60,"aggregator":"avg"}`,
			envToken: "from-env",
			want:     Config{Query: "data('demo').publish()", Realm: "us0", AccessToken: "inline", Duration: 60, Aggregator: "avg"},
		},
		{
			name: "stream URL without realm",
			raw:  `{"query":"data('demo').publish()","streamURL":"ws://127.0.0.1:8080/signalflow","accessToken":"inline","duration":60,"aggregator":"sum"}`,
			want: Config{Query: "data('demo').publish()", StreamURL: "ws://127.0.0.1:8080/signalflow", AccessToken: "inline", Duration: 60, Aggregator: "sum"},
		},
		{
			name:     "matching stream URL with environment token",
			raw:      `{"query":"data('demo').publish()","realm":"us0","streamURL":"wss://stream.us0.signalfx.com/v2/signalflow","duration":60,"aggregator":"sum"}`,
			envToken: "from-env",
			want:     Config{Query: "data('demo').publish()", Realm: "us0", StreamURL: "wss://stream.us0.signalfx.com/v2/signalflow", AccessToken: "from-env", Duration: 60, Aggregator: "sum"},
		},
		{name: "malformed JSON", raw: `{`, wantErr: "failed to parse plugin config"},
		{name: "missing query", raw: `{"realm":"us0","accessToken":"secret-value","duration":60,"aggregator":"avg"}`, wantErr: "config field 'query' is required"},
		{name: "blank query", raw: `{"query":"  ","realm":"us0","accessToken":"secret-value","duration":60,"aggregator":"avg"}`, wantErr: "config field 'query' is required"},
		{name: "missing token", raw: `{"query":"data('demo').publish()","realm":"us0","duration":60,"aggregator":"avg"}`, wantErr: "config field 'accessToken' or environment variable 'SIGNALFX_ACCESS_TOKEN' is required"},
		{name: "zero duration", raw: `{"query":"data('demo').publish()","realm":"us0","accessToken":"secret-value","duration":0,"aggregator":"avg"}`, wantErr: "config field 'duration' must be greater than zero"},
		{name: "negative duration", raw: `{"query":"data('demo').publish()","realm":"us0","accessToken":"secret-value","duration":-1,"aggregator":"avg"}`, wantErr: "config field 'duration' must be greater than zero"},
		{name: "unsupported aggregator", raw: `{"query":"data('demo').publish()","realm":"us0","accessToken":"secret-value","duration":60,"aggregator":"median"}`, wantErr: "config field 'aggregator' must be one of max, min, avg, sum, count, latest"},
		{name: "missing realm", raw: `{"query":"data('demo').publish()","accessToken":"secret-value","duration":60,"aggregator":"avg"}`, wantErr: "config field 'realm' is required when streamURL is empty"},
		{name: "environment token requires realm for custom stream URL", raw: `{"query":"data('demo').publish()","streamURL":"ws://127.0.0.1:8080/signalflow","duration":60,"aggregator":"sum"}`, envToken: "from-env", wantErr: "config field 'realm' is required when streamURL uses environment authentication"},
		{name: "environment token rejects unrelated stream URL", raw: `{"query":"data('demo').publish()","realm":"us0","streamURL":"wss://attacker.example/signalflow","duration":60,"aggregator":"sum"}`, envToken: "from-env", wantErr: "config field 'streamURL' host must match the SignalFlow host for realm"},
		{name: "realm rejects URL authority injection", raw: `{"query":"data('demo').publish()","realm":"us0.signalfx.com@attacker.example/","duration":60,"aggregator":"sum"}`, envToken: "from-env", wantErr: "config field 'realm' is invalid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseConfigJSON(json.RawMessage(test.raw), test.envToken)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error = %v, want substring %q", err, test.wantErr)
				}
				if strings.Contains(err.Error(), "secret-value") {
					t.Fatalf("error leaked access token: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("config = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestParseConfigProviderLookup(t *testing.T) {
	t.Run("missing provider map", func(t *testing.T) {
		_, err := parseConfig(v1alpha1.Metric{Name: "latency"})
		if err == nil || !strings.Contains(err.Error(), "has no plugin provider config") {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("missing provider key", func(t *testing.T) {
		metric := v1alpha1.Metric{
			Name: "latency",
			Provider: v1alpha1.MetricProvider{Plugin: map[string]json.RawMessage{
				"example/other": json.RawMessage(`{}`),
			}},
		}
		_, err := parseConfig(metric)
		if err == nil || !strings.Contains(err.Error(), `is missing provider.plugin["argoproj-labs/rollouts-plugin-metric-signalfx"] config`) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestValidateStreamURL(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "websocket", value: "ws://127.0.0.1:8080/signalflow"},
		{name: "secure websocket", value: "wss://stream.us0.signalfx.com/v2/signalflow"},
		{name: "HTTP", value: "http://example.com", wantErr: true},
		{name: "missing host", value: "wss:///signalflow", wantErr: true},
		{name: "malformed", value: "://bad", wantErr: true},
		{name: "user info", value: "wss://user@example.com/signalflow", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateStreamURL(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateStreamURL(%q) error = %v, wantErr %v", test.value, err, test.wantErr)
			}
		})
	}
}

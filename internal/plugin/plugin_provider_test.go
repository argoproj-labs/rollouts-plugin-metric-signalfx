package plugin

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	"github.com/signalfx/signalflow-client-go/v2/signalflow/messages"
	"github.com/signalfx/signalfx-go/idtool"
)

func fakeMetric(program, endpoint string, duration int, aggregator, success, failure string) v1alpha1.Metric {
	return v1alpha1.Metric{
		Name:             "latency",
		SuccessCondition: success,
		FailureCondition: failure,
		Provider: v1alpha1.MetricProvider{Plugin: map[string]json.RawMessage{
			PluginName: json.RawMessage(fmt.Sprintf(`{"query":%q,"accessToken":"abcd","duration":%d,"aggregator":%q,"streamURL":%q}`, program, duration, aggregator, endpoint)),
		}},
	}
}

func TestRunMapsPhases(t *testing.T) {
	const program = "data('demo').publish()"
	tests := []struct {
		name    string
		success string
		failure string
		want    v1alpha1.AnalysisPhase
	}{
		{name: "successful", success: "result >= 42", want: v1alpha1.AnalysisPhaseSuccessful},
		{name: "failed", failure: "result >= 42", want: v1alpha1.AnalysisPhaseFailed},
		{name: "inconclusive", success: "result > 100", failure: "result < 0", want: v1alpha1.AnalysisPhaseInconclusive},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := signalflow.NewRunningFakeBackend()
			defer fake.Stop()
			fake.AddProgramTSIDs(program, []idtool.ID{idtool.ID(1)})
			fake.AddTSIDMetadata(idtool.ID(1), &messages.MetadataProperties{ResolutionMS: 100})
			fake.SetTSIDFloatData(idtool.ID(1), 42)

			// Inject fake client via endpoint - but Run creates its own client via streamURL.
			// So we need to use streamURL that points to fake backend.
			metric := fakeMetric(program, fake.URL(), 1, "latest", tc.success, tc.failure)
			provider := &RpcPlugin{LogCtx: testLogger()}
			m := provider.Run(nil, metric)
			if m.Phase != tc.want {
				t.Fatalf("phase = %q, want %q, message %q value %q", m.Phase, tc.want, m.Message, m.Value)
			}
			if m.StartedAt == nil || m.FinishedAt == nil {
				t.Fatal("measurement timestamps missing")
			}
			if tc.want != v1alpha1.AnalysisPhaseError && m.Value != "42" {
				t.Fatalf("value = %q, want 42", m.Value)
			}
		})
	}
}

func TestRunReturnsErrorOnInvalidConfigAndEmptyData(t *testing.T) {
	provider := &RpcPlugin{LogCtx: testLogger()}
	invalid := v1alpha1.Metric{Name: "latency"}
	m := provider.Run(nil, invalid)
	if m.Phase != v1alpha1.AnalysisPhaseError {
		t.Fatalf("invalid config phase = %q, want Error", m.Phase)
	}
	if m.FinishedAt == nil {
		t.Fatal("finishedAt missing on error")
	}

	fake := signalflow.NewRunningFakeBackend()
	defer fake.Stop()
	const program = "data('empty').publish()"
	metric := fakeMetric(program, fake.URL(), 1, "latest", "", "")
	// No TSIDs added -> empty data
	m = provider.Run(nil, metric)
	if m.Phase != v1alpha1.AnalysisPhaseError || !strings.Contains(m.Message, "query returned no data points") {
		t.Fatalf("empty data phase=%q message=%q", m.Phase, m.Message)
	}
}

func TestGetMetadata(t *testing.T) {
	provider := &RpcPlugin{LogCtx: testLogger()}
	raw := json.RawMessage(`{"query":"data('demo').publish()","realm":"us0","accessToken":"secret","duration":60,"aggregator":"avg"}`)
	metric := v1alpha1.Metric{Provider: v1alpha1.MetricProvider{Plugin: map[string]json.RawMessage{PluginName: raw}}}
	meta := provider.GetMetadata(metric)
	if meta[ResolvedQueryKey] != "data('demo').publish()" {
		t.Fatalf("metadata = %#v", meta)
	}
	if _, ok := meta["accessToken"]; ok {
		t.Fatal("metadata leaked access token")
	}
	if _, ok := meta[AccessTokenEnv]; ok {
		t.Fatal("metadata leaked token env")
	}
	// error case
	empty := provider.GetMetadata(v1alpha1.Metric{})
	if _, ok := empty["error"]; !ok {
		t.Fatalf("expected error metadata for missing config, got %#v", empty)
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	signalfxPlugin "github.com/argoproj-labs/rollouts-plugin-metric-signalfx/internal/plugin"
	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	goPlugin "github.com/hashicorp/go-plugin"
	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	"github.com/signalfx/signalfx-go/idtool"
)

func TestBuiltPluginBinaryUsesFakeSignalFlow(t *testing.T) {
	const program = "data('demo').publish()"
	fake := signalflow.NewRunningFakeBackend()
	defer fake.Stop()
	fake.AddProgramTSIDs(program, []idtool.ID{idtool.ID(1)})
	fake.SetTSIDFloatData(idtool.ID(1), 42)

	binaryPath := buildPluginBinary(t)
	client, provider := startBinaryProvider(t, binaryPath)
	defer client.Kill()

	metric := v1alpha1.Metric{
		SuccessCondition: "result >= 42",
		Provider: v1alpha1.MetricProvider{Plugin: map[string]json.RawMessage{
			signalfxPlugin.PluginName: json.RawMessage(fmt.Sprintf(
				`{"query":%q,"streamURL":%q,"accessToken":"abcd","duration":2,"aggregator":"latest"}`,
				program, fake.URL())),
		}},
	}

	measurement := provider.Run(nil, metric)
	if measurement.Phase != v1alpha1.AnalysisPhaseSuccessful {
		t.Fatalf("phase = %q, message = %q", measurement.Phase, measurement.Message)
	}
	if measurement.Value != "42" {
		t.Fatalf("value = %q, want 42", measurement.Value)
	}
	if measurement.StartedAt == nil || measurement.FinishedAt == nil {
		t.Fatal("measurement timestamps missing")
	}
}

func buildPluginBinary(t *testing.T) string {
	t.Helper()

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not resolve test source path")
	}
	repoRoot := filepath.Dir(sourceFile)
	binaryPath := filepath.Join(t.TempDir(), "metric-plugin")
	command := exec.Command("go", "build", "-trimpath", "-o", binaryPath, ".")
	command.Dir = repoRoot
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, output)
	}
	return binaryPath
}

func startBinaryProvider(t *testing.T, binaryPath string) (*goPlugin.Client, rolloutsPlugin.MetricProviderPlugin) {
	t.Helper()

	client := goPlugin.NewClient(&goPlugin.ClientConfig{
		Cmd:             exec.Command(binaryPath),
		HandshakeConfig: handshakeConfig,
		Plugins: map[string]goPlugin.Plugin{
			"RpcMetricProviderPlugin": &rolloutsPlugin.RpcMetricProviderPlugin{},
		},
	})

	raw, err := client.Client()
	if err != nil {
		client.Kill()
		t.Fatalf("plugin client error: %v", err)
	}
	dispensed, err := raw.Dispense("RpcMetricProviderPlugin")
	if err != nil {
		client.Kill()
		t.Fatalf("plugin dispense error: %v", err)
	}
	provider, ok := dispensed.(rolloutsPlugin.MetricProviderPlugin)
	if !ok {
		client.Kill()
		t.Fatalf("dispensed plugin has type %T, want MetricProviderPlugin", dispensed)
	}
	return client, provider
}

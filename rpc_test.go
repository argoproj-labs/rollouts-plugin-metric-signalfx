package main

import (
	"context"
	"testing"
	"time"

	signalfxPlugin "github.com/argoproj-labs/rollouts-plugin-metric-signalfx/internal/plugin"
	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	goPlugin "github.com/hashicorp/go-plugin"
	log "github.com/sirupsen/logrus"
)

func TestEntrypointContract(t *testing.T) {
	if handshakeConfig.ProtocolVersion != 1 {
		t.Fatalf("protocol version = %d, want 1", handshakeConfig.ProtocolVersion)
	}
	if handshakeConfig.MagicCookieKey != "ARGO_ROLLOUTS_RPC_PLUGIN" {
		t.Fatalf("cookie key = %q", handshakeConfig.MagicCookieKey)
	}
	if handshakeConfig.MagicCookieValue != "metricprovider" {
		t.Fatalf("cookie value = %q", handshakeConfig.MagicCookieValue)
	}

	plugins := pluginMap(*log.WithField("test", "entrypoint"))
	if _, ok := plugins["RpcMetricProviderPlugin"]; !ok {
		t.Fatal("RpcMetricProviderPlugin is not registered")
	}
}

func TestRPCProviderContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	implementation := &signalfxPlugin.RpcPlugin{LogCtx: *log.WithField("test", "rpc")}
	plugins := map[string]goPlugin.Plugin{
		"RpcMetricProviderPlugin": &rolloutsPlugin.RpcMetricProviderPlugin{Impl: implementation},
	}

	received := make(chan *goPlugin.ReattachConfig, 1)
	closed := make(chan struct{})
	go goPlugin.Serve(&goPlugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         plugins,
		Test: &goPlugin.ServeTestConfig{
			Context:          ctx,
			ReattachConfigCh: received,
			CloseCh:          closed,
		},
	})

	var config *goPlugin.ReattachConfig
	select {
	case config = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive reattach config")
	}

	client := goPlugin.NewClient(&goPlugin.ClientConfig{
		Cmd:             nil,
		HandshakeConfig: handshakeConfig,
		Plugins:         plugins,
		Reattach:        config,
	})
	defer client.Kill()

	raw, err := client.Client()
	if err != nil {
		t.Fatalf("client error: %v", err)
	}

	dispensed, err := raw.Dispense("RpcMetricProviderPlugin")
	if err != nil {
		t.Fatalf("dispense error: %v", err)
	}

	provider, ok := dispensed.(rolloutsPlugin.MetricProviderPlugin)
	if !ok {
		t.Fatal("dispensed plugin has wrong type")
	}

	if err := provider.InitPlugin(); err.HasError() {
		t.Fatalf("InitPlugin error: %v", err)
	}

	if got := provider.Type(); got != "RPCPlugin" {
		t.Fatalf("Type() = %q, want RPCPlugin", got)
	}

	measurement := provider.Run(nil, v1alpha1.Metric{})
	if measurement.Phase != v1alpha1.AnalysisPhaseError {
		t.Fatalf("Run phase = %q, want Error", measurement.Phase)
	}

	resumeInput := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseRunning, Value: "42"}
	if got := provider.Resume(nil, v1alpha1.Metric{}, resumeInput); got.Phase != resumeInput.Phase || got.Value != resumeInput.Value {
		t.Fatalf("Resume() = %#v, want %#v", got, resumeInput)
	}

	terminateInput := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseRunning, Value: "99"}
	if got := provider.Terminate(nil, v1alpha1.Metric{}, terminateInput); got.Phase != terminateInput.Phase {
		t.Fatalf("Terminate() = %#v, want %#v", got, terminateInput)
	}

	if err := provider.GarbageCollect(nil, v1alpha1.Metric{}, 0); err.HasError() {
		t.Fatalf("GarbageCollect error: %v", err)
	}

	metadata := provider.GetMetadata(v1alpha1.Metric{})
	if _, ok := metadata["error"]; !ok {
		t.Fatalf("GetMetadata() = %#v, want error for empty metric", metadata)
	}

	client.Kill()
	cancel()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("RPC server did not stop")
	}
}

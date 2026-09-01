package plugin

import (
	"reflect"
	"testing"

	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	log "github.com/sirupsen/logrus"
)

var _ rolloutsPlugin.MetricProviderPlugin = (*RpcPlugin)(nil)

func TestRpcPluginType(t *testing.T) {
	provider := &RpcPlugin{LogCtx: *log.WithField("test", "provider")}
	if got := provider.Type(); got != "RPCPlugin" {
		t.Fatalf("Type() = %q, want RPCPlugin", got)
	}
}

func TestRpcPluginScaffoldLifecycle(t *testing.T) {
	provider := &RpcPlugin{LogCtx: *log.WithField("test", "provider")}
	if err := provider.InitPlugin(); err.HasError() {
		t.Fatalf("InitPlugin() error = %v", err)
	}

	measurement := v1alpha1.Measurement{Phase: v1alpha1.AnalysisPhaseRunning, Value: "42"}
	if got := provider.Resume(nil, v1alpha1.Metric{}, measurement); !reflect.DeepEqual(got, measurement) {
		t.Fatalf("Resume() = %#v, want %#v", got, measurement)
	}
	if got := provider.Terminate(nil, v1alpha1.Metric{}, measurement); !reflect.DeepEqual(got, measurement) {
		t.Fatalf("Terminate() = %#v, want %#v", got, measurement)
	}
	if err := provider.GarbageCollect(nil, v1alpha1.Metric{}, 0); err.HasError() {
		t.Fatalf("GarbageCollect() error = %v", err)
	}
	if got := provider.GetMetadata(v1alpha1.Metric{}); got["error"] == "" {
		t.Fatalf("GetMetadata() = %#v, want error", got)
	}
	if got := provider.Run(nil, v1alpha1.Metric{}); got.Phase != v1alpha1.AnalysisPhaseError {
		t.Fatalf("Run() phase = %q, want Error", got.Phase)
	}
}

package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	providerPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin"
	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
	"github.com/argoproj/argo-rollouts/utils/evaluate"
	metricutil "github.com/argoproj/argo-rollouts/utils/metric"
	pluginTypes "github.com/argoproj/argo-rollouts/utils/plugin/types"
	timeutil "github.com/argoproj/argo-rollouts/utils/time"
	log "github.com/sirupsen/logrus"
)

type RpcPlugin struct {
	LogCtx log.Entry
}

var _ rolloutsPlugin.MetricProviderPlugin = (*RpcPlugin)(nil)

func (p *RpcPlugin) InitPlugin() pluginTypes.RpcError {
	return pluginTypes.RpcError{}
}

func (p *RpcPlugin) Run(_ *v1alpha1.AnalysisRun, metric v1alpha1.Metric) v1alpha1.Measurement {
	started := timeutil.MetaNow()
	measurement := v1alpha1.Measurement{
		StartedAt: &started,
	}

	config, err := parseConfig(metric)
	if err != nil {
		return metricutil.MarkMeasurementError(measurement, err)
	}

	client, err := newSignalFlowClient(config, p.LogCtx)
	if err != nil {
		return metricutil.MarkMeasurementError(measurement, err)
	}
	defer client.Close()

	value, err := collectSignalFlow(context.Background(), client, config, p.LogCtx)
	if err != nil {
		return metricutil.MarkMeasurementError(measurement, err)
	}

	measurement.Value = strconv.FormatFloat(value, 'f', -1, 64)

	phase, err := evaluate.EvaluateResult(value, metric, p.LogCtx)
	if err != nil {
		return metricutil.MarkMeasurementError(measurement, err)
	}
	measurement.Phase = phase

	finished := timeutil.MetaNow()
	measurement.FinishedAt = &finished
	return measurement
}

func (p *RpcPlugin) Resume(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (p *RpcPlugin) Terminate(_ *v1alpha1.AnalysisRun, _ v1alpha1.Metric, measurement v1alpha1.Measurement) v1alpha1.Measurement {
	return measurement
}

func (p *RpcPlugin) GarbageCollect(*v1alpha1.AnalysisRun, v1alpha1.Metric, int) pluginTypes.RpcError {
	return pluginTypes.RpcError{}
}

func (p *RpcPlugin) Type() string {
	return providerPlugin.ProviderType
}

func (p *RpcPlugin) GetMetadata(metric v1alpha1.Metric) map[string]string {
	if metric.Provider.Plugin == nil {
		return map[string]string{"error": "missing plugin configuration"}
	}
	raw, ok := metric.Provider.Plugin[PluginName]
	if !ok {
		return map[string]string{"error": "missing plugin configuration"}
	}

	var config Config
	if err := json.Unmarshal(raw, &config); err != nil {
		return map[string]string{"error": fmt.Sprintf("failed to parse plugin config: %v", err)}
	}

	metadata := map[string]string{}
	if config.Query != "" {
		metadata[ResolvedQueryKey] = config.Query
	}
	return metadata
}

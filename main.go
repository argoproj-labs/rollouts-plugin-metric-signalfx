package main

import (
	signalfxPlugin "github.com/argoproj-labs/rollouts-plugin-metric-signalfx/internal/plugin"
	rolloutsPlugin "github.com/argoproj/argo-rollouts/metricproviders/plugin/rpc"
	goPlugin "github.com/hashicorp/go-plugin"
	log "github.com/sirupsen/logrus"
)

var handshakeConfig = goPlugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "ARGO_ROLLOUTS_RPC_PLUGIN",
	MagicCookieValue: "metricprovider",
}

func pluginMap(logCtx log.Entry) map[string]goPlugin.Plugin {
	return map[string]goPlugin.Plugin{
		"RpcMetricProviderPlugin": &rolloutsPlugin.RpcMetricProviderPlugin{
			Impl: &signalfxPlugin.RpcPlugin{LogCtx: logCtx},
		},
	}
}

func main() {
	logCtx := *log.WithField("plugin", "signalfx")
	goPlugin.Serve(&goPlugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap(logCtx),
	})
}

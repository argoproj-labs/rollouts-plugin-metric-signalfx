package plugin

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/argoproj/argo-rollouts/pkg/apis/rollouts/v1alpha1"
)

const (
	PluginName       = "argoproj-labs/rollouts-plugin-metric-signalfx"
	AccessTokenEnv   = "SIGNALFX_ACCESS_TOKEN"
	ResolvedQueryKey = "ResolvedSignalFlowQuery"
)

var supportedAggregators = map[string]struct{}{
	"max":    {},
	"min":    {},
	"avg":    {},
	"sum":    {},
	"count":  {},
	"latest": {},
}

type Config struct {
	Query       string `json:"query"`
	Realm       string `json:"realm"`
	AccessToken string `json:"accessToken"`
	Duration    int    `json:"duration"`
	Aggregator  string `json:"aggregator"`
	StreamURL   string `json:"streamURL"`
}

func parseConfig(metric v1alpha1.Metric) (Config, error) {
	if metric.Provider.Plugin == nil {
		return Config{}, fmt.Errorf("metric %q has no plugin provider config", metric.Name)
	}

	raw, ok := metric.Provider.Plugin[PluginName]
	if !ok {
		return Config{}, fmt.Errorf("metric %q is missing provider.plugin[%q] config", metric.Name, PluginName)
	}

	return parseConfigJSON(raw, os.Getenv(AccessTokenEnv))
}

func parseConfigJSON(raw json.RawMessage, envToken string) (Config, error) {
	var config Config
	if err := json.Unmarshal(raw, &config); err != nil {
		return Config{}, fmt.Errorf("failed to parse plugin config: %w", err)
	}

	if strings.TrimSpace(config.Query) == "" {
		return Config{}, fmt.Errorf("config field 'query' is required")
	}
	if strings.TrimSpace(config.AccessToken) == "" {
		config.AccessToken = envToken
	}
	if strings.TrimSpace(config.AccessToken) == "" {
		return Config{}, fmt.Errorf("config field 'accessToken' or environment variable '%s' is required", AccessTokenEnv)
	}
	if config.Duration <= 0 {
		return Config{}, fmt.Errorf("config field 'duration' must be greater than zero")
	}
	if _, ok := supportedAggregators[config.Aggregator]; !ok {
		return Config{}, fmt.Errorf("config field 'aggregator' must be one of max, min, avg, sum, count, latest")
	}

	if config.StreamURL != "" {
		if err := validateStreamURL(config.StreamURL); err != nil {
			return Config{}, err
		}
		return config, nil
	}
	if strings.TrimSpace(config.Realm) == "" {
		return Config{}, fmt.Errorf("config field 'realm' is required when streamURL is empty")
	}

	return config, nil
}

func validateStreamURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("config field 'streamURL' is invalid: %w", err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return fmt.Errorf("config field 'streamURL' must use ws or wss")
	}
	if parsed.Host == "" {
		return fmt.Errorf("config field 'streamURL' must include a host")
	}
	return nil
}

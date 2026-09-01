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
	inlineToken := strings.TrimSpace(config.AccessToken) != ""
	if !inlineToken {
		config.AccessToken = envToken
	}
	if strings.TrimSpace(config.AccessToken) == "" {
		return Config{}, fmt.Errorf("config field 'accessToken' or environment variable '%s' is required", AccessTokenEnv)
	}
	if config.Duration <= 0 {
		return Config{}, fmt.Errorf("config field 'duration' must be greater than zero")
	}
	if int64(config.Duration) > maxDurationSeconds {
		return Config{}, fmt.Errorf("config field 'duration' is too large")
	}
	if _, ok := supportedAggregators[config.Aggregator]; !ok {
		return Config{}, fmt.Errorf("config field 'aggregator' must be one of max, min, avg, sum, count, latest")
	}
	if config.Realm != "" {
		if err := validateRealm(config.Realm); err != nil {
			return Config{}, err
		}
	}

	if config.StreamURL != "" {
		streamURL, err := parseStreamURL(config.StreamURL)
		if err != nil {
			return Config{}, err
		}
		if !inlineToken {
			if strings.TrimSpace(config.Realm) == "" {
				return Config{}, fmt.Errorf("config field 'realm' is required when streamURL uses environment authentication")
			}
			if streamURL.Scheme != "wss" {
				return Config{}, fmt.Errorf("config field 'streamURL' must use wss with environment authentication")
			}
			if !strings.EqualFold(streamURL.Host, signalFlowHost(config.Realm)) {
				return Config{}, fmt.Errorf("config field 'streamURL' host must match the SignalFlow host for realm")
			}
		}
		return config, nil
	}
	if strings.TrimSpace(config.Realm) == "" {
		return Config{}, fmt.Errorf("config field 'realm' is required when streamURL is empty")
	}

	return config, nil
}

func validateStreamURL(value string) error {
	_, err := parseStreamURL(value)
	return err
}

func parseStreamURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("config field 'streamURL' is invalid: %w", err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return nil, fmt.Errorf("config field 'streamURL' must use ws or wss")
	}
	if parsed.Host == "" || parsed.Hostname() == "" {
		return nil, fmt.Errorf("config field 'streamURL' must include a host")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("config field 'streamURL' must not include user info")
	}
	return parsed, nil
}

func validateRealm(realm string) error {
	if len(realm) == 0 || len(realm) > 63 {
		return fmt.Errorf("config field 'realm' is invalid")
	}
	for index := range realm {
		character := realm[index]
		isLetter := character >= 'a' && character <= 'z'
		isDigit := character >= '0' && character <= '9'
		if !isLetter && !isDigit && character != '-' {
			return fmt.Errorf("config field 'realm' is invalid")
		}
		if character == '-' && (index == 0 || index == len(realm)-1) {
			return fmt.Errorf("config field 'realm' is invalid")
		}
	}
	return nil
}

func signalFlowHost(realm string) string {
	return "stream." + realm + ".signalfx.com"
}

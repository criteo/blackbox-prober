package opensearch

import (
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
)

// Config used to configure the endpoint of OpenSearch
type OpenSearchEndpointConfig struct {
	AuthEnabled bool `yaml:"auth_enabled,omitempty"`
	// ENV related config
	// Env variable name to use to load credentials for OpenSearch
	UsernameEnv string `yaml:"username_env,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty"`
	// TLS related config
	// Tag to use to determine if OpenSearch need to be configured with TLS
	TLSTag string `yaml:"tls_tag,omitempty"`
	// Skip TLS verification
	InsecureSkipVerify bool `yaml:"insecure_skip_verify,omitempty"`
	// Metadata key to get the Hostname to use for TLS auth (only used if tlsTag is set)
	AddressMetaKey string `yaml:"address_meta_key,omitempty"`
}

var (
	defaultOpenSearchEndpointConfig = OpenSearchEndpointConfig{
		AuthEnabled:        true,
		UsernameEnv:        "OPENSEARCH_USERNAME",
		PasswordEnv:        "OPENSEARCH_PASSWORD",
		TLSTag:             "tls",
		InsecureSkipVerify: true,
	}
)

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *OpenSearchEndpointConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultOpenSearchEndpointConfig
	type plain OpenSearchEndpointConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	return nil
}

type OpenSearchProbeConfig struct {
	// Generic discovery configurations
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Client configuration
	OpenSearchEndpointConfig OpenSearchEndpointConfig `yaml:"client_config,omitempty"`
	// Check configurations
	OpenSearchChecksConfigs OpenSearchChecksConfigs `yaml:"checks_configs,omitempty"`
}

type OpenSearchChecksConfigs struct {
	LatencyCheckConfig    scheduler.CheckConfig `yaml:"latency_check,omitempty"`
	DurabilityCheckConfig scheduler.CheckConfig `yaml:"durability_check,omitempty"`
}

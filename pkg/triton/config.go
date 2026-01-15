package triton

import (
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
)

// TritonEndpointConfig holds configuration for connecting to Triton servers.
type TritonEndpointConfig struct {
	// Timeout for gRPC operations (connect, inference, metadata fetch)
	Timeout time.Duration `yaml:"timeout,omitempty"`
	// BatchSize for inference requests during latency checks
	BatchSize int64 `yaml:"batch_size,omitempty"`

	// Activity detection configuration
	// OnlyProbeActiveModels skips probing models that have no external traffic
	OnlyProbeActiveModels bool `yaml:"only_probe_active_models,omitempty"`
	// ActivityMargin is the minimum number of external executions (beyond probe traffic)
	// required to consider a model as active. Default is 0.
	ActivityMargin int64 `yaml:"activity_margin,omitempty"`
}

var defaultTritonEndpointConfig = TritonEndpointConfig{
	Timeout:               30 * time.Second,
	BatchSize:             1,
	OnlyProbeActiveModels: false,
	ActivityMargin:        0,
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *TritonEndpointConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultTritonEndpointConfig
	type plain TritonEndpointConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	return nil
}

// TritonProbeConfig is the top-level configuration for the Triton probe.
type TritonProbeConfig struct {
	// Discovery configuration for Consul service discovery
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Endpoint configuration for Triton gRPC connections
	TritonEndpointConfig TritonEndpointConfig `yaml:"client_config,omitempty"`
	// Check configurations
	TritonChecksConfigs TritonChecksConfigs `yaml:"checks_configs,omitempty"`
}

// TritonChecksConfigs holds configuration for individual checks.
type TritonChecksConfigs struct {
	LatencyCheckConfig scheduler.CheckConfig `yaml:"latency_check,omitempty"`
}

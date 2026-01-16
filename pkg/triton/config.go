package triton

import (
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
)

// SkipInactiveModelsConfig configures skipping of inactive models.
type SkipInactiveModelsConfig struct {
	// Enabled activates inactive model filtering.
	// When true, models with no external traffic will be skipped.
	Enabled bool `yaml:"enabled,omitempty"`
	// ProbeReplicas is the number of probe instances running against the same Triton servers.
	// Used to calculate expected probe traffic. Default is 1.
	ProbeReplicas int64 `yaml:"probe_replicas,omitempty"`
	// Margin is the minimum external executions (beyond expected probe traffic)
	// required to consider a model as active. Default is 0.
	Margin int64 `yaml:"margin,omitempty"`
}

// TritonEndpointConfig holds configuration for connecting to Triton servers.
type TritonEndpointConfig struct {
	// Timeout for gRPC operations (connect, inference, metadata fetch)
	Timeout time.Duration `yaml:"timeout,omitempty"`
	// BatchSize for inference requests during latency checks
	BatchSize int64 `yaml:"batch_size,omitempty"`
	// SkipInactiveModels configures skipping of inactive models
	SkipInactiveModels SkipInactiveModelsConfig `yaml:"skip_inactive_models,omitempty"`
}

var defaultTritonEndpointConfig = TritonEndpointConfig{
	Timeout:   30 * time.Second,
	BatchSize: 1,
	SkipInactiveModels: SkipInactiveModelsConfig{
		Enabled:       false,
		ProbeReplicas: 2,
		Margin:        5,
	},
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

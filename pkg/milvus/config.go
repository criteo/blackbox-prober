package milvus

import (
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
)

// Config used to configure the endpoint of Milvus
type MilvusEndpointConfig struct {
	AuthEnabled bool `yaml:"auth_enabled,omitempty"`
	// ENV related config
	// Env variable name to use to load credentials for Milvus
	UsernameEnv string `yaml:"username_env,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty"`
	// DISCOVERY related config
	// Tag to use to determine if Milvus need to be configured with TLS
	TLSTag string `yaml:"tls_tag,omitempty"`
	// Metadata key to get the Hostname to use for TLS auth (only used if tlsTag is set)
	AddressMetaKey string `yaml:"address_meta_key,omitempty"`
	// Probe configuration
	DatabaseMetaKey       string `yaml:"database_meta_key,omitempty"`
	DatabaseMetaKeyPrefix string `yaml:"database_meta_key_prefix,omitempty"`
	// MonitoringSet is the database name managed by the probe (defaults to "monitoring")
	MonitoringSet       string        `yaml:"monitoring_set,omitempty"`
	LatencyKeyPrefix    string        `yaml:"latency_key_prefix,omitempty"`
	DurabilityKeyPrefix string        `yaml:"durability_key_prefix,omitempty"`
	DurabilityKeyTotal  int           `yaml:"durability_key_total,omitempty"`
	MaxRetry            uint          `yaml:"max_retry,omitempty"`
	MaxBackoff          time.Duration `yaml:"max_backoff,omitempty"`
}

var (
	defaultMilvusClient = MilvusEndpointConfig{
		UsernameEnv:           "MILVUS_USERNAME",
		PasswordEnv:           "MILVUS_PASSWORD",
		TLSTag:                "tls",
		DatabaseMetaKeyPrefix: "milvus-monitoring-",
		MonitoringSet:         "monitoring",
		LatencyKeyPrefix:      "monitoring_latency_",
		DurabilityKeyPrefix:   "monitoring_durability_",
		DurabilityKeyTotal:    10000,
	}
)

type MilvusProbeConfig struct {
	// Generic consul configurations
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Client configuration
	MilvusEndpointConfig MilvusEndpointConfig `yaml:"client_config,omitempty"`
	// Check configurations
	MilvusChecksConfigs MilvusChecksConfigs `yaml:"checks_configs,omitempty"`
}

type MilvusChecksConfigs struct {
	LatencyCheckConfig    scheduler.CheckConfig `yaml:"latency_check,omitempty"`
	DurabilityCheckConfig scheduler.CheckConfig `yaml:"durability_check,omitempty"`
}

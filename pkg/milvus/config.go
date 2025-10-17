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
	MonitoringDatabase             string `yaml:"monitoring_database,omitempty"`
	MonitoringCollectionLatencyRW  string `yaml:"monitoring_collection_latency_rw,omitempty"`
	MonitoringCollectionLatencyRO  string `yaml:"monitoring_collection_latency,omitempty"`
	MonitoringCollectionDurability string `yaml:"monitoring_collection_durability,omitempty"`

	LatencyRWKeyPrefix      string `yaml:"latency_rw_key_prefix,omitempty"`
	LatencyInitKeyPrefix    string `yaml:"latency_init_key_prefix,omitempty"`
	DurabilityKeyPrefix     string `yaml:"durability_key_prefix,omitempty"`
	InitFlagKey             string `yaml:"init_flag_key,omitempty"`
	InitItemsPerCollection  int    `yaml:"init_items_per_collection,omitempty"`
	LatencyRWInsertPerCheck int    `yaml:"latency_rw_insert_per_check,omitempty"`

	// Timeouts
	LoadTimeout           time.Duration `yaml:"load_timeout,omitempty"`
	SearchTimeout         time.Duration `yaml:"search_timout,omitempty"`
	InitialFlushTimeout   time.Duration `yaml:"initial_flush_timeout,omitempty"`
	IndexTimeout          time.Duration `yaml:"index_timeout,omitempty"`
	CreateDatabaseTimeout time.Duration `yaml:"create_database_timeout,omitempty"`

	// Client configuration for probe
	MaxRetry   uint          `yaml:"max_retry,omitempty"`
	MaxBackoff time.Duration `yaml:"max_backoff,omitempty"`
}

var (
	defaultMilvusEndpointConfig = MilvusEndpointConfig{
		UsernameEnv:                    "MILVUS_USERNAME",
		PasswordEnv:                    "MILVUS_PASSWORD",
		TLSTag:                         "tls",
		MonitoringDatabase:             "monitoring",
		MonitoringCollectionLatencyRW:  "monitoring_latency_rw",
		MonitoringCollectionLatencyRO:  "monitoring_latency_ro",
		MonitoringCollectionDurability: "monitoring_durability",

		LatencyRWKeyPrefix:      "latency_rw_",
		LatencyInitKeyPrefix:    "latency_init_",
		DurabilityKeyPrefix:     "durability_",
		InitFlagKey:             "init_flag",
		InitItemsPerCollection:  10000,
		LatencyRWInsertPerCheck: 10,

		LoadTimeout:           120 * time.Second,
		SearchTimeout:         120 * time.Second,
		InitialFlushTimeout:   300 * time.Second,
		IndexTimeout:          600 * time.Second,
		CreateDatabaseTimeout: 600 * time.Second,
	}
)

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *MilvusEndpointConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultMilvusEndpointConfig
	type plain MilvusEndpointConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	return nil
}

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

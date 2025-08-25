package aerospike

import (
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/alecthomas/kingpin/v2"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
	"github.com/pkg/errors"
)

// Config used to configure the client of Aerospike
type AerospikeClientConfig struct {
	// auth
	authEnabled bool
	username    string
	password    string
	// tls
	tlsEnabled  bool
	tlsHostname string
	// Contact point
	host as.Host
	// Config
	genericConfig *AerospikeEndpointConfig
}

// Config used to configure the endpoint of Aerospike
type AerospikeEndpointConfig struct {
	AuthEnabled bool `yaml:"auth_enabled,omitempty"`
	// If AuthEnabled use Aerospike auth external otherwise use internal
	AuthExternal  bool `yaml:"auth_external,omitempty"`
	TLSSkipVerify bool `yaml:"tls_skip_verify,omitempty"`
	// ENV related config
	// Env variable name to use to load credentials for Aerospike
	UsernameEnv string `yaml:"username_env,omitempty"`
	PasswordEnv string `yaml:"password_env,omitempty"`
	// DISCOVERY related config
	// Tag to use to determine if Aerospike need to be configured with TLS
	TLSTag string `yaml:"tls_tag,omitempty"`
	// Metadata key to get the Hostname to use for TLS auth (only used if tlsTag is set)
	TLSHostnameMetaKey string `yaml:"tls_hostname_meta_key,omitempty"`
	// Probe configuration
	NamespaceMetaKey                  string        `yaml:"namespace_meta_key,omitempty"`
	NamespaceMetaKeyPrefix            string        `yaml:"namespace_meta_key_prefix,omitempty"`
	MonitoringSet                     string        `yaml:"monitoring_set,omitempty"`
	LatencyKeyPrefix                  string        `yaml:"latency_key_prefix,omitempty"`
	DurabilityKeyPrefix               string        `yaml:"durability_key_prefix,omitempty"`
	DurabilityKeyTotal                int           `yaml:"durability_key_total,omitempty"`
	ConnectionQueueSize               int           `yaml:"connection_queue_size,omitempty"`
	OpeningConnectionThreshold        int           `yaml:"opening_connection_threshold,omitempty"`
	MinConnectionsPerNode             int           `yaml:"min_connections_per_node,omitempty"`
	TendInterval                      time.Duration `yaml:"tend_interval,omitempty"`
	TotalTimeout                      time.Duration `yaml:"total_timeout,omitempty"`
	ExitFastOnExhaustedConnectionPool bool          `yaml:"exit_fast_on_exhausted_connection_pool,omitempty"`
}

var (
	defaultAerospikeClient = AerospikeEndpointConfig{
		AuthEnabled:                       true,
		AuthExternal:                      true,
		UsernameEnv:                       "AEROSPIKE_USERNAME",
		PasswordEnv:                       "AEROSPIKE_PASSWORD",
		TLSTag:                            "tls",
		TLSHostnameMetaKey:                "tls-hostname",
		NamespaceMetaKeyPrefix:            "aerospike-monitoring-",
		MonitoringSet:                     "monitoring",
		LatencyKeyPrefix:                  "monitoring_latency_",
		DurabilityKeyPrefix:               "monitoring_durability_",
		DurabilityKeyTotal:                10000,
		ConnectionQueueSize:               256,
		OpeningConnectionThreshold:        0,
		MinConnectionsPerNode:             0,
		TendInterval:                      time.Second,
		TotalTimeout:                      30 * time.Second,
		ExitFastOnExhaustedConnectionPool: false,
	}
)

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *AerospikeEndpointConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultAerospikeClient
	type plain AerospikeEndpointConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	return nil
}

func AddFlags(a *kingpin.Application, cfg *AerospikeProbeCommandLine) {
	a.HelpFlag.Short('h')
	a.Flag("aeropsike.log.level", "Only log messages with the given severity or above. One of: [debug, info, warn, error, off]").
		Default("off").StringVar(&cfg.AerospikeLogLevel)
}

func GetLevel(s string) (asl.LogPriority, error) {
	switch s {
	case "off":
		return asl.OFF, nil
	case "debug":
		return asl.DEBUG, nil
	case "info":
		return asl.INFO, nil
	case "warn":
		return asl.WARNING, nil
	case "error":
		return asl.ERR, nil
	default:
		return asl.OFF, errors.Errorf("unrecognized log level %q", s)
	}
}

type AerospikeProbeCommandLine struct {
	AerospikeLogLevel string `yaml:"aeropsike_log_level,omitempty"`
}

type AerospikeProbeConfig struct {
	// Generic consul configurations
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Client configuration
	AerospikeEndpointConfig AerospikeEndpointConfig `yaml:"client_config,omitempty"`
	// Check configurations
	AerospikeChecksConfigs AerospikeChecksConfigs `yaml:"checks_configs,omitempty"`
}

type AerospikeChecksConfigs struct {
	LatencyCheckConfig    scheduler.CheckConfig `yaml:"latency_check,omitempty"`
	DurabilityCheckConfig scheduler.CheckConfig `yaml:"durability_check,omitempty"`
}

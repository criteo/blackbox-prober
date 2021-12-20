package main

import (
	as "github.com/aerospike/aerospike-client-go"
	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/scheduler"
)

// Config used to configure the client of Aerospike
type AerospikeClientConfig struct {
	// auth
	authEnabled  bool
	authExternal bool
	username     string
	password     string
	// tls
	tlsEnabled    bool
	tlsHostname   string
	tlsSkipVerify bool
	// Contact point
	host as.Host
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
}

var (
	defaultAerospikeClient = AerospikeEndpointConfig{
		AuthEnabled:        true,
		AuthExternal:       true,
		UsernameEnv:        "AEROSPIKE_USERNAME",
		PasswordEnv:        "AEROSPIKE_PASSWORD",
		TLSTag:             "tls",
		TLSHostnameMetaKey: "tls-hostname",
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

type AerospikeProbeConfig struct {
	// Generic consul configurations
	DiscoveryConfig discovery.GenericDiscoveryConfig `yaml:"discovery,omitempty"`
	// Client configuration
	AerospikeEndpointConfig AerospikeEndpointConfig `yaml:"client_config,omitempty"`
	// Check configurations
	AerospikeChecksConfigs AerospikeChecksConfigs `yaml:"checks_configs,omitempty"`
}

type AerospikeChecksConfigs struct {
	LatencyCheckConfig scheduler.CheckConfig `yaml:"latency_check,omitempty"`
}

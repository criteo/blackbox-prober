package memcached

// Config used to configure the endpoint of Memcached
type MemcachedEndpointConfig struct {
	LatencyKeyPrefix string `yaml:"latency_key_prefix,omitempty"`
}

var (
	defaultMemcachedClient = MemcachedEndpointConfig{
		LatencyKeyPrefix: "monitoring_latency_",
	}
)

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *MemcachedEndpointConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = defaultMemcachedClient
	type plain MemcachedEndpointConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	return nil
}

package discovery

import (
	"errors"
	"log"
	"strings"
	"time"

	"github.com/criteo/blackbox-prober/pkg/topology"
	consul "github.com/hashicorp/consul/api"
	promconfig "github.com/prometheus/common/config"
)

type ConsulDiscoverer struct {
	client *consul.Client
	config ConsulConfig
	// Function to build a topology out of service entries
	topologyBuilderFn func([]ServiceEntry) (topology.ClusterMap, error)
	// Chan where to send new topologies
	topologyChan chan topology.ClusterMap
}

func NewConsulDiscoverer(config ConsulConfig, topologyChan chan topology.ClusterMap, topologyBuilderFn func([]ServiceEntry) (topology.ClusterMap, error)) (ConsulDiscoverer, error) {
	wrapper, err := promconfig.NewClientFromConfig(config.HTTPClientConfig, "consul_sd")
	if err != nil {
		return ConsulDiscoverer{}, err
	}

	clientConf := &consul.Config{
		Address:    config.Server,
		Scheme:     config.Scheme,
		Datacenter: config.Datacenter,
		Namespace:  config.Namespace,
		Token:      string(config.Token),
		HttpClient: wrapper,
	}
	client, err := consul.NewClient(clientConf)
	if err != nil {
		return ConsulDiscoverer{}, err
	}

	// Check that we can reach Consul
	_, err = client.Catalog().Datacenters()
	if err != nil {
		return ConsulDiscoverer{}, err
	}

	return ConsulDiscoverer{client: client, config: config, topologyBuilderFn: topologyBuilderFn, topologyChan: topologyChan}, nil
}

func (cd ConsulDiscoverer) Start() error {
	catalog := cd.client.Catalog()
	health := cd.client.Health()

	opts := &consul.QueryOptions{
		AllowStale: cd.config.AllowStale,
		NodeMeta:   cd.config.NodeMeta,
	}

	srvs, _, err := catalog.Services(opts)
	if err != nil {
		return err
	}

	matchedServices := []string{}

	for name := range srvs {
		if cd.config.matchFromName(name) && cd.config.matchFromTags(srvs[name]) {
			matchedServices = append(matchedServices, name)
		}
	}

	log.Println("Found following services:", matchedServices)

	allServiceEntries := []ServiceEntry{}
	// Check for removed services.
	for _, srvc := range matchedServices {
		entries, _, err := health.ServiceMultipleTags(srvc, cd.config.ServiceTags, false, opts)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			allServiceEntries = append(allServiceEntries, toServiceEntry(entry))
		}
	}

	clusterMap, err := cd.topologyBuilderFn(allServiceEntries)
	if err != nil {
		return err
	}
	// Send the new topology to the scheduler
	cd.topologyChan <- clusterMap
	return nil
}

func toServiceEntry(entry *consul.ServiceEntry) ServiceEntry {
	return ServiceEntry{
		Service: entry.Service.Service,
		Tags:    entry.Service.Tags,
		Meta:    entry.Service.Meta,
		Port:    entry.Service.Port,
		Address: entry.Service.Address,
	}
}

// This part is (Apache licensed) code from Prometheus modified for the probe

var (
	DefaultConsulConfig = ConsulConfig{
		TagSeparator:     ",",
		Scheme:           "http",
		Server:           "localhost:8500",
		AllowStale:       true,
		PassingOnly:      false,
		RefreshInterval:  time.Duration(30 * time.Second),
		HTTPClientConfig: promconfig.DefaultHTTPClientConfig,
	}
)

// ConsulConfig is the configuration for Consul service discovery.
type ConsulConfig struct {
	Server       string            `yaml:"server,omitempty"`
	Token        promconfig.Secret `yaml:"token,omitempty"`
	Datacenter   string            `yaml:"datacenter,omitempty"`
	Namespace    string            `yaml:"namespace,omitempty"`
	TagSeparator string            `yaml:"tag_separator,omitempty"`
	Scheme       string            `yaml:"scheme,omitempty"`
	Username     string            `yaml:"username,omitempty"`
	Password     promconfig.Secret `yaml:"password,omitempty"`

	// See https://www.consul.io/docs/internals/consensus.html#consistency-modes,
	// stale reads are a lot cheaper and are a necessity if you have >5k targets.
	AllowStale bool `yaml:"allow_stale"`
	// By default use blocking queries (https://www.consul.io/api/index.html#blocking-queries)
	// but allow users to throttle updates if necessary. This can be useful because of "bugs" like
	// https://github.com/hashicorp/consul/issues/3712 which cause an un-necessary
	// amount of requests on consul.
	RefreshInterval time.Duration `yaml:"refresh_interval,omitempty"`

	// See https://www.consul.io/api/catalog.html#list-services
	// The list of services for which targets are discovered.
	// Defaults to all services if empty.
	Services []string `yaml:"services,omitempty"`
	// A list of tags used to filter instances inside a service. Services must contain all tags in the list.
	ServiceTags []string `yaml:"tags,omitempty"`
	// Desired node metadata.
	NodeMeta map[string]string `yaml:"node_meta,omitempty"`

	HTTPClientConfig promconfig.HTTPClientConfig `yaml:",inline"`

	// Prober specifics
	PassingOnly bool `yaml:"passing_only,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *ConsulConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConsulConfig
	type plain ConsulConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if strings.TrimSpace(c.Server) == "" {
		return errors.New("consul SD configuration requires a server address")
	}
	if c.Username != "" || c.Password != "" {
		if c.HTTPClientConfig.BasicAuth != nil {
			return errors.New("at most one of consul SD configuration username and password and basic auth can be configured")
		}
		c.HTTPClientConfig.BasicAuth = &promconfig.BasicAuth{
			Username: c.Username,
			Password: c.Password,
		}
	}
	if c.Token != "" && (c.HTTPClientConfig.Authorization != nil || c.HTTPClientConfig.OAuth2 != nil) {
		return errors.New("at most one of consul SD token, authorization, or oauth2 can be configured")
	}
	return c.HTTPClientConfig.Validate()
}

// matchFromName returns whether the service of the given name should be used based on its name.
func (c *ConsulConfig) matchFromName(name string) bool {
	// If there's no fixed set of watched services, we watch everything.
	if len(c.Services) == 0 {
		return true
	}

	for _, sn := range c.Services {
		if sn == name {
			return true
		}
	}
	return false
}

// matchFromTags returns whether the service of the given name should be watched based on its tags.
// This gets called when the user doesn't specify a list of services in order to avoid watching
// *all* services. Details in https://github.com/prometheus/prometheus/pull/3814
func (c *ConsulConfig) matchFromTags(tags []string) bool {
	// If there's no fixed set of watched tags, we watch everything.
	if len(c.ServiceTags) == 0 {
		return true
	}

tagOuter:
	for _, wtag := range c.ServiceTags {
		for _, tag := range tags {
			if wtag == tag {
				continue tagOuter
			}
		}
		return false
	}
	return true
}

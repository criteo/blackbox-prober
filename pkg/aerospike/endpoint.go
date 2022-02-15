package aerospike

import (
	"crypto/tls"
	"fmt"
	"regexp"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	as "github.com/aerospike/aerospike-client-go"
)

var (
	setExtractionRegex, _ = regexp.Compile("ns=([a-zA-Z0-9]+):")
)

type AerospikeEndpoint struct {
	Name                   string
	ClusterLevel           bool
	ClusterName            string
	Client                 *as.Client
	Config                 AerospikeClientConfig
	Logger                 log.Logger
	AutoDiscoverNamespaces bool
	Namespaces             map[string]struct{}
}

func (e *AerospikeEndpoint) GetHash() string {
	// If Namespaces are pushed through service discovery
	// the hash should change according to the Namespaces
	if !e.AutoDiscoverNamespaces {
		// Make sure the list is always in the same order
		namespaces := make([]string, 0, len(e.Namespaces))
		for str := range e.Namespaces {
			namespaces = append(namespaces, str)
		}
		sort.Strings(namespaces)
		return fmt.Sprintf("%s/ns:%s", e.Name, namespaces)
	}
	return e.Name
}

func (e *AerospikeEndpoint) GetName() string {
	return e.Name
}

func (e *AerospikeEndpoint) IsCluster() bool {
	return e.ClusterLevel
}

func (e *AerospikeEndpoint) Connect() error {
	clientPolicy := as.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = e.Config.genericConfig.ConnectionQueueSize
	clientPolicy.OpeningConnectionThreshold = e.Config.genericConfig.OpeningConnectionThreshold

	if e.Config.tlsEnabled {
		// Setup TLS Config
		tlsConfig := &tls.Config{
			InsecureSkipVerify:       e.Config.genericConfig.TLSSkipVerify,
			PreferServerCipherSuites: true,
		}
		clientPolicy.TlsConfig = tlsConfig
	}

	if e.Config.authEnabled {
		if e.Config.genericConfig.AuthExternal {
			clientPolicy.AuthMode = as.AuthModeExternal
		} else {
			clientPolicy.AuthMode = as.AuthModeInternal
		}

		clientPolicy.User = e.Config.username
		clientPolicy.Password = e.Config.password
	}

	client, err := as.NewClientWithPolicyAndHost(clientPolicy, &e.Config.host)
	if err != nil {
		return err
	}
	e.Client = client
	e.Refresh()
	e.Client.WarmUp(2)
	return nil
}

func (e *AerospikeEndpoint) Refresh() error {
	if !e.AutoDiscoverNamespaces {
		return nil
	}
	nodes := e.Client.GetNodes()

	infop := as.NewInfoPolicy()

	e.Namespaces = make(map[string]struct{})
	for _, n := range nodes {

		data, err := n.RequestInfo(infop, fmt.Sprintln("sets"))
		if err != nil {
			return err
		}
		for _, val := range data {
			matches := setExtractionRegex.FindAllStringSubmatch(val, -1)
			for _, match := range matches {
				if len(match) > 1 {
					e.Namespaces[match[1]] = struct{}{}
				}
			}
		}
	}
	level.Debug(e.Logger).Log("msg", fmt.Sprintf("Refresh finished: current Namespaces: %s", e.Namespaces))
	return nil
}

func (e *AerospikeEndpoint) Close() error {
	return nil
}

package main

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
	clusterLevel           bool
	ClusterName            string
	Client                 *as.Client
	Config                 AerospikeClientConfig
	Logger                 log.Logger
	AutoDiscoverNamespaces bool
	namespaces             map[string]struct{}
}

func (e *AerospikeEndpoint) GetHash() string {
	// If namespaces are pushed through service discovery
	// the hash should change according to the namespaces
	if !e.AutoDiscoverNamespaces {
		// Make sure the list is always in the same order
		namespaces := make([]string, 0, len(e.namespaces))
		for str := range e.namespaces {
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
	return e.clusterLevel
}

func (e *AerospikeEndpoint) Connect() error {
	clientPolicy := as.NewClientPolicy()

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

	e.namespaces = make(map[string]struct{})
	for _, n := range nodes {

		data, err := n.RequestInfo(infop, fmt.Sprintln("sets"))
		if err != nil {
			return err
		}
		for _, val := range data {
			matches := setExtractionRegex.FindAllStringSubmatch(val, -1)
			for _, match := range matches {
				if len(match) > 1 {
					e.namespaces[match[1]] = struct{}{}
				}
			}
		}
	}
	level.Debug(e.Logger).Log("msg", fmt.Sprintf("Refresh finished: current namespaces: %s", e.namespaces))
	return nil
}

func (e *AerospikeEndpoint) Close() error {
	return nil
}

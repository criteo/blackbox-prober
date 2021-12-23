package main

import (
	"crypto/tls"
	"fmt"
	"regexp"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	as "github.com/aerospike/aerospike-client-go"
)

var (
	setExtractionRegex, _ = regexp.Compile("ns=([a-zA-Z0-9]+):")
)

type AerospikeEndpoint struct {
	Name         string
	clusterLevel bool
	Client       *as.Client
	Config       AerospikeClientConfig
	Logger       log.Logger
	namespaces   map[string]struct{}
}

func (e *AerospikeEndpoint) GetHash() string {
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
			InsecureSkipVerify:       e.Config.tlsSkipVerify,
			PreferServerCipherSuites: true,
		}
		clientPolicy.TlsConfig = tlsConfig
	}

	if e.Config.authEnabled {
		if e.Config.authExternal {
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
	nodes := e.Client.GetNodes()

	infop := as.NewInfoPolicy()
	e.namespaces = make(map[string]struct{})
	for _, n := range nodes {

		data, err := n.RequestInfo(infop, fmt.Sprintln("sets"))
		if err != nil {
			return err
		}
		for _, val := range data {
			matches := setExtractionRegex.FindStringSubmatch(val)
			if len(matches) > 1 {
				e.namespaces[matches[1]] = struct{}{}
			}
		}
	}
	level.Debug(e.Logger).Log("msg", fmt.Sprintf("Refresh finished: current namespaces: %s", e.namespaces))
	return nil
}

func (e *AerospikeEndpoint) Close() error {
	return nil
}

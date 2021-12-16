package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"regexp"

	as "github.com/aerospike/aerospike-client-go"
)

var (
	setExtractionRegex, _ = regexp.Compile("ns=([a-zA-Z0-9]+):")
)

type AerospikeEndpoint struct {
	Name       string
	Client     *as.Client
	Config     AerospikeClientConfig
	namespaces map[string]struct{}
}

func (e *AerospikeEndpoint) Hash() string {
	return e.Name
}

func (e *AerospikeEndpoint) GetName() string {
	return e.Name
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
	log.Println("Refresh finished: current namespaces: ", e.namespaces)
	return nil
}

func (e *AerospikeEndpoint) Close() error {
	return nil
}

package main

import (
	"crypto/tls"

	as "github.com/aerospike/aerospike-client-go"
)

type AerospikeEndpoint struct {
	Name   string
	Client *as.Client
	Config AerospikeClientConfig
}

func (e AerospikeEndpoint) Hash() string {
	return e.Name
}

func (e AerospikeEndpoint) GetName() string {
	return e.Name
}

func (e AerospikeEndpoint) Connect() error {
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
	return nil
}

func (e AerospikeEndpoint) Close() error {
	return nil
}

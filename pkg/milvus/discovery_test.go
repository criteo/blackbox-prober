package milvus

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/go-kit/log"
)

func TestMilvusBuildAddress(t *testing.T) {
	conf := MilvusProbeConfig{}

	tests := []struct {
		name     string
		tls      bool
		target   string
		expected string
	}{
		{
			name:     "TLSDisabled",
			tls:      false,
			target:   "milvus.local",
			expected: "http://milvus.local",
		},
		{
			name:     "TLSEnabled",
			tls:      true,
			target:   "milvus.local",
			expected: "https://milvus.local",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			got := (&conf).buildAddress(tt.tls, tt.target)
			if got != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestGenerateClusterEndpointsFromEntry(t *testing.T) {
	baseConfig := MilvusProbeConfig{
		MilvusEndpointConfig: MilvusEndpointConfig{
			TLSTag:         "tls",
			AddressMetaKey: "address_meta",
			MaxRetry:       5,
			MaxBackoff:     2 * time.Second,
		},
	}

	entryTemplate := discovery.ServiceEntry{
		Service: "milvus-proxy",
		Address: "10.0.0.1",
		Tags:    []string{"tls"},
		Meta: map[string]string{
			"address_meta": "milvus.foo.bar",
			"CLUSTER":      "milvuss99",
		},
	}

	tests := []struct {
		name        string
		authEnabled bool
		username    string
		password    string
	}{
		{
			name:        "AuthDisabled",
			authEnabled: false,
			username:    "",
			password:    "",
		},
		{
			name:        "AuthEnabled",
			authEnabled: true,
			username:    "alice",
			password:    "secret",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			conf := baseConfig
			conf.MilvusEndpointConfig.AuthEnabled = tt.authEnabled
			conf.DiscoveryConfig.MetaClusterKey = "CLUSTER"
			entry := entryTemplate

			if tt.authEnabled {
				conf.MilvusEndpointConfig.UsernameEnv = "MILVUS_TEST_USERNAME"
				conf.MilvusEndpointConfig.PasswordEnv = "MILVUS_TEST_PASSWORD"
				t.Setenv(conf.MilvusEndpointConfig.UsernameEnv, tt.username)
				t.Setenv(conf.MilvusEndpointConfig.PasswordEnv, tt.password)
			}

			endpoints, err := conf.generateClusterEndpointsFromEntry(log.NewNopLogger(), entry)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(endpoints) != 1 {
				t.Fatalf("expected a single endpoint, got %d", len(endpoints))
			}

			endpoint := endpoints[0]

			if endpoint.Name != "milvuss99" {
				t.Fatalf("unexpected endpoint name: %s", endpoint.Name)
			}
			if endpoint.ClusterName != "milvuss99" {
				t.Fatalf("unexpected cluster name: %s", endpoint.ClusterName)
			}
			if !endpoint.ClusterLevel {
				t.Fatal("expected endpoint to be marked as cluster level")
			}
			if endpoint.ClientConfig.Address != "https://milvus.foo.bar" {
				t.Fatalf("unexpected address: %s", endpoint.ClientConfig.Address)
			}

			if tt.authEnabled {
				if endpoint.ClientConfig.Username != tt.username {
					t.Fatalf("expected username %q, got %q", tt.username, endpoint.ClientConfig.Username)
				}
				if endpoint.ClientConfig.Password != tt.password {
					t.Fatalf("expected password %q, got %q", tt.password, endpoint.ClientConfig.Password)
				}
			} else if endpoint.ClientConfig.Username != "" || endpoint.ClientConfig.Password != "" {
				t.Fatalf("expected empty credentials when auth disabled, got %q/%q", endpoint.ClientConfig.Username, endpoint.ClientConfig.Password)
			}

			if endpoint.ClientConfig.RetryRateLimit == nil {
				t.Fatal("expected retry rate limit configuration")
			}
			if endpoint.ClientConfig.RetryRateLimit.MaxRetry != conf.MilvusEndpointConfig.MaxRetry {
				t.Fatalf("expected retry max %d, got %d", conf.MilvusEndpointConfig.MaxRetry, endpoint.ClientConfig.RetryRateLimit.MaxRetry)
			}
			if endpoint.ClientConfig.RetryRateLimit.MaxBackoff != conf.MilvusEndpointConfig.MaxBackoff {
				t.Fatalf("expected retry backoff %s, got %s", conf.MilvusEndpointConfig.MaxBackoff, endpoint.ClientConfig.RetryRateLimit.MaxBackoff)
			}

			if endpoint.Config != conf.MilvusEndpointConfig {
				t.Fatalf("expected endpoint config to match MilvusEndpointConfig")
			}
		})
	}
}

func TestGenerateClusterEndpointsFromEntryAuthMissingEnv(t *testing.T) {
	usernameEnv := fmt.Sprintf("MILVUS_TEST_USERNAME_%d", time.Now().UnixNano())
	passwordEnv := fmt.Sprintf("MILVUS_TEST_PASSWORD_%d", time.Now().UnixNano())

	conf := MilvusProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		MilvusEndpointConfig: MilvusEndpointConfig{
			AuthEnabled:    true,
			UsernameEnv:    usernameEnv,
			PasswordEnv:    passwordEnv,
			TLSTag:         "tls",
			AddressMetaKey: "address_meta",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "milvus-proxy",
		Address: "10.0.0.1",
		Tags:    []string{"tls"},
		Meta: map[string]string{
			"address_meta": "milvus.foo.bar",
			"CLUSTER":      "milvuss99",
		},
	}

	_ = os.Unsetenv(usernameEnv)
	_ = os.Unsetenv(passwordEnv)

	_, err := conf.generateClusterEndpointsFromEntry(log.NewNopLogger(), entry)
	if err == nil {
		t.Fatal("expected error when auth env vars are missing")
	}

	expected := fmt.Sprintf("error: username not found in env (%s)", usernameEnv)
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGenerateClusterEndpointsFromEntryMissingAddressMeta(t *testing.T) {
	conf := MilvusProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		MilvusEndpointConfig: MilvusEndpointConfig{
			TLSTag:         "tls",
			AddressMetaKey: "address_meta",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "milvus-proxy",
		Address: "10.0.0.1",
		Tags:    []string{"tls"},
		Meta: map[string]string{
			"CLUSTER": "milvuss99",
		},
	}

	_, err := conf.generateClusterEndpointsFromEntry(log.NewNopLogger(), entry)
	if err == nil {
		t.Fatal("expected error when address meta is missing")
	}

	expected := fmt.Sprintf("%s not found in consul meta key for service %s", conf.MilvusEndpointConfig.AddressMetaKey, entry.Service)
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestGenerateClusterEndpointsFromEntryMissingClusterMeta(t *testing.T) {
	conf := MilvusProbeConfig{
		DiscoveryConfig: discovery.GenericDiscoveryConfig{
			MetaClusterKey: "CLUSTER",
		},
		MilvusEndpointConfig: MilvusEndpointConfig{
			TLSTag:         "tls",
			AddressMetaKey: "address_meta",
		},
	}

	entry := discovery.ServiceEntry{
		Service: "milvus-proxy",
		Address: "10.0.0.1",
		Tags:    []string{"tls"},
		Meta: map[string]string{
			"address_meta": "milvus.foo.bar",
		},
	}

	_, err := conf.generateClusterEndpointsFromEntry(log.NewNopLogger(), entry)
	if err == nil {
		t.Fatal("expected error when cluster meta is missing")
	}

	expected := fmt.Sprintf("ClusterName meta key not found. Ignoring service %s.", entry.Service)
	if err.Error() != expected {
		t.Fatalf("unexpected error message: %v", err)
	}
}

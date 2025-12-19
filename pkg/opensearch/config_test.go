package opensearch

import (
	"testing"

	"gopkg.in/yaml.v2"
)

func TestOpenSearchEndpointConfigDefaults(t *testing.T) {
	yamlStr := `auth_enabled: true`

	var config OpenSearchEndpointConfig
	err := yaml.Unmarshal([]byte(yamlStr), &config)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Check that defaults are applied
	if !config.AuthEnabled {
		t.Fatal("expected AuthEnabled to be true")
	}
	if config.UsernameEnv != "OPENSEARCH_USERNAME" {
		t.Fatalf("expected default UsernameEnv to be 'OPENSEARCH_USERNAME', got %q", config.UsernameEnv)
	}
	if config.PasswordEnv != "OPENSEARCH_PASSWORD" {
		t.Fatalf("expected default PasswordEnv to be 'OPENSEARCH_PASSWORD', got %q", config.PasswordEnv)
	}
	if config.TLSTag != "tls" {
		t.Fatalf("expected default TLSTag to be 'tls', got %q", config.TLSTag)
	}
	if !config.InsecureSkipVerify {
		t.Fatal("expected default InsecureSkipVerify to be true")
	}
}

func TestOpenSearchEndpointConfigCustomValues(t *testing.T) {
	yamlStr := `
auth_enabled: false
username_env: MY_CUSTOM_USERNAME
password_env: MY_CUSTOM_PASSWORD
tls_tag: secure
insecure_skip_verify: false
address_meta_key: custom_address
`

	var config OpenSearchEndpointConfig
	err := yaml.Unmarshal([]byte(yamlStr), &config)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if config.AuthEnabled {
		t.Fatal("expected AuthEnabled to be false")
	}
	if config.UsernameEnv != "MY_CUSTOM_USERNAME" {
		t.Fatalf("expected UsernameEnv to be 'MY_CUSTOM_USERNAME', got %q", config.UsernameEnv)
	}
	if config.PasswordEnv != "MY_CUSTOM_PASSWORD" {
		t.Fatalf("expected PasswordEnv to be 'MY_CUSTOM_PASSWORD', got %q", config.PasswordEnv)
	}
	if config.TLSTag != "secure" {
		t.Fatalf("expected TLSTag to be 'secure', got %q", config.TLSTag)
	}
	if config.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be false")
	}
	if config.AddressMetaKey != "custom_address" {
		t.Fatalf("expected AddressMetaKey to be 'custom_address', got %q", config.AddressMetaKey)
	}
}

func TestOpenSearchEndpointConfigEmptyYAML(t *testing.T) {
	yamlStr := ``

	var config OpenSearchEndpointConfig
	err := yaml.Unmarshal([]byte(yamlStr), &config)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Empty YAML with no UnmarshalYAML call won't populate defaults
	// The defaults are only applied when UnmarshalYAML is called with actual YAML content
	// So for truly empty YAML, the struct will have zero values
	if config.AuthEnabled {
		t.Fatal("expected AuthEnabled to be false (zero value)")
	}
	if config.UsernameEnv != "" {
		t.Fatalf("expected empty UsernameEnv, got %q", config.UsernameEnv)
	}
	if config.PasswordEnv != "" {
		t.Fatalf("expected empty PasswordEnv, got %q", config.PasswordEnv)
	}
	if config.TLSTag != "" {
		t.Fatalf("expected empty TLSTag, got %q", config.TLSTag)
	}
	if config.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify to be false (zero value)")
	}
}

func TestOpenSearchEndpointConfigPartialOverride(t *testing.T) {
	yamlStr := `
username_env: CUSTOM_USER
`

	var config OpenSearchEndpointConfig
	err := yaml.Unmarshal([]byte(yamlStr), &config)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	// Custom value should be set
	if config.UsernameEnv != "CUSTOM_USER" {
		t.Fatalf("expected UsernameEnv to be 'CUSTOM_USER', got %q", config.UsernameEnv)
	}

	// Defaults should still apply for non-overridden values
	if !config.AuthEnabled {
		t.Fatal("expected default AuthEnabled to be true")
	}
	if config.PasswordEnv != "OPENSEARCH_PASSWORD" {
		t.Fatalf("expected default PasswordEnv, got %q", config.PasswordEnv)
	}
	if config.TLSTag != "tls" {
		t.Fatalf("expected default TLSTag, got %q", config.TLSTag)
	}
}

func TestOpenSearchProbeConfigStructure(t *testing.T) {
	yamlStr := `
discovery:
  meta_cluster_key: CLUSTER
client_config:
  auth_enabled: false
  tls_tag: secure
checks_configs:
  latency_check:
    enabled: true
  durability_check:
    enabled: false
`

	var config OpenSearchProbeConfig
	err := yaml.Unmarshal([]byte(yamlStr), &config)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if config.DiscoveryConfig.MetaClusterKey != "CLUSTER" {
		t.Fatalf("expected MetaClusterKey to be 'CLUSTER', got %q", config.DiscoveryConfig.MetaClusterKey)
	}

	if config.OpenSearchEndpointConfig.AuthEnabled {
		t.Fatal("expected AuthEnabled to be false")
	}

	if config.OpenSearchEndpointConfig.TLSTag != "secure" {
		t.Fatalf("expected TLSTag to be 'secure', got %q", config.OpenSearchEndpointConfig.TLSTag)
	}
}

func TestDefaultOpenSearchEndpointConfig(t *testing.T) {
	defaults := defaultOpenSearchEndpointConfig

	if !defaults.AuthEnabled {
		t.Fatal("expected default AuthEnabled to be true")
	}
	if defaults.UsernameEnv != "OPENSEARCH_USERNAME" {
		t.Fatalf("expected default UsernameEnv to be 'OPENSEARCH_USERNAME', got %q", defaults.UsernameEnv)
	}
	if defaults.PasswordEnv != "OPENSEARCH_PASSWORD" {
		t.Fatalf("expected default PasswordEnv to be 'OPENSEARCH_PASSWORD', got %q", defaults.PasswordEnv)
	}
	if defaults.TLSTag != "tls" {
		t.Fatalf("expected default TLSTag to be 'tls', got %q", defaults.TLSTag)
	}
	if !defaults.InsecureSkipVerify {
		t.Fatal("expected default InsecureSkipVerify to be true")
	}
}

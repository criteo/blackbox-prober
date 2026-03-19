package aerospike

import (
	"fmt"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
	"gopkg.in/yaml.v2"
)

func TestAerospikeEndpointConfigDefaults(t *testing.T) {
	var cfg AerospikeEndpointConfig
	if err := yaml.Unmarshal([]byte("{}"), &cfg); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if cfg.ReauthInterval != 2*time.Minute {
		t.Fatalf("expected default reauth interval to be 2m, got %s", cfg.ReauthInterval)
	}
}

func TestAerospikeEndpointShouldReauth(t *testing.T) {
	now := time.Now()
	endpoint := AerospikeEndpoint{
		ClusterConfig: &AerospikeClientConfig{
			genericConfig: &AerospikeEndpointConfig{
				ReauthInterval: 2 * time.Minute,
			},
		},
		lastReauthAttemptAt: now.Add(-3 * time.Minute),
	}

	if !endpoint.shouldReauth(now) {
		t.Fatal("expected reauth to be required once the interval has elapsed")
	}

	endpoint.lastReauthAttemptAt = now.Add(-1 * time.Minute)
	if endpoint.shouldReauth(now) {
		t.Fatal("expected reauth to stay disabled before the interval elapses")
	}
}

func TestLDAPSpecificResultCode(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &as.AerospikeError{ResultCode: 93})

	rc, rcName, ok := ldapSpecificResultCode(err)
	if !ok {
		t.Fatal("expected LDAP-specific result code to be detected")
	}
	if rc != 93 {
		t.Fatalf("expected LDAP-specific result code 93, got %d", rc)
	}
	if rcName != "AS_SEC_ERR_LDAP_AUTHENTICATION" {
		t.Fatalf("expected LDAP-specific result code name AS_SEC_ERR_LDAP_AUTHENTICATION, got %q", rcName)
	}

	err = fmt.Errorf("wrapped: %w", &as.AerospikeError{ResultCode: ast.NOT_AUTHENTICATED})
	if _, _, ok := ldapSpecificResultCode(err); ok {
		t.Fatal("expected NOT_AUTHENTICATED to stay outside the LDAP auth failure range")
	}
}

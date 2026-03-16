package aerospike

import (
	"testing"
	"time"

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

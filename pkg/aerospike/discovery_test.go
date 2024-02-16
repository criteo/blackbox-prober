package aerospike

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/criteo/blackbox-prober/pkg/discovery"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/go-kit/log"
)

type testEndpoint struct {
	topology.DummyEndpoint
	deadline         time.Time
	CheckCallCount   int
	RefreshCallCount int
}

func (te *testEndpoint) Refresh() error {
	te.RefreshCallCount += 1
	if te.RefreshCallCount%2 == 0 {
		return errors.New("fake err")
	}
	return nil
}

func TestGetNamespacesFromEntry(t *testing.T) {
	entry_newValid := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-monitoring-test1": "true",
			"aerospike-monitoring-test2": "true",
			"aerospike-monitoring-test3": "false",
		},
	}
	expected_newValid := map[string]struct{}{
		"test1": struct{}{},
		"test2": struct{}{},
	}

	entry_oldValid := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-namespaces": "test1;test2;test3",
		},
	}
	expected_oldValid := map[string]struct{}{
		"test1": struct{}{},
		"test2": struct{}{},
		"test3": struct{}{},
	}

	entry_noFallback := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-namespaces":       "test3",
			"aerospike-monitoring-test1": "true",
			"aerospike-monitoring-test2": "false",
			"aerospike-monitoring-test3": "true",
		},
	}
	expected_noFallback := map[string]struct{}{
		"test1": struct{}{},
		"test3": struct{}{},
	}

	entry_fallback := discovery.ServiceEntry{
		Meta: map[string]string{
			"aerospike-namespaces":       "test3",
			"aerospike-monitoring-test1": "true",
			"aerospike-monitoring-test2": "foo",
			"aerospike-monitoring-test3": "false",
		},
	}
	expected_fallback := map[string]struct{}{
		"test3": struct{}{},
	}

	entry_empty := discovery.ServiceEntry{
		Meta: map[string]string{},
	}
	expected_empty := map[string]struct{}{}

	// Minimum config for getNamespacesFromEntry tests
	config := AerospikeProbeConfig{}
	config.AerospikeEndpointConfig = AerospikeEndpointConfig{
		NamespaceMetaKey:       "aerospike-namespaces",
		NamespaceMetaKeyPrefix: "aerospike-monitoring-",
	}

	namespaces := config.getNamespacesFromEntry(log.NewNopLogger(), entry_newValid)
	if !reflect.DeepEqual(namespaces, expected_newValid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_newValid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_oldValid)
	if !reflect.DeepEqual(namespaces, expected_oldValid) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_oldValid'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_noFallback)
	if !reflect.DeepEqual(namespaces, expected_noFallback) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_noFallback'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_fallback)
	if !reflect.DeepEqual(namespaces, expected_fallback) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_fallback'.")
	}
	namespaces = config.getNamespacesFromEntry(log.NewNopLogger(), entry_empty)
	if !reflect.DeepEqual(namespaces, expected_empty) {
		t.Errorf("getNamespacesFromEntry didn't return expected value for entry 'entry_empty'.")
	}
}

package opensearch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/go-kit/log"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// newTestEndpoint creates an OpenSearchEndpoint backed by the given test server.
func newTestEndpoint(t *testing.T, server *httptest.Server, nodeInfoCache map[string]*common.ClusterNodeInfo) *OpenSearchEndpoint {
	t.Helper()
	cfg := opensearchapi.Config{
		Client: opensearch.Config{
			Addresses: []string{server.URL},
		},
	}
	client, err := opensearchapi.NewClient(cfg)
	if err != nil {
		t.Fatalf("failed to create test client: %v", err)
	}
	return &OpenSearchEndpoint{
		Name:          "test-cluster",
		ClusterName:   "test-cluster",
		ClusterLevel:  true,
		Client:        client,
		ClientConfig:  cfg,
		Logger:        log.NewNopLogger(),
		nodeInfoCache: nodeInfoCache,
	}
}

// catNodesMux returns an http.Handler that serves /_cat/nodes with the given node names.
func catNodesMux(t *testing.T, nodeNames []string) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/_cat/nodes", func(w http.ResponseWriter, r *http.Request) {
		type node struct {
			Name string `json:"name"`
			IP   string `json:"ip"`
			Port int    `json:"port,string"`
		}
		nodes := make([]node, len(nodeNames))
		for i, n := range nodeNames {
			nodes[i] = node{Name: n, IP: "10.0.0.1", Port: 9200}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(nodes)
	})
	return mux
}

func TestAvailabilityCheckAllNodesAvailable(t *testing.T) {
	// Reset the metric to avoid pollution from other tests
	nodeAvailability.Reset()

	server := httptest.NewServer(catNodesMux(t, []string{"pod-1", "pod-2"}))
	defer server.Close()

	cache := map[string]*common.ClusterNodeInfo{
		"pod-1": {NodeIP: "10.0.0.1", PodName: "pod-1", NodeFqdn: "node1.example.com"},
		"pod-2": {NodeIP: "10.0.0.2", PodName: "pod-2", NodeFqdn: "node2.example.com"},
	}
	endpoint := newTestEndpoint(t, server, cache)

	err := AvailabilityCheck(endpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Both nodes should have availability = 1
	val1 := testutil.ToFloat64(nodeAvailability.WithLabelValues("test-cluster", "node1.example.com", "pod-1"))
	if val1 != 1 {
		t.Fatalf("expected node availability 1 for pod-1, got %v", val1)
	}
	val2 := testutil.ToFloat64(nodeAvailability.WithLabelValues("test-cluster", "node2.example.com", "pod-2"))
	if val2 != 1 {
		t.Fatalf("expected node availability 1 for pod-2, got %v", val2)
	}
}

func TestAvailabilityCheckPartialNodes(t *testing.T) {
	nodeAvailability.Reset()

	// catNodes only returns pod-1, pod-2 is missing from the cluster
	server := httptest.NewServer(catNodesMux(t, []string{"pod-1"}))
	defer server.Close()

	cache := map[string]*common.ClusterNodeInfo{
		"pod-1": {NodeIP: "10.0.0.1", PodName: "pod-1", NodeFqdn: "node1.example.com"},
		"pod-2": {NodeIP: "10.0.0.2", PodName: "pod-2", NodeFqdn: "node2.example.com"},
	}
	endpoint := newTestEndpoint(t, server, cache)

	err := AvailabilityCheck(endpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val1 := testutil.ToFloat64(nodeAvailability.WithLabelValues("test-cluster", "node1.example.com", "pod-1"))
	if val1 != 1 {
		t.Fatalf("expected node availability 1 for pod-1, got %v", val1)
	}
	// pod-2 not returned by catNodes → should remain 0
	val2 := testutil.ToFloat64(nodeAvailability.WithLabelValues("test-cluster", "node2.example.com", "pod-2"))
	if val2 != 0 {
		t.Fatalf("expected node availability 0 for pod-2, got %v", val2)
	}
}

func TestAvailabilityCheckNoNodesReturned(t *testing.T) {
	nodeAvailability.Reset()

	server := httptest.NewServer(catNodesMux(t, []string{}))
	defer server.Close()

	cache := map[string]*common.ClusterNodeInfo{
		"pod-1": {NodeIP: "10.0.0.1", PodName: "pod-1", NodeFqdn: "node1.example.com"},
	}
	endpoint := newTestEndpoint(t, server, cache)

	err := AvailabilityCheck(endpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := testutil.ToFloat64(nodeAvailability.WithLabelValues("test-cluster", "node1.example.com", "pod-1"))
	if val != 0 {
		t.Fatalf("expected node availability 0 when no nodes returned, got %v", val)
	}
}

func TestAvailabilityCheckUnknownNodeIgnored(t *testing.T) {
	nodeAvailability.Reset()

	// catNodes returns a node not in the cache
	server := httptest.NewServer(catNodesMux(t, []string{"pod-1", "unknown-pod"}))
	defer server.Close()

	cache := map[string]*common.ClusterNodeInfo{
		"pod-1": {NodeIP: "10.0.0.1", PodName: "pod-1", NodeFqdn: "node1.example.com"},
	}
	endpoint := newTestEndpoint(t, server, cache)

	err := AvailabilityCheck(endpoint)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	val := testutil.ToFloat64(nodeAvailability.WithLabelValues("test-cluster", "node1.example.com", "pod-1"))
	if val != 1 {
		t.Fatalf("expected node availability 1 for pod-1, got %v", val)
	}

	// Only 1 metric series should exist for this cluster (the known node)
	count := testutil.CollectAndCount(nodeAvailability)
	if count != 1 {
		t.Fatalf("expected 1 metric series, got %d", count)
	}
}

func TestAvailabilityCheckCatNodesError(t *testing.T) {
	nodeAvailability.Reset()
	clusterErrorsCount.Reset()

	// Return 500 to simulate a failure
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cache := map[string]*common.ClusterNodeInfo{
		"pod-1": {NodeIP: "10.0.0.1", PodName: "pod-1", NodeFqdn: "node1.example.com"},
	}
	endpoint := newTestEndpoint(t, server, cache)

	err := AvailabilityCheck(endpoint)
	if err == nil {
		t.Fatal("expected error when catNodes fails")
	}

	// Cluster error count should have been incremented
	errCount := testutil.ToFloat64(clusterErrorsCount.WithLabelValues("test-cluster"))
	if errCount != 1 {
		t.Fatalf("expected cluster error count 1, got %v", errCount)
	}
}

func TestAvailabilityCheckWrongEndpointType(t *testing.T) {
	err := AvailabilityCheck(&fakeEndpoint{})
	if err == nil {
		t.Fatal("expected error for wrong endpoint type")
	}
	expected := "error: given endpoint is not an opensearch endpoint"
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

// fakeEndpoint implements ProbeableEndpoint but is NOT an OpenSearchEndpoint.
type fakeEndpoint struct{}

func (f *fakeEndpoint) GetHash() string { return "fake" }
func (f *fakeEndpoint) GetName() string { return "fake" }
func (f *fakeEndpoint) IsCluster() bool { return false }
func (f *fakeEndpoint) Connect() error  { return nil }
func (f *fakeEndpoint) Refresh() error  { return nil }
func (f *fakeEndpoint) Close() error    { return nil }

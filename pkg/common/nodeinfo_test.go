package common

import "testing"

func TestNodeInfoCacheReplaceDropsGoneNodes(t *testing.T) {
	cache := NewNodeInfoCache()
	cache.Replace(map[string]*ClusterNodeInfo{
		"10.0.0.1": {NodeName: "10.0.0.1", PodName: "aerospike-0", NodeFqdn: "node-1"},
	})

	// The pod comes back with a new address on another physical node.
	cache.Replace(map[string]*ClusterNodeInfo{
		"10.0.0.2": {NodeName: "10.0.0.2", PodName: "aerospike-0", NodeFqdn: "node-2"},
	})

	info, found := cache.Get("10.0.0.2")
	if !found {
		t.Fatal("expected the new address to be resolved after Replace")
	}
	if info.PodName != "aerospike-0" || info.NodeFqdn != "node-2" {
		t.Fatalf("unexpected node info: %+v", info)
	}
	if _, found := cache.Get("10.0.0.1"); found {
		t.Error("expected the address of the previous pod to be dropped")
	}
	if cache.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", cache.Len())
	}
}

func TestNodeInfoCacheReplaceWithNilEmptiesCache(t *testing.T) {
	cache := NewNodeInfoCache()
	cache.Replace(map[string]*ClusterNodeInfo{"10.0.0.1": {NodeName: "10.0.0.1"}})

	cache.Replace(nil)

	if _, found := cache.Get("10.0.0.1"); found {
		t.Error("expected no entry to be resolved after being replaced by nil")
	}
	if cache.Len() != 0 {
		t.Errorf("expected an empty cache, got %d entries", cache.Len())
	}
}

func TestNilNodeInfoCacheResolvesNothing(t *testing.T) {
	var cache *NodeInfoCache

	if _, found := cache.Get("10.0.0.1"); found {
		t.Error("expected a nil cache to resolve nothing")
	}
	if cache.Len() != 0 {
		t.Errorf("expected a nil cache to be empty, got %d entries", cache.Len())
	}
}

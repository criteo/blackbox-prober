package milvus

import (
	"os"
	"testing"

	"gopkg.in/yaml.v2"
)

// TestShippedConfigResolvesHNSWCollections is the "doesn't break prod" gate
// for switching every probed collection (latency and durability) to HNSW: the
// shipped config file must parse to the new collection names, and
// InitItemsPerCollection (shared by all three collections) must resolve.
func TestShippedConfigResolvesHNSWCollections(t *testing.T) {
	data, err := os.ReadFile("../../configs/milvus/milvus_config.yaml")
	if err != nil {
		t.Fatalf("failed to read shipped config: %v", err)
	}

	conf := MilvusProbeConfig{}
	if err := yaml.Unmarshal(data, &conf); err != nil {
		t.Fatalf("failed to parse shipped config: %v", err)
	}

	ec := conf.MilvusEndpointConfig
	if ec.MonitoringCollectionLatencyRW != "monitoring_latency_hnsw_rw" {
		t.Errorf("MonitoringCollectionLatencyRW = %q, want monitoring_latency_hnsw_rw", ec.MonitoringCollectionLatencyRW)
	}
	if ec.MonitoringCollectionLatencyRO != "monitoring_latency_hnsw_ro" {
		t.Errorf("MonitoringCollectionLatencyRO = %q, want monitoring_latency_hnsw_ro", ec.MonitoringCollectionLatencyRO)
	}
	if ec.MonitoringCollectionDurability != "monitoring_durability_hnsw" {
		t.Errorf("MonitoringCollectionDurability = %q, want monitoring_durability_hnsw", ec.MonitoringCollectionDurability)
	}
	if ec.InitItemsPerCollection != 10000 {
		t.Errorf("InitItemsPerCollection = %d, want 10000", ec.InitItemsPerCollection)
	}
}

// TestDefaultConfigResolvesHNSWCollections confirms a config file that omits
// client_config entirely still gets the new collection names by default
// (defaultMilvusEndpointConfig is only applied when UnmarshalYAML runs, i.e.
// when the client_config key is present at all - covered by the
// shipped-config test above; this covers the standalone zero-config case for
// the struct itself).
func TestDefaultConfigResolvesHNSWCollections(t *testing.T) {
	ec := defaultMilvusEndpointConfig

	if ec.MonitoringCollectionLatencyRW != "monitoring_latency_hnsw_rw" {
		t.Errorf("default MonitoringCollectionLatencyRW = %q, want monitoring_latency_hnsw_rw", ec.MonitoringCollectionLatencyRW)
	}
	if ec.MonitoringCollectionLatencyRO != "monitoring_latency_hnsw_ro" {
		t.Errorf("default MonitoringCollectionLatencyRO = %q, want monitoring_latency_hnsw_ro", ec.MonitoringCollectionLatencyRO)
	}
	if ec.MonitoringCollectionDurability != "monitoring_durability_hnsw" {
		t.Errorf("default MonitoringCollectionDurability = %q, want monitoring_durability_hnsw", ec.MonitoringCollectionDurability)
	}
	if ec.InitItemsPerCollection != 10000 {
		t.Errorf("default InitItemsPerCollection = %d, want 10000", ec.InitItemsPerCollection)
	}
}

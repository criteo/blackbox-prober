package topology

import (
	"sort"
	"testing"
)

func TestDiffWorksWithEmptyMap(t *testing.T) {
	oldMap := NewClusterMap()
	newMap := NewClusterMap()
	oe, ne := oldMap.Diff(&newMap)
	if len(oe) > 0 || len(ne) > 0 {
		t.Error()
	}
}

func TestDiffWorksOnSingleCluster(t *testing.T) {
	oldCluster := NewCluster(DummyEndpoint{Name: "old_cluster", Hash: "old_cluster1"})
	oldCluster.AddEndpoint(DummyEndpoint{Name: "old_endpoint", Hash: "old_endpoint1"})
	oldCluster.AddEndpoint(DummyEndpoint{Name: "old_endpoint", Hash: "old_endpoint2"})
	oldCluster.AddEndpoint(DummyEndpoint{Name: "old_endpoint3", Hash: "old_endpoint3"})

	newCluster := NewCluster(DummyEndpoint{Name: "old_cluster", Hash: "old_cluster1"})
	newCluster.AddEndpoint(DummyEndpoint{Name: "old_endpoint", Hash: "old_endpoint2"})
	newCluster.AddEndpoint(DummyEndpoint{Name: "old_endpoint3", Hash: "old_endpoint4"})

	oldMap := NewClusterMap()
	oldMap.AppendCluster(oldCluster)
	newMap := NewClusterMap()
	newMap.AppendCluster(newCluster)

	oe, ne := oldMap.Diff(&newMap)
	sort.Slice(oe, func(i, j int) bool {
		return oe[i].GetHash() < oe[j].GetHash()
	})
	sort.Slice(ne, func(i, j int) bool {
		return ne[i].GetHash() < ne[j].GetHash()
	})
	if len(oe) != 2 || len(ne) != 1 ||
		oe[0].GetHash() != "old_endpoint1" ||
		oe[1].GetHash() != "old_endpoint3" ||
		ne[0].GetHash() != "old_endpoint4" {
		t.Errorf("Diff missmatch (%s|%s)", oe, ne)
	}
}

func TestDiffWorksOnMultipleClusters(t *testing.T) {
	oldCluster1 := NewCluster(DummyEndpoint{Name: "old_cluster", Hash: "old_cluster1"})
	oldCluster1.AddEndpoint(DummyEndpoint{Name: "old_endpoint", Hash: "old_endpoint1"})
	oldCluster2 := NewCluster(DummyEndpoint{Name: "old_cluster", Hash: "old_cluster2"})
	oldCluster2.AddEndpoint(DummyEndpoint{Name: "old_endpoint", Hash: "old_endpoint2"})

	newCluster2 := NewCluster(DummyEndpoint{Name: "new_cluster", Hash: "new_cluster1"})
	newCluster2.AddEndpoint(DummyEndpoint{Name: "new_endpoint", Hash: "new_endpoint1"})

	oldMap := NewClusterMap()
	oldMap.AppendCluster(oldCluster1)
	oldMap.AppendCluster(oldCluster2)
	newMap := NewClusterMap()
	newMap.AppendCluster(oldCluster1)
	newMap.AppendCluster(newCluster2)

	oe, ne := oldMap.Diff(&newMap)
	sort.Slice(oe, func(i, j int) bool {
		return oe[i].GetHash() < oe[j].GetHash()
	})
	sort.Slice(ne, func(i, j int) bool {
		return ne[i].GetHash() < ne[j].GetHash()
	})
	if len(oe) != 2 || len(ne) != 2 ||
		oe[0].GetHash() != "old_cluster2" ||
		oe[1].GetHash() != "old_endpoint2" ||
		ne[0].GetHash() != "new_cluster1" ||
		ne[1].GetHash() != "new_endpoint1" {
		t.Errorf("Diff missmatch (%s|%s)", oe, ne)
	}
}

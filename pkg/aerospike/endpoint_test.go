package aerospike

import "testing"

func TestHashWorks(t *testing.T) {
	name := "foobar"
	ns := map[string]struct{}{
		"fo":  {},
		"ob":  {},
		"bar": {},
		"00":  {},
	}

	e := AerospikeEndpoint{Name: name, ClusterName: "foo", AutoDiscoverNamespaces: true}
	if e.GetHash() != "foo/foobar" {
		t.Errorf("Hash failed: expected: %s, got: %s", name, e.GetHash())
	}
	e = AerospikeEndpoint{Name: name, ClusterName: "foo", AutoDiscoverNamespaces: false}
	if e.GetHash() != "foo/foobar/ns:[]" {
		t.Errorf("Hash failed: expected: %s, got: %s", "foobar/ns:[]", e.GetHash())
	}
	e = AerospikeEndpoint{Name: name, ClusterName: "foo", AutoDiscoverNamespaces: false, Namespaces: ns}
	if e.GetHash() != "foo/foobar/ns:[00 bar fo ob]" {
		t.Errorf("Hash failed: expected: %s, got: %s (order is important)", "foobar/ns:[00 bar fo ob]", e.GetHash())
	}
}

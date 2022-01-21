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

	e := AerospikeEndpoint{Name: name, AutoDiscoverNamespaces: true}
	if e.GetHash() != "foobar" {
		t.Errorf("Hash failed: expected: %s, got: %s", name, e.GetHash())
	}
	e = AerospikeEndpoint{Name: name, AutoDiscoverNamespaces: false}
	if e.GetHash() != "foobar/ns:[]" {
		t.Errorf("Hash failed: expected: %s, got: %s", "foobar/ns:[]", e.GetHash())
	}
	e = AerospikeEndpoint{Name: name, AutoDiscoverNamespaces: false, namespaces: ns}
	if e.GetHash() != "foobar/ns:[00 bar fo ob]" {
		t.Errorf("Hash failed: expected: %s, got: %s (order is important)", "foobar/ns:[00 bar fo ob]", e.GetHash())
	}
}

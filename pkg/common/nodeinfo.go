package common

import "sync"

type ClusterNodeInfo struct {
	NodeName string // node name as returned by database
	NodeIP   string // node ip
	PodName  string // name of the pod running this Aerospike node
	NodeFqdn string // fqdn of the physical node running the pod
}

// NodeInfoCache holds the node metadata (pod name, physical node fqdn) discovered for one
// cluster, keyed by the address the database client reports for a node.
//
// It is shared between the discovery loop, which replaces its content at every refresh, and
// the running checks, which read it concurrently (namespace fanout). A check endpoint keeps a
// pointer to this cache rather than a snapshot map: the scheduler only restarts a worker when
// the endpoint hash changes, so an endpoint outlives many discovery refreshes. Without this
// indirection a pod that comes back with a new IP would never be resolved again and its
// metrics would stay labelled "unknown".
type NodeInfoCache struct {
	mu        sync.RWMutex
	byAddress map[string]*ClusterNodeInfo
}

func NewNodeInfoCache() *NodeInfoCache {
	return &NodeInfoCache{byAddress: map[string]*ClusterNodeInfo{}}
}

// Replace swaps the whole content of the cache for a freshly discovered one. Entries are
// replaced rather than merged so nodes that disappeared stop being resolved.
func (c *NodeInfoCache) Replace(byAddress map[string]*ClusterNodeInfo) {
	if byAddress == nil {
		byAddress = map[string]*ClusterNodeInfo{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byAddress = byAddress
}

// Get returns the node info known for an address, if any. The returned value must be treated
// as read-only: it is shared by every reader until the next Replace. A nil cache resolves
// nothing, so an endpoint built without discovery data (tests) stays usable.
func (c *NodeInfoCache) Get(address string) (*ClusterNodeInfo, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	info, found := c.byAddress[address]
	return info, found
}

// Len returns the number of known nodes. Mostly useful for tests and logging.
func (c *NodeInfoCache) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.byAddress)
}

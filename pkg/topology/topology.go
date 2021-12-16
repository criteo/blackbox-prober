package topology

// ProbeableEndpoint represent an endpoint that can be checked by the probe
type ProbeableEndpoint interface {
	// Hash used to compare two endpoints (useful for topology updates)
	Hash() string
	// Name of the endpoint useful for metrics and loggings
	GetName() string
	// Connect is called to initiliaze connections to the remote database
	Connect() error
	// Refresh is called to refresh the states of the endpoint
	// Can be used to check for new tables/namespaces/nodes
	Refresh() error
	// Close should terminate all connections to the remote database
	Close() error
}

type ClusterMap struct {
	Clusters map[string]Cluster
}

func NewClusterMap() ClusterMap {
	c := make(map[string]Cluster)
	return ClusterMap{Clusters: c}
}

func (gt *ClusterMap) GetAllClusters() (clusters []Cluster) {
	for _, cluster := range gt.Clusters {
		clusters = append(clusters, cluster)
	}
	return clusters
}

type Cluster struct {
	ClusterEndpoint ProbeableEndpoint
	NodeEndpoints   map[string]ProbeableEndpoint
}

func NewCluster(cluster ProbeableEndpoint) Cluster {
	n := make(map[string]ProbeableEndpoint)
	return Cluster{ClusterEndpoint: cluster, NodeEndpoints: n}
}

func (c *Cluster) AddEndpoint(endpoint ProbeableEndpoint) {
	c.NodeEndpoints[endpoint.GetName()] = endpoint
}

func (c *Cluster) GetAllEndpoints() (endpoints []ProbeableEndpoint) {
	for _, endpoint := range c.NodeEndpoints {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

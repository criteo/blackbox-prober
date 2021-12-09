package topology

// ProbeableEndpoint represent an endpoint that can be checked by the probe
type ProbeableEndpoint interface {
	Hash() string
	GetName() string
}

type ClusterMap struct {
	Clusters map[string]Cluster
}

func NewClusterMap() ClusterMap {
	c := make(map[string]Cluster)
	return ClusterMap{Clusters: c}
}

func (gt *ClusterMap) FetchAllClusters() (clusters []Cluster) {
	for _, cluster := range gt.Clusters {
		clusters = append(clusters, cluster)
	}
	return clusters
}

type Cluster struct {
	Cluster ProbeableEndpoint
	Nodes   map[string]ProbeableEndpoint
}

func NewCluster(cluster ProbeableEndpoint) Cluster {
	n := make(map[string]ProbeableEndpoint)
	return Cluster{Cluster: cluster, Nodes: n}
}

func (c *Cluster) AddEndpoint(endpoint ProbeableEndpoint) {
	c.Nodes[endpoint.GetName()] = endpoint
}

func (c *Cluster) FetchAllEndpoints() (endpoints []ProbeableEndpoint) {
	for _, endpoint := range c.Nodes {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

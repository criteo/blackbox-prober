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
	t := make(map[string]Cluster)
	return ClusterMap{Clusters: t}
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

func (ct *Cluster) FetchAllEndpoints() (endpoints []ProbeableEndpoint) {
	for _, endpoint := range ct.Nodes {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

package topology

// ProbeableEndpoint represent an endpoint that can be checked by the probe
type ProbeableEndpoint interface {
	// Hash used to compare two endpoints (useful for topology updates)
	GetHash() string
	// Name of the endpoint useful for metrics and loggings
	GetName() string
	// IsCluster return true if the endpoint is cluster endpoint, false if node
	IsCluster() bool
	// Connect is called to initiliaze connections to the remote database
	Connect() error
	// Refresh is called to refresh the states of the endpoint
	// Can be used to check for new tables/namespaces/nodes
	Refresh() error
	// Close should terminate all connections to the remote database
	Close() error
}

// DummyEndpoint is a fake ProbeableEndpoint that don't do anything
// Useful for testing
type DummyEndpoint struct {
	Name string
	Hash string
}

func (d DummyEndpoint) GetHash() string {
	return d.Hash
}

func (d DummyEndpoint) GetName() string {
	return d.Name
}

func (d DummyEndpoint) IsCluster() bool {
	return false
}

func (d DummyEndpoint) Connect() error {
	return nil
}

func (d DummyEndpoint) Refresh() error {
	return nil
}

func (d DummyEndpoint) Close() error {
	return nil
}

type ClusterMap struct {
	Clusters map[string]Cluster
}

func NewClusterMap() ClusterMap {
	c := make(map[string]Cluster)
	return ClusterMap{Clusters: c}
}

func (gt *ClusterMap) AppendCluster(cluster Cluster) {
	gt.Clusters[cluster.ClusterEndpoint.GetHash()] = cluster
}

func (gt *ClusterMap) GetAllClusters() (clusters []Cluster) {
	for _, cluster := range gt.Clusters {
		clusters = append(clusters, cluster)
	}
	return clusters
}

// Diff make the intersection between two clusters and return:
// oldEndpoints: the endpoints that were present in the old cluster map but not in the new one
// newEndpoints: the endpoints that are present in the new cluster map but not in the old one
func (oldMap *ClusterMap) Diff(newMap *ClusterMap) (oldEndpoints []ProbeableEndpoint, newEndpoints []ProbeableEndpoint) {
	oldClusters := []string{}
	newClusters := []string{}
	// Diff of clusters
	for clusterName := range oldMap.Clusters {
		newCluster, ok := newMap.Clusters[clusterName]
		if !ok {
			oldClusters = append(oldClusters, clusterName)
			continue
		}
		oldCluster := oldMap.Clusters[clusterName]

		for nodeName := range oldCluster.NodeEndpoints {
			if _, ok := newCluster.NodeEndpoints[nodeName]; !ok {
				oldEndpoints = append(oldEndpoints, oldCluster.NodeEndpoints[nodeName])
			}
		}
		for nodeName := range newCluster.NodeEndpoints {
			if _, ok := oldCluster.NodeEndpoints[nodeName]; !ok {
				newEndpoints = append(newEndpoints, newCluster.NodeEndpoints[nodeName])
			}
		}
	}
	for cluster := range newMap.Clusters {
		if _, ok := oldMap.Clusters[cluster]; !ok {
			newClusters = append(newClusters, cluster)
		}
	}

	// If a cluster is gone, we return all its endpoints
	for _, clusterName := range oldClusters {
		oldEndpoints = append(oldEndpoints, oldMap.Clusters[clusterName].ClusterEndpoint)
		for _, e := range oldMap.Clusters[clusterName].NodeEndpoints {
			oldEndpoints = append(oldEndpoints, e)
		}
	}

	for _, clusterName := range newClusters {
		newEndpoints = append(newEndpoints, newMap.Clusters[clusterName].ClusterEndpoint)
		for _, e := range newMap.Clusters[clusterName].NodeEndpoints {
			newEndpoints = append(newEndpoints, e)
		}
	}
	return oldEndpoints, newEndpoints
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
	c.NodeEndpoints[endpoint.GetHash()] = endpoint
}

func (c *Cluster) GetAllEndpoints() (endpoints []ProbeableEndpoint) {
	for _, endpoint := range c.NodeEndpoints {
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

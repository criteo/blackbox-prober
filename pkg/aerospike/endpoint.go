package aerospike

import (
	"crypto/tls"
	"fmt"
	"regexp"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	as "github.com/aerospike/aerospike-client-go/v7"
)

var (
	setExtractionRegex, _ = regexp.Compile("ns=([a-zA-Z0-9]+):")
)

var clusterStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_aerospike_client_cluster_stats",
	Help: "Cluster aggregated metrics from the go aerospike client",
}, []string{"cluster", "probe_endpoint", "namespace", "name"})

type AerospikeEndpoint struct {
	Name         string
	ClusterLevel bool
	ClusterName  string
	Client       *as.Client
	Config       AerospikeClientConfig
	Logger       log.Logger
	Namespace    string
}

func (e *AerospikeEndpoint) GetHash() string {
	return fmt.Sprintf("%s/%s/ns:%s", e.ClusterName, e.Name, e.Namespace)
}

func (e *AerospikeEndpoint) GetName() string {
	return e.Name
}

func (e *AerospikeEndpoint) IsCluster() bool {
	return e.ClusterLevel
}

func (e *AerospikeEndpoint) setMetricFromASStats(stats map[string]interface{}, key string) {
	val, ok := stats[key]
	if !ok {
		return
	}

	value, ok := val.(float64)
	if !ok {
		return
	}
	clusterStats.WithLabelValues(e.ClusterName, e.GetName(), e.Namespace, key).Set(value)
}

func (e *AerospikeEndpoint) refreshMetrics() {
	stats, err := e.Client.Stats()
	cluster_stats := stats["cluster-aggregated-stats"].(map[string]interface{})
	if err != nil {
		level.Error(e.Logger).Log("msg", "Failed to pull metrics from aerospike client", "err", err)
		return
	}
	e.setMetricFromASStats(cluster_stats, "open-connections")
	e.setMetricFromASStats(cluster_stats, "closed-connections")
	e.setMetricFromASStats(cluster_stats, "connections-attempts")
	e.setMetricFromASStats(cluster_stats, "connections-successful")
	e.setMetricFromASStats(cluster_stats, "connections-failed")
	e.setMetricFromASStats(cluster_stats, "connections-pool-empty")
	e.setMetricFromASStats(cluster_stats, "node-added-count")
	e.setMetricFromASStats(cluster_stats, "node-removed-count")
	e.setMetricFromASStats(cluster_stats, "partition-map-updates")
	e.setMetricFromASStats(cluster_stats, "tends-total")
	e.setMetricFromASStats(cluster_stats, "tends-successful")
	e.setMetricFromASStats(cluster_stats, "tends-failed")
}

func (e *AerospikeEndpoint) Connect() error {
	clientPolicy := as.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = e.Config.genericConfig.ConnectionQueueSize
	clientPolicy.OpeningConnectionThreshold = e.Config.genericConfig.OpeningConnectionThreshold
	clientPolicy.MinConnectionsPerNode = e.Config.genericConfig.MinConnectionsPerNode
	clientPolicy.TendInterval = e.Config.genericConfig.TendInterval

	if e.Config.tlsEnabled {
		// Setup TLS Config
		tlsConfig := &tls.Config{
			InsecureSkipVerify:       e.Config.genericConfig.TLSSkipVerify,
			PreferServerCipherSuites: true,
		}
		clientPolicy.TlsConfig = tlsConfig
	}

	if e.Config.authEnabled {
		if e.Config.genericConfig.AuthExternal {
			clientPolicy.AuthMode = as.AuthModeExternal
		} else {
			clientPolicy.AuthMode = as.AuthModeInternal
		}

		clientPolicy.User = e.Config.username
		clientPolicy.Password = e.Config.password
	}

	client, err := as.NewClientWithPolicyAndHost(clientPolicy, &e.Config.host)
	if err != nil {
		return err
	}
	e.Client = client
	e.Refresh()
	return nil
}

func (e *AerospikeEndpoint) Refresh() error {
	e.refreshMetrics()
	return nil
}

func (e *AerospikeEndpoint) Close() error {
	if e != nil {
		e.Client.Close()
	}
	return nil
}

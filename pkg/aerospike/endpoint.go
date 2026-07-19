package aerospike

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	as "github.com/aerospike/aerospike-client-go/v8"
)

var clusterStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_aerospike_client_cluster_stats",
	Help: "Cluster aggregated metrics from the go aerospike client",
}, []string{"cluster", "probe_endpoint", "name"})

type AerospikeEndpoint struct {
	Name          string
	ClusterLevel  bool
	ClusterName   string
	Client        *as.Client
	ClusterConfig *AerospikeClientConfig
	Logger        log.Logger
	Namespaces    []string // Namespaces monitored on this cluster
}

func (e *AerospikeEndpoint) GetHash() string {
	// NB: The namespace set is part of the hash so a change in monitored namespaces triggers a
	// worker restart. Namespaces are kept sorted at construction so the hash is stable.
	return fmt.Sprintf("%s/%s/ns:%s", e.ClusterConfig.clusterName, e.Name, strings.Join(e.Namespaces, ","))
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

	clusterStats.WithLabelValues(e.ClusterConfig.clusterName, e.GetName(), key).Set(value)
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

	// Dynamically adjust the pool from the expected probe concurrency. Latency and durability can
	// run independently, each with bounded namespace fanout; add headroom for refresh/tend traffic.
	namespaceParallelism := namespaceCheckParallelism(len(e.Namespaces))
	expectedConcurrency := 2*namespaceParallelism + 1
	clientPolicy.MinConnectionsPerNode = 2 * expectedConcurrency
	if clientPolicy.ConnectionQueueSize <= clientPolicy.MinConnectionsPerNode {
		clientPolicy.ConnectionQueueSize = clientPolicy.MinConnectionsPerNode + 1
	}

	clientPolicy.TendInterval = e.ClusterConfig.genericConfig.TendInterval
	// Timeout bounds connection establishment: the TCP dial and the initial socket
	// deadline (incl. TLS handshake) of a freshly opened connection. The driver defaults
	// it to 30s, so tends stall ~30s dialing each unreachable node during a roll-restart.
	// (Steady-state reads/writes and LDAP login use their own per-command timeouts.)
	clientPolicy.Timeout = e.ClusterConfig.genericConfig.ConnectionTimeout

	if e.ClusterConfig.tlsEnabled {
		// Setup TLS Config
		tlsConfig := &tls.Config{
			InsecureSkipVerify:       e.ClusterConfig.genericConfig.TLSSkipVerify,
			PreferServerCipherSuites: true,
		}
		clientPolicy.TlsConfig = tlsConfig
	}

	if e.ClusterConfig.authEnabled {
		if e.ClusterConfig.genericConfig.AuthExternal {
			clientPolicy.AuthMode = as.AuthModeExternal
		} else {
			clientPolicy.AuthMode = as.AuthModeInternal
		}

		clientPolicy.User = e.ClusterConfig.username
		clientPolicy.Password = e.ClusterConfig.password
	}

	client, err := as.NewClientWithPolicyAndHost(clientPolicy, e.ClusterConfig.hosts...)
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
	if e != nil && e.Client != nil {
		e.Client.Close()
	}
	return nil
}

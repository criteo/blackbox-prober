package aerospike

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	as "github.com/aerospike/aerospike-client-go/v8"
)

var clusterStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_aerospike_client_cluster_stats",
	Help: "Cluster aggregated metrics from the go aerospike client",
}, []string{"cluster", "probe_endpoint", "namespace", "name"})

type AerospikeEndpoint struct {
	Name                string
	ClusterLevel        bool
	ClusterName         string
	Client              *as.Client
	ClusterConfig       *AerospikeClientConfig
	Logger              log.Logger
	Namespace           string
	lastReauthAttemptAt time.Time
}

func (e *AerospikeEndpoint) GetHash() string {
	return fmt.Sprintf("%s/%s/ns:%s", e.ClusterConfig.clusterName, e.Name, e.Namespace)
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
	clusterStats.WithLabelValues(e.ClusterConfig.clusterName, e.GetName(), e.Namespace, key).Set(value)
}

func (e *AerospikeEndpoint) refreshMetrics() {
	// Do note that these are client-side metrics, which means they are periodically lost as we now
	// periodically re-create the client to detect auth issues.

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

func (e *AerospikeEndpoint) connectClient() error {
	e.lastReauthAttemptAt = time.Now()

	clientPolicy := as.NewClientPolicy()
	clientPolicy.ConnectionQueueSize = e.ClusterConfig.genericConfig.ConnectionQueueSize
	clientPolicy.OpeningConnectionThreshold = e.ClusterConfig.genericConfig.OpeningConnectionThreshold
	clientPolicy.MinConnectionsPerNode = e.ClusterConfig.genericConfig.MinConnectionsPerNode
	clientPolicy.TendInterval = e.ClusterConfig.genericConfig.TendInterval

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

	client, err := as.NewClientWithPolicyAndHost(clientPolicy, &e.ClusterConfig.host)
	if err != nil {
		return err
	}
	e.Client = client
	return nil
}

func (e *AerospikeEndpoint) shouldReauth(now time.Time) bool {
	reauthInterval := e.ClusterConfig.genericConfig.ReauthInterval
	return reauthInterval > 0 && !e.lastReauthAttemptAt.IsZero() && now.Sub(e.lastReauthAttemptAt) >= reauthInterval
}

func (e *AerospikeEndpoint) reauth() error {
	e.Close()
	return e.connectClient()
}

func (e *AerospikeEndpoint) EnsureFreshClient() error {
	if e.shouldReauth(time.Now()) {
		level.Debug(e.Logger).Log("msg", "Reauthenticating Aerospike client")
		if err := e.reauth(); err != nil {
			return err
		}
	}
	return nil
}

func (e *AerospikeEndpoint) Connect() error {
	if err := e.connectClient(); err != nil {
		return err
	}
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

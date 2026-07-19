package aerospike

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/criteo/blackbox-prober/pkg/common"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"golang.org/x/sync/errgroup"
)

var (
	ASSuffix = utils.MetricSuffix + "_aerospike"
)

var authCheckTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: ASSuffix + "_auth_check_total",
	Help: "Total number of authentication attempts per node and outcome. " +
		"status: success | auth_failure | connection_error",
}, []string{"cluster", "endpoint", "node_id", "status"})

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    ASSuffix + "_op_latency",
	Help:    "Latency for operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "namespace", "node", "pod", "cluster", "node_id"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: ASSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "namespace", "node", "pod", "cluster", "node_id"})

var durabilityExpectedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_durability_expected_items",
	Help: "Total number of items expected for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

var durabilityFoundItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_durability_found_items",
	Help: "Total number of items found with correct value for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

var durabilityCorruptedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: ASSuffix + "_durability_corrupted_items",
	Help: "Total number of items found to be corrupted for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

// namespaceCheckParallelism keeps the total check fanout at or below the old rough maximum.
// With independent scheduler checks, latency and durability may run at the same time; limiting
// each check to ceil(namespaces/2) means both checks together run at most one namespace lane per
// monitored namespace. DurabilityPrepare uses a lower limit to avoid startup write bursts.
func namespaceCheckParallelism(namespaceCount int) int {
	if namespaceCount <= 1 {
		return 1
	}
	return (namespaceCount + 1) / 2
}

// forEachNamespace runs fn for every monitored namespace of the endpoint, with a bounded number
// of namespaces active at once. All namespaces run to completion; the first returned error is an
// execution error for the scheduler. Domain health, such as missing durability keys, is reported
// through check-specific metrics instead of necessarily being returned as an error.
func forEachNamespace(e *AerospikeEndpoint, parallelism int, fn func(namespace string) error) error {
	if parallelism < 1 {
		parallelism = 1
	}
	if len(e.Namespaces) > 0 && parallelism > len(e.Namespaces) {
		parallelism = len(e.Namespaces)
	}

	var g errgroup.Group
	g.SetLimit(parallelism)
	for _, namespace := range e.Namespaces {
		namespace := namespace
		g.Go(func() error {
			return fn(namespace)
		})
	}
	return g.Wait()
}

// getWriteNode find the node against which the write will be made
func getWriteNode(c *as.Client, policy *as.WritePolicy, key *as.Key) (*as.Node, error) {
	partition, err := as.PartitionForWrite(c.Cluster(), &policy.BasePolicy, key)
	if err != nil {
		return nil, err
	}
	node, err := partition.GetNodeWrite(c.Cluster())
	if err != nil {
		return nil, err
	}
	return node, nil
}

func ObserveOpLatency(op func() error, labels []string) error {
	start := time.Now()
	err := op()
	opLatency.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	if err != nil {
		opFailuresTotal.WithLabelValues(labels...).Inc()
	} else {
		opFailuresTotal.WithLabelValues(labels...).Add(0) // Force creation of metric
	}
	return err
}

func keyAsStr(key *as.Key) string {
	return fmt.Sprintf("[namespace=%s set=%s key=%s]", key.Namespace(), key.SetName(), key.Value())
}

// Hash a string and return the resulting hex value
func hash(str string) string {
	hasher := sha1.New()
	hasher.Write([]byte(str))

	return hex.EncodeToString(hasher.Sum(nil))
}

func LatencyCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}
	return forEachNamespace(e, namespaceCheckParallelism(len(e.Namespaces)), func(namespace string) error {
		return latencyCheckNamespace(e, namespace)
	})
}

func latencyCheckNamespace(e *AerospikeEndpoint, namespace string) error {
	keyPrefix := e.ClusterConfig.genericConfig.LatencyKeyPrefix

	policy := as.NewWritePolicy(0, 3600)                             // Expire after one hour if the delete didn't work
	policy.MaxRetries = 0                                            // Ensure we never retry (0 is default Client value in v7)
	policy.ReplicaPolicy = as.MASTER                                 // Read are always done on master (SEQUENCE is default Client value in v7)
	policy.TotalTimeout = e.ClusterConfig.genericConfig.TotalTimeout // 0 is default Client value in v7
	// Do not wait until timeout if connections cannot be open
	policy.ExitFastOnExhaustedConnectionPool = e.ClusterConfig.genericConfig.ExitFastOnExhaustedConnectionPool

	// FIXME: despite code intent we have no guarantee to hit all nodes of the cluster in a single iteration
	// Instead we should generate one key per partition (number of partitions is known and constant with aerospike => 4096 partitions)
	// These keys could be generated once in probe lifetime (partition id is the first 12 bits of the digest of the key) and then reused
	// at each latency check.
	for range e.Client.Cluster().GetNodes() { // scale the number of latency checks to the number of nodes
		key, as_err := as.NewKey(namespace, e.ClusterConfig.genericConfig.MonitoringSet, fmt.Sprintf("%s%s", keyPrefix, utils.RandomHex(20)))
		if as_err != nil {
			return as_err
		}
		val := as.BinMap{
			"val": utils.RandomHex(1024),
		}

		node, err := getWriteNode(e.Client, policy, key)
		if err != nil {
			return errors.Wrapf(err, "error when trying to find node for: %s", keyAsStr(key))
		}

		// lookup node fqdn and pod name associated to aerospike endpoint
		nodeInfo := &common.ClusterNodeInfo{NodeName: node.GetHost().Name, NodeFqdn: "unknown", PodName: "unknown"}
		if ni, found := e.ClusterConfig.nodeInfoCache[node.GetHost().Name]; found {
			nodeInfo = ni
		}

		// PUT OPERATION
		labels := []string{"put", node.GetHost().Name, namespace, nodeInfo.NodeFqdn, nodeInfo.PodName, e.ClusterConfig.clusterName, node.GetName()}

		// PUT OPERATION
		opPut := func() error {
			return e.Client.Put(policy, key, val)
		}

		err = ObserveOpLatency(opPut, labels)
		if err != nil {
			return errors.Wrapf(err, "record put failed for: %s", keyAsStr(key))
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record put: %s", keyAsStr(key)))

		// GET OPERATION
		labels[0] = "get"
		opGet := func() error {
			recVal, err := e.Client.Get(&policy.BasePolicy, key)
			if err != nil {
				return err
			}
			if recVal == nil {
				return errors.Errorf("Record not found after being put")
			}
			if recVal.Bins["val"] != val["val"] {
				return errors.Errorf("Get succeeded but there is a missmatch between server value {%s} and pushed value", recVal.Bins["val"])
			}
			return err
		}

		err = ObserveOpLatency(opGet, labels)
		if err != nil {
			return errors.Wrapf(err, "record get failed for: %s", keyAsStr(key))
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record get: %s", keyAsStr(key)))

		// DELETE OPERATION
		labels[0] = "delete"
		opDelete := func() error {
			existed, as_err := e.Client.Delete(policy, key)
			if !existed {
				return errors.Errorf("Delete succeeded but there was no data to delete")
			}
			if as_err != nil {
				return as_err
			} else {
				return nil
			}
		}

		err = ObserveOpLatency(opDelete, labels)
		if err != nil {
			return errors.Wrapf(err, "record delete failed for: %s", keyAsStr(key))
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record delete: %s", keyAsStr(key)))
	}
	return nil
}

func DurabilityPrepare(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}
	return forEachNamespace(e, 1, func(namespace string) error {
		return durabilityPrepareNamespace(e, namespace)
	})
}

func durabilityPrepareNamespace(e *AerospikeEndpoint, namespace string) error {
	policy := as.NewWritePolicy(0, as.TTLDontExpire)                 // No expiration
	policy.MaxRetries = 2                                            // We can retry for durability (0 is default Client value in v7)
	policy.TotalTimeout = e.ClusterConfig.genericConfig.TotalTimeout // 0 is default Client value in v7
	// Do not wait until timeout if connections cannot be open
	policy.ExitFastOnExhaustedConnectionPool = e.ClusterConfig.genericConfig.ExitFastOnExhaustedConnectionPool
	keyRange := e.ClusterConfig.genericConfig.DurabilityKeyTotal
	keyPrefix := e.ClusterConfig.genericConfig.DurabilityKeyPrefix
	// allPushedFlag indicate if a probe have pushed all data once
	// The value contains information about the data pushed (scheme:key_range)
	// scheme: the format of the data (v1=shasum of the key)
	// key_range: the number of keys
	// If the probe find a missmatch it will repush the keys
	expectedAllPushedFlagVal := fmt.Sprintf("%s:%d", "v1", keyRange) // v1 represents the format of the data stored.

	// allPushedFlag indicate if a probe have pushed all data once
	allPushedFlag, err := as.NewKey(namespace, e.ClusterConfig.genericConfig.MonitoringSet, fmt.Sprintf("%s%s", keyPrefix, "all_pushed_flag"))
	if err != nil {
		return err
	}

	recVal, err := e.Client.Get(&policy.BasePolicy, allPushedFlag)

	if err != nil && !err.Matches(as.ErrKeyNotFound.ResultCode) {
		return err
	}
	// If the flag was found we skip the init as it has already been done
	if recVal != nil && recVal.Bins["val"] == expectedAllPushedFlagVal {
		return nil
	}

	for i := 0; i < keyRange; i++ {
		keyName := fmt.Sprintf("%s%d", keyPrefix, i)
		key, err := as.NewKey(namespace, e.ClusterConfig.genericConfig.MonitoringSet, keyName)
		if err != nil {
			return err
		}

		val := as.BinMap{
			"val": hash(keyName),
		}

		err = e.Client.Put(policy, key, val)
		if err != nil {
			return errors.Wrapf(err, "record put failed for: %s", keyAsStr(key))
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record durability put: %s (%s)", keyAsStr(key), val["val"]))
	}

	allPushedFlagVal := as.BinMap{
		"val": expectedAllPushedFlagVal,
	}
	err = e.Client.Put(policy, allPushedFlag, allPushedFlagVal)
	if err != nil {
		return errors.Wrapf(err, "Push flag put failed for: %s", keyAsStr(allPushedFlag))
	}

	return nil
}

func DurabilityCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}
	return forEachNamespace(e, namespaceCheckParallelism(len(e.Namespaces)), func(namespace string) error {
		return durabilityCheckNamespace(e, namespace)
	})
}

// durabilityCheckNamespace completes a sweep and publishes durability gauges. Missing records,
// corrupted values, and read failures are represented by durability_found_items and
// durability_corrupted_items; they do not fail the scheduler check unless the sweep itself cannot
// execute far enough to publish those gauges.
func durabilityCheckNamespace(e *AerospikeEndpoint, namespace string) error {
	policy := as.NewPolicy()
	policy.MaxRetries = 2                                            // 2 is default Client value in v7
	policy.ReplicaPolicy = as.SEQUENCE                               // SEQUENCE is default Client value (alternate across master/replica in case of errors)
	policy.TotalTimeout = e.ClusterConfig.genericConfig.TotalTimeout // 0 is default Client value in v7
	// Do not wait until timeout if connections cannot be open
	policy.ExitFastOnExhaustedConnectionPool = e.ClusterConfig.genericConfig.ExitFastOnExhaustedConnectionPool
	keyRange := e.ClusterConfig.genericConfig.DurabilityKeyTotal
	keyPrefix := e.ClusterConfig.genericConfig.DurabilityKeyPrefix

	total_found_items := 0.0
	total_corrupted_items := 0.0
	for i := 0; i < keyRange; i++ {
		keyName := fmt.Sprintf("%s%d", keyPrefix, i)
		key, err := as.NewKey(namespace, e.ClusterConfig.genericConfig.MonitoringSet, keyName)
		if err != nil {
			return err
		}

		recVal, err := e.Client.Get(policy, key)
		if err != nil {
			level.Error(e.Logger).Log("msg", fmt.Sprintf("Error while fetching record: %s", keyAsStr(key)), "err", err)
			continue
		}
		if recVal.Bins["val"] != hash(keyName) {
			level.Warn(e.Logger).Log("msg",
				fmt.Sprintf("Get successful but the data didn't match what was expected got: '%s', expected: '%s' (for %s)",
					recVal.Bins["val"], hash(keyName), keyAsStr(key)))
			total_corrupted_items += 1
		} else {
			total_found_items += 1
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("durability record validated: %s (%s)", keyAsStr(key), recVal.Bins["val"]))
	}
	durabilityExpectedItems.WithLabelValues(namespace, e.ClusterConfig.clusterName, e.GetName()).Set(float64(keyRange))
	durabilityFoundItems.WithLabelValues(namespace, e.ClusterConfig.clusterName, e.GetName()).Set(total_found_items)
	durabilityCorruptedItems.WithLabelValues(namespace, e.ClusterConfig.clusterName, e.GetName()).Set(total_corrupted_items)
	return nil
}

// authStatus values double as the `status` label on auth_check_total.
const (
	authStatusSuccess   = "success"
	authStatusAuthFail  = "auth_failure"
	authStatusConnError = "connection_error"

	maxAuthCheckParallelism = 8
)

func authCheckParallelism(targetCount int) int {
	if targetCount <= 1 {
		return 1
	}
	if targetCount < maxAuthCheckParallelism {
		return targetCount
	}
	return maxAuthCheckParallelism
}

// authNodeKey identifies a probed node by its stable aerospike node id and its current IP.
// It is comparable, so it doubles as the map key when reconciling live vs. departed series.
type authNodeKey struct {
	nodeId string
	ip     string
}

// authTarget is a single node to probe with a fresh authentication. It embeds authNodeKey
// (the identity used for metrics) and adds the host to dial (needed for its port / TLS name).
type authTarget struct {
	authNodeKey
	host *as.Host
}

// authTargets and freshLogin are indirected through package variables so unit tests can
// mock them without a live cluster.
var (
	// authTargets returns the currently live nodes to probe.
	authTargets = func(e *AerospikeEndpoint) []authTarget {
		nodes := e.Client.Cluster().GetNodes()
		targets := make([]authTarget, 0, len(nodes))
		for _, node := range nodes {
			host := node.GetHost()
			targets = append(targets, authTarget{
				authNodeKey: authNodeKey{nodeId: node.GetName(), ip: host.Name},
				host:        host,
			})
		}
		return targets
	}

	// freshLogin opens a brand-new connection to host and performs a full authentication
	// handshake (bypassing the pool and the cached session token), then discards it. It
	// returns the auth_check status: success, auth_failure (login rejected) or
	// connection_error (could not connect). It reuses the exact policy the live client was
	// built with (auth mode, credentials, TLS, timeouts), so the fresh login mirrors what a
	// new client would do. Both the dial (policy.Timeout) and the login (policy.LoginTimeout)
	// are time-bounded, so a hung auth backend surfaces as an error.
	freshLogin = func(e *AerospikeEndpoint, host *as.Host) (string, error) {
		policy := e.Client.Cluster().ClientPolicy()
		conn, err := as.NewConnection(&policy, host)
		if err != nil {
			return authStatusConnError, err
		}
		defer conn.Close()
		if err := conn.Login(&policy); err != nil {
			return authStatusAuthFail, err
		}
		return authStatusSuccess, nil
	}
)

// AuthCheck verifies that a brand-new client could still authenticate against the cluster,
// contrary to latency and durability checks that reuse an already-authenticated client, and
// that keeps working even after server-side auth breaks (LDAP down, credentials revoked,
// security config change), masking real outage. This check catches that blind spot by doing
// a fresh login on every live node at each interval.
func AuthCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}

	// Nothing to verify if authentication is disabled for this cluster.
	if !e.ClusterConfig.authEnabled {
		return nil
	}

	// Build the live-node set up front (in this goroutine) so the concurrent probes below
	// don't write the map. Fresh logins bypass the client pool, so bound the number of
	// concurrent dials/logins to avoid a burst against Aerospike or the auth backend.
	targets := authTargets(e)
	current := make(map[authNodeKey]struct{}, len(targets))
	for _, target := range targets {
		current[target.authNodeKey] = struct{}{}
	}

	var g errgroup.Group
	g.SetLimit(authCheckParallelism(len(targets)))
	for _, target := range targets {
		target := target
		g.Go(func() error {
			status, err := freshLogin(e, target.host)
			authCheckTotal.WithLabelValues(e.ClusterConfig.clusterName, target.ip, target.nodeId, status).Inc()

			switch status {
			case authStatusAuthFail:
				level.Error(e.Logger).Log("msg", fmt.Sprintf("Fresh authentication failed on %s (%s)", target.nodeId, target.ip), "err", err)
				return errors.Wrapf(err, "fresh authentication failed on %s (%s)", target.nodeId, target.ip)
			case authStatusConnError:
				// Connectivity problem (node down / network), not an authentication failure.
				// Counted but NOT returned, so the scheduler's auth_check failure signal stays
				// specific to authentication (connectivity is already surfaced elsewhere).
				level.Error(e.Logger).Log("msg", fmt.Sprintf("Failed to open connection for auth check on %s (%s)", target.nodeId, target.ip), "err", err)
			}
			return nil
		})
	}
	// Wait returns the first authentication failure (if any); connection errors return nil
	// above, so the scheduler's _scheduler_check_failure{check_name="auth_check"} means
	// "auth is broken".
	err := g.Wait()

	e.cleanupAuthMetrics(current)
	return err
}

// cleanupAuthMetrics deletes auth_check_total series for nodes that are no longer live (pod
// restart with a new IP, node replacement, downscale). Without this the departed node's
// counters would flatline forever, false-firing "counter must keep increasing" alerts and
// growing cardinality as node IPs churn.
//
// Rather than tracking previously-seen nodes, it reconciles against the live set passed in:
// it reads back this endpoint's currently-exported series and drops any whose node is absent
// from `current`. The whole channel is drained before deleting because Collect holds a read
// lock for its full duration and DeletePartialMatch needs the write lock.
func (e *AerospikeEndpoint) cleanupAuthMetrics(current map[authNodeKey]struct{}) {
	ch := make(chan prometheus.Metric)
	go func() {
		authCheckTotal.Collect(ch)
		close(ch)
	}()

	stale := make(map[authNodeKey]struct{})
	for m := range ch {
		var dm dto.Metric
		if err := m.Write(&dm); err != nil {
			continue
		}
		var cluster, endpoint, nodeId string
		for _, lp := range dm.GetLabel() {
			switch lp.GetName() {
			case "cluster":
				cluster = lp.GetValue()
			case "endpoint":
				endpoint = lp.GetValue()
			case "node_id":
				nodeId = lp.GetValue()
			}
		}
		// Only touch series belonging to this cluster (the vec is shared across clusters).
		if cluster != e.ClusterConfig.clusterName {
			continue
		}
		key := authNodeKey{nodeId: nodeId, ip: endpoint}
		if _, ok := current[key]; !ok {
			stale[key] = struct{}{}
		}
	}

	for key := range stale {
		authCheckTotal.DeletePartialMatch(prometheus.Labels{
			"cluster":  e.ClusterConfig.clusterName,
			"endpoint": key.ip,
			"node_id":  key.nodeId,
		})
	}
}

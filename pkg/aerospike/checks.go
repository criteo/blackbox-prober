package aerospike

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ASSuffix = utils.MetricSuffix + "_aerospike"
)

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    ASSuffix + "_op_latency",
	Help:    "Latency for operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "namespace", "node", "cluster", "id"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: ASSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "namespace", "node", "cluster", "id"})

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

	keyPrefix := e.ClusterConfig.genericConfig.LatencyKeyPrefix

	policy := as.NewWritePolicy(0, 3600)                             // Expire after one hour if the delete didn't work
	policy.MaxRetries = 0                                            // Ensure we never retry (0 is default Client value in v7)
	policy.ReplicaPolicy = as.MASTER                                 // Read are always done on master (SEQUENCE is default Client value in v7)
	policy.TotalTimeout = e.ClusterConfig.genericConfig.TotalTimeout // 0 is default Client value in v7
	// Do not wait until timeout if connections cannot be open
	policy.ExitFastOnExhaustedConnectionPool = e.ClusterConfig.genericConfig.ExitFastOnExhaustedConnectionPool

	for range e.Client.Cluster().GetNodes() { // scale the number of latency checks to the number of nodes
		// TODO configurable set
		key, as_err := as.NewKey(e.Namespace, e.ClusterConfig.genericConfig.MonitoringSet, fmt.Sprintf("%s%s", keyPrefix, utils.RandomHex(20)))
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

		nodeInfo := &AerospikeNodeInfo{NodeName: node.GetName(), NodeFqdn: "unknown", PodName: "unknown"}
		if ni, found := e.ClusterConfig.nodeInfoCache[node.GetName()]; found {
			nodeInfo = ni
		}

		// PUT OPERATION
		labels := []string{"put", node.GetHost().Name, e.Namespace, nodeInfo.NodeFqdn, e.ClusterConfig.clusterName, node.GetName()}

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

	policy := as.NewWritePolicy(0, as.TTLDontExpire)                 // No expiration
	policy.MaxRetries = 2                                            // We can retry for durability (0 is default Client value in v7)
	policy.TotalTimeout = e.ClusterConfig.genericConfig.TotalTimeout // 0 is default Client value in v7
	keyRange := e.ClusterConfig.genericConfig.DurabilityKeyTotal
	keyPrefix := e.ClusterConfig.genericConfig.DurabilityKeyPrefix
	// allPushedFlag indicate if a probe have pushed all data once
	// The value contains information about the data pushed (scheme:key_range)
	// scheme: the format of the data (v1=shasum of the key)
	// key_range: the number of keys
	// If the probe find a missmatch it will repush the keys
	expectedAllPushedFlagVal := fmt.Sprintf("%s:%d", "v1", keyRange) // v1 represents the format of the data stored.

	// allPushedFlag indicate if a probe have pushed all data once
	allPushedFlag, err := as.NewKey(e.Namespace, e.ClusterConfig.genericConfig.MonitoringSet, fmt.Sprintf("%s%s", keyPrefix, "all_pushed_flag"))
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
		key, err := as.NewKey(e.Namespace, e.ClusterConfig.genericConfig.MonitoringSet, keyName)
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

	policy := as.NewPolicy()
	policy.MaxRetries = 2                                            // 2 is default Client value in v7
	policy.ReplicaPolicy = as.SEQUENCE                               // SEQUENCE is default Client value (alternate across master/replica in case of errors)
	policy.TotalTimeout = e.ClusterConfig.genericConfig.TotalTimeout // 0 is default Client value in v7
	keyRange := e.ClusterConfig.genericConfig.DurabilityKeyTotal
	keyPrefix := e.ClusterConfig.genericConfig.DurabilityKeyPrefix

	total_found_items := 0.0
	total_corrupted_items := 0.0
	for i := 0; i < keyRange; i++ {
		keyName := fmt.Sprintf("%s%d", keyPrefix, i)
		key, err := as.NewKey(e.Namespace, e.ClusterConfig.genericConfig.MonitoringSet, keyName)
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
	durabilityExpectedItems.WithLabelValues(e.Namespace, e.ClusterConfig.clusterName, e.GetName()).Set(float64(keyRange))
	durabilityFoundItems.WithLabelValues(e.Namespace, e.ClusterConfig.clusterName, e.GetName()).Set(total_found_items)
	durabilityCorruptedItems.WithLabelValues(e.Namespace, e.ClusterConfig.clusterName, e.GetName()).Set(total_corrupted_items)
	return nil
}

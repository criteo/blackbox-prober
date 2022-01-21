package aerospike

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go"
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
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: ASSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

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

	keyPrefix := e.Config.genericConfig.LatencyKeyPrefix

	policy := as.NewWritePolicy(0, 3600) // Expire after one hour if the delete didn't work
	policy.MaxRetries = 0                // Ensure we never retry
	policy.ReplicaPolicy = as.MASTER     // Read are always done on master

	for namespace := range e.namespaces {
		// TODO configurable set
		key, err := as.NewKey(namespace, e.Config.genericConfig.MonitoringSet, fmt.Sprintf("%s%s", keyPrefix, utils.RandomHex(20)))
		if err != nil {
			return err
		}
		val := as.BinMap{
			"val": utils.RandomHex(1024),
		}

		node, err := getWriteNode(e.Client, policy, key)
		if err != nil {
			return errors.Wrapf(err, "error when trying to find node for: %s", keyAsStr(key))
		}

		// PUT OPERATION
		labels := []string{"put", node.GetHost().Name, namespace, e.ClusterName, node.GetName()}

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
			existed, err := e.Client.Delete(policy, key)
			if !existed {
				err = errors.Errorf("Delete succeeded but there was no data to delete")
			}
			return err
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

	policy := as.NewWritePolicy(0, 0) // No expiration
	keyRange := e.Config.genericConfig.DurabilityKeyTotal
	keyPrefix := e.Config.genericConfig.DurabilityKeyPrefix
	// allPushedFlag indicate if a probe have pushed all data once
	// The value contains information about the data pushed (scheme:key_range)
	// scheme: the format of the data (v1=shasum of the key)
	// key_range: the number of keys
	// If the probe find a missmatch it will repush the keys
	expectedAllPushedFlagVal := fmt.Sprintf("%s:%d", "v1", keyRange) // v1 represents the format of the data stored.

	for namespace := range e.namespaces {
		// allPushedFlag indicate if a probe have pushed all data once
		allPushedFlag, err := as.NewKey(namespace, e.Config.genericConfig.MonitoringSet, fmt.Sprintf("%s%s", keyPrefix, "all_pushed_flag"))
		if err != nil {
			return err
		}

		recVal, err := e.Client.Get(&policy.BasePolicy, allPushedFlag)
		if err != nil && err.Error() != "Key not found" {
			level.Debug(e.Logger).Log("msg", "called")
			return err
		}
		// If the flag was found we skip the init as it has already been done
		if recVal != nil && recVal.Bins["val"] == expectedAllPushedFlagVal {
			continue
		}

		for i := 0; i < keyRange; i++ {
			keyName := fmt.Sprintf("%s%d", keyPrefix, i)
			key, err := as.NewKey(namespace, e.Config.genericConfig.MonitoringSet, keyName)
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

	}
	return nil
}

func DurabilityCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}

	policy := as.NewPolicy()
	keyRange := e.Config.genericConfig.DurabilityKeyTotal
	keyPrefix := e.Config.genericConfig.DurabilityKeyPrefix
	total_found_items := 0.0
	total_corrupted_items := 0.0

	for namespace := range e.namespaces {
		for i := 0; i < keyRange; i++ {
			keyName := fmt.Sprintf("%s%d", keyPrefix, i)
			key, err := as.NewKey(namespace, e.Config.genericConfig.MonitoringSet, keyName)
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
		durabilityExpectedItems.WithLabelValues(namespace, e.ClusterName, e.GetName()).Set(float64(keyRange))
		durabilityFoundItems.WithLabelValues(namespace, e.ClusterName, e.GetName()).Set(total_found_items)
		durabilityCorruptedItems.WithLabelValues(namespace, e.ClusterName, e.GetName()).Set(total_corrupted_items)
	}
	return nil
}

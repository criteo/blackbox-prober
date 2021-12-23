package main

import (
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

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    ASSuffix + "_op_latency",
	Help:    "Latency for operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: ASSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

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
	}
	return err
}

func keyAsStr(key *as.Key) string {
	return fmt.Sprintf("[namespace=%s set=%s key=%s]", key.Namespace(), key.SetName(), key.Value())
}

func LatencyCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*AerospikeEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an aerospike endpoint")
	}

	policy := as.NewWritePolicy(0, 0)
	policy.MaxRetries = 0            // Ensure we never retry
	policy.ReplicaPolicy = as.MASTER // Read are always done on master

	for namespace := range e.namespaces {
		// TODO configurable set
		key, err := as.NewKey(namespace, "monitoring", utils.RandomHex(20))
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
		labels := []string{"put", node.GetHost().Name, namespace, e.GetName(), node.GetName()}

		opPut := func() error {
			return e.Client.Put(policy, key, val)
		}

		ObserveOpLatency(opPut, labels)
		if err != nil {
			return errors.Wrapf(err, "record put failed for: %s", keyAsStr(key))
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record put: %s", keyAsStr(key)))

		// GET OPERATION
		labels[0] = "get"
		opGet := func() error {
			recVal, err := e.Client.Get(&policy.BasePolicy, key)
			if recVal.Bins["val"] != val["val"] {
				err = errors.Errorf("Get succeeded but there is a missmatch between server value {%s} and pushed value", recVal.Bins["val"])
			}
			return err
		}

		ObserveOpLatency(opGet, labels)
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

		ObserveOpLatency(opDelete, labels)
		if err != nil {
			return errors.Wrapf(err, "record delete failed for: %s", keyAsStr(key))
		}
		level.Debug(e.Logger).Log("msg", fmt.Sprintf("record delete: %s", keyAsStr(key)))
	}
	return nil
}

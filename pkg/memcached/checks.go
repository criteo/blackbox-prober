package memcached

import (
	"bytes"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MemcachedSuffix = utils.MetricSuffix + "_memcached"
)

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    MemcachedSuffix + "_op_latency",
	Help:    "Latency for operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "cluster"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: MemcachedSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "cluster"})

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

func LatencyCheck(p topology.ProbeableEndpoint) error {
	m, ok := p.(*MemcachedEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not a Memcached endpoint")
	}

	keyPrefix := m.Config.MemcachedEndpointConfig.LatencyKeyPrefix
	key := fmt.Sprintf("%s%s", keyPrefix, utils.RandomHex(20))
	val := []byte(utils.RandomHex(1024))

	item := memcache.Item{
		Key:        key,
		Value:      val,
		Expiration: 300,
	}

	// PUT OPERATION
	labels := []string{"put", m.Name, m.ClusterName}
	opPut := func() error {
		return m.Client.Set(&item)
	}
	err := ObserveOpLatency(opPut, labels)
	if err != nil {
		return errors.Wrapf(err, "record put failed for: %s", key)
	}
	level.Debug(m.Logger).Log("msg", fmt.Sprintf("record put: %s", key))

	// GET OPERATION
	labels[0] = "get"

	opGet := func() error {
		item, err := m.Client.Get(key)
		if err == memcache.ErrCacheMiss {
			return errors.Errorf("Record not found after being put")
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(item.Value, val) {
			return errors.Errorf("Get succeeded but there is a missmatch between server value and pushed value on key: %s", key)
		}
		return err
	}
	err = ObserveOpLatency(opGet, labels)
	if err != nil {
		return errors.Wrapf(err, "record get failed for: %s", key)
	}
	level.Debug(m.Logger).Log("msg", fmt.Sprintf("record get: %s", key))

	// DELETE OPERATION
	labels[0] = "delete"
	opDelete := func() error {
		return m.Client.Delete(key)
	}
	err = ObserveOpLatency(opDelete, labels)
	if err != nil {
		return errors.Wrapf(err, "record delete failed for: %s", key)
	}
	level.Debug(m.Logger).Log("msg", fmt.Sprintf("record delete: %s", key))

	return nil
}

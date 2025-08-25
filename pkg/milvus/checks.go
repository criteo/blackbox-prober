package milvus

import (
	"time"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MVSuffix = utils.MetricSuffix + "_milvus"
)

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    MVSuffix + "_op_latency",
	Help:    "Latency for operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: MVSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "namespace", "cluster", "id"})

var durabilityExpectedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: MVSuffix + "_durability_expected_items",
	Help: "Total number of items expected for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

var MilvusSuffix = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: MVSuffix + "_durability_found_items",
	Help: "Total number of items found with correct value for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

var durabilityCorruptedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: MVSuffix + "_durability_corrupted_items",
	Help: "Total number of items found to be corrupted for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

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
	e, _ := p.(*MilvusEndpoint)
	level.Info(e.Logger).Log("msg", "Milvus latency check to be implemented")
	return nil
}

func DurabilityPrepare(p topology.ProbeableEndpoint) error {
	return nil
}

func DurabilityCheck(p topology.ProbeableEndpoint) error {
	e, _ := p.(*MilvusEndpoint)
	level.Info(e.Logger).Log("msg", "Milvus durability check to be implemented")
	return nil
}

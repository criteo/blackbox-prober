package triton

import (
	"fmt"
	"time"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TritonSuffix = utils.MetricSuffix + "_triton"
)

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    TritonSuffix + "_op_latency",
	Help:    "Latency for inference operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "cluster", "model", "pod"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: TritonSuffix + "_op_latency_failures",
	Help: "Total number of inference operations that resulted in failure",
}, []string{"operation", "endpoint", "cluster", "model", "pod"})

var modelActiveGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: TritonSuffix + "_model_active",
	Help: "Whether a model has external (non-probe) traffic. 1=active, 0=inactive",
}, []string{"cluster", "endpoint", "model", "pod"})

// ObserveOpLatency measures the duration of an operation and records it in the histogram.
// Only successful operations are recorded in the latency histogram to avoid skewing metrics
// with failure latencies (e.g., timeouts).
func ObserveOpLatency(op func() error, labels []string) error {
	start := time.Now()
	err := op()
	if err != nil {
		opFailuresTotal.WithLabelValues(labels...).Inc()
	} else {
		opLatency.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
		opFailuresTotal.WithLabelValues(labels...).Add(0) // Initialize metric to 0 for these labels
	}
	return err
}

// LatencyCheck performs inference requests against all discovered models
// and measures the latency of each operation.
func LatencyCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*TritonEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not a Triton endpoint")
	}

	models := e.GetModels()
	if len(models) == 0 {
		level.Debug(e.Logger).Log("msg", "no models available for latency check", "endpoint", e.Name)
		return nil
	}

	batchSize := int64(1)
	onlyProbeActive := false
	if e.Config != nil {
		if e.Config.BatchSize > 0 {
			batchSize = e.Config.BatchSize
		}
		onlyProbeActive = e.Config.SkipInactiveModels.Enable
	}

	for modelKey, modelInfo := range models {
		// Skip inactive models if activity filtering is enabled
		if onlyProbeActive && !modelInfo.IsActive {
			level.Debug(e.Logger).Log("msg", "skipping inactive model", "model", modelKey)
			continue
		}

		// Skip models that can't be probed with random data
		if canProbe, reason := CanProbe(modelInfo); !canProbe {
			level.Debug(e.Logger).Log("msg", "skipping model", "model", modelKey, "reason", reason)
			continue
		}

		labels := []string{"infer", e.Address, e.ClusterName, modelInfo.Name, e.PodName}

		opInfer := func() error {
			_, err := e.Infer(modelInfo, batchSize)
			if err != nil {
				return fmt.Errorf("inference failed for model %s: %w", modelInfo.Name, err)
			}
			// TODO : Check response
			// if len(resp.GetOutputs()) == 0 {
			// return fmt.Errorf("inference returned no outputs for model %s", modelInfo.Name)
			// }
			return nil
		}
		err := ObserveOpLatency(opInfer, labels)
		if err != nil {
			level.Error(e.Logger).Log(
				"msg", "latency check failed",
				"model", modelKey,
				"endpoint", e.Name,
				"err", err,
			)
			// Continue to next model instead of failing entirely
			continue
		}

		level.Debug(e.Logger).Log("msg", "inference successful", "model", modelKey, "endpoint", e.Name)
	}

	return nil
}

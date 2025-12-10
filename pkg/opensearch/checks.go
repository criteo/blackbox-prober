package opensearch

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OSSuffix    = utils.MetricSuffix + "_opensearch"
	clusterLock = new(sync.RWMutex)
)

const (
	LATENCY_INDEX_NAME         = ".monitoring_latency"
	LATENCY_INDEX_NUM_SHARDS   = 1
	LATENCY_INDEX_NUM_REPLICAS = 1
	LATENCY_DOCUMENT_ID_PREFIX = "latency_document_1_"
	LATENCY_DOCUMENT_CONTENT   = "While the exact amount of text data in a kilobyte (KB) or megabyte (MB) can vary depending on the nature of a document, a kilobyte can hold about half of a page of text, while a megabyte holds about 500 pages of text."

	DURABILITY_INDEX_NAME         = ".monitoring_durability"
	DURABILITY_INDEX_NUM_SHARDS   = 1
	DURABILITY_INDEX_NUM_REPLICAS = 1
	DURABILITY_DOCUMENT_ID_PREFIX = "durability_document_1_"
	DURABILITY_DOCUMENT_COUNT     = 10000
	DURABILITY_DOCUMENT_CONTENT   = "While the exact amount of text data in a kilobyte (KB) or megabyte (MB) can vary depending on the nature of a document, a kilobyte can hold about half of a page of text, while a megabyte holds about 500 pages of text."
)

var opLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    OSSuffix + "_op_latency",
	Help:    "Latency for operations",
	Buckets: utils.MetricHistogramBuckets,
}, []string{"operation", "endpoint", "cluster"})

var opFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: OSSuffix + "_op_latency_failures",
	Help: "Total number of operations that resulted in failure",
}, []string{"operation", "endpoint", "cluster"})

var opDurabilityExpectedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: OSSuffix + "_durability_expected_items",
	Help: "Total number of items expected in the durability index",
}, []string{"cluster"})

var opDurabilityFoundItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: OSSuffix + "_durability_found_items",
	Help: "Total number of items found in the durability index",
}, []string{"cluster"})

var opDurabilityCorruptedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: OSSuffix + "_durability_corrupted_items",
	Help: "Total number of corrupted items in the durability index",
}, []string{"cluster"})

// ObserveOpLatency measures the latency of the given operation function 'op' and records it in the opLatency histogram.
// It also increments the opFailuresTotal counter if the operation results in an error.
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

func LatencyPrepare(p topology.ProbeableEndpoint) error {
	e, ok := p.(*OpenSearchEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an opensearch endpoint")
	}

	// Check if latency index exists, create it if it does not
	exists, err := e.checkIndexExists(LATENCY_INDEX_NAME)
	if err != nil {
		return fmt.Errorf("error checking if latency index exists: %v", err)
	}
	if !exists {
		level.Info(e.Logger).Log("msg", fmt.Sprintf("Latency index %s does not exist, creating it", LATENCY_INDEX_NAME))
		err = e.createIndex(LATENCY_INDEX_NAME, LATENCY_INDEX_NUM_SHARDS, LATENCY_INDEX_NUM_REPLICAS)
		if err != nil {
			return fmt.Errorf("error creating latency index: %v", err)
		}
	}

	return nil
}

func LatencyCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*OpenSearchEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an opensearch endpoint")
	}

	// CRAFT DOCUMENT ID
	documentID := LATENCY_DOCUMENT_ID_PREFIX + uuid.New().String()

	// CREATE DOCUMENT
	labels := []string{"put", e.Name, e.ClusterName}
	opPut := func() error {
		return e.insertDocument(LATENCY_INDEX_NAME, documentID, LATENCY_DOCUMENT_CONTENT)
	}

	err := ObserveOpLatency(opPut, labels)
	if err != nil {
		return errors.Wrapf(err, "fail to create document %s: %s", documentID, err)
	}
	level.Debug(e.Logger).Log("msg", fmt.Sprintf("document created: %s", documentID))

	// GET DOCUMENT
	labels = []string{"get", e.Name, e.ClusterName}
	opGet := func() error {
		content, err := e.getDocument(LATENCY_INDEX_NAME, documentID)
		if err != nil {
			return err
		}
		if content != LATENCY_DOCUMENT_CONTENT {
			return fmt.Errorf("retrieved document content does not match expected content")
		}

		level.Debug(e.Logger).Log("msg", fmt.Sprintf("document get: %s", documentID))

		return nil
	}
	err = ObserveOpLatency(opGet, labels)
	if err != nil {
		return errors.Wrapf(err, "record get failed for: %s", documentID)
	}

	// DELETE DOCUMENT
	labels = []string{"delete", e.Name, e.ClusterName}
	opDelete := func() error {
		return e.deleteDocument(LATENCY_INDEX_NAME, documentID)
	}

	err = ObserveOpLatency(opDelete, labels)
	if err != nil {
		return errors.Wrapf(err, "record delete failed for: %s", documentID)
	}
	level.Debug(e.Logger).Log("msg", fmt.Sprintf("document delete: %s", documentID))

	// CAT HEALTH
	labels = []string{"cat_health", e.Name, e.ClusterName}
	opCat := func() error {
		return e.catHealth()
	}

	err = ObserveOpLatency(opCat, labels)
	if err != nil {
		return errors.Wrapf(err, "failed to get cat health for %s: %s", e.Name, err)
	}
	level.Debug(e.Logger).Log("msg", fmt.Sprintf("cat health success for: %s", e.Name))

	return nil
}

func DurabilityPrepare(p topology.ProbeableEndpoint) error {
	e, ok := p.(*OpenSearchEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an opensearch endpoint")
	}

	// First we acquire a cluster-level lock to avoid race conditions
	clusterLock.Lock()
	defer clusterLock.Unlock()

	// Check if durability index exists, create it if it does not
	exists, err := e.checkIndexExists(DURABILITY_INDEX_NAME)
	if err != nil {
		return fmt.Errorf("error checking if durability index exists: %v", err)
	}
	if !exists {
		level.Info(e.Logger).Log("msg", fmt.Sprintf("Durability index %s does not exist, creating it", DURABILITY_INDEX_NAME))
		err = e.createIndex(DURABILITY_INDEX_NAME, DURABILITY_INDEX_NUM_SHARDS, DURABILITY_INDEX_NUM_REPLICAS)
		if err != nil {
			return fmt.Errorf("error creating durability index: %v", err)
		}

		// Create all the durability documents
		err = e.insertDocumentBulk(DURABILITY_INDEX_NAME, DURABILITY_DOCUMENT_COUNT, DURABILITY_DOCUMENT_ID_PREFIX, DURABILITY_DOCUMENT_CONTENT)
		if err != nil {
			return fmt.Errorf("error creating durability documents: %v", err)
		}
	}

	return nil
}

func DurabilityCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*OpenSearchEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not an opensearch endpoint")
	}

	// Prepare metrics labels
	labels := []string{e.ClusterName}

	// Get all documents
	files, err := e.getAllIndexDocuments(DURABILITY_INDEX_NAME)
	if err != nil {
		return fmt.Errorf("error retrieving durability documents: %v", err)
	}

	// Init coorrupted items metric to 0
	opDurabilityCorruptedItems.WithLabelValues(labels...).Set(0)

	// Iterate over retrieved documents and check their content
	for id, content := range files {
		expectedContent := []byte(DURABILITY_DOCUMENT_CONTENT)
		if string(content) != string(expectedContent) {
			level.Error(e.Logger).Log("msg", fmt.Sprintf("corrupted document detected on document %s: '%s'!='%s'", id, content, expectedContent))
			opDurabilityCorruptedItems.WithLabelValues(labels...).Inc()
		}
	}

	// Update metrics
	opDurabilityExpectedItems.WithLabelValues(labels...).Set(float64(DURABILITY_DOCUMENT_COUNT))
	opDurabilityFoundItems.WithLabelValues(labels...).Set(float64(len(files)))

	// Check all durability documents
	return nil
}

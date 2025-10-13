package milvus

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/criteo/blackbox-prober/pkg/topology"
	"github.com/criteo/blackbox-prober/pkg/utils"
	"github.com/go-kit/log/level"

	mvcol "github.com/milvus-io/milvus/client/v2/column"
	mvindex "github.com/milvus-io/milvus/client/v2/index"
	milvusclient "github.com/milvus-io/milvus/client/v2/milvusclient"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/pkg/errors"
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

var durabilityFoundItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: MVSuffix + "_durability_found_items",
	Help: "Total number of items found with correct value for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

var durabilityCorruptedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: MVSuffix + "_durability_corrupted_items",
	Help: "Total number of items found to be corrupted for durability",
}, []string{"namespace", "cluster", "probe_endpoint"})

const (
	// Vector setup
	DIMENSION   = 100
	TOP_K       = 1
	METRIC_TYPE = entity.COSINE

	// Init
	MAX_VARCHAR_LEN         = 256
	INITIAL_VALUE_HEX_BYTES = 128 // 128 hex chars <= 256
)

func ObserveOpLatency(op func() error, labels []string) error {
	start := time.Now()
	err := op()
	opLatency.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	if err != nil {
		opFailuresTotal.WithLabelValues(labels...).Inc()
	} else {
		opFailuresTotal.WithLabelValues(labels...).Add(0)
	}
	return err
}

func hash(str string) string {
	hasher := sha1.New()
	hasher.Write([]byte(str))
	return hex.EncodeToString(hasher.Sum(nil))
}

func generateRandomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}

func normalizeVector(vec []float32) []float32 {
	var ss float32
	for _, v := range vec {
		ss += v * v
	}
	if ss == 0 {
		return vec
	}
	invMag := float32(1 / math.Sqrt(float64(ss)))
	out := make([]float32, len(vec))
	for i, v := range vec {
		out[i] = v * invMag
	}
	return out
}

func sampleUniqueInts(k, n int) []int64 {
	if k > n {
		k = n
	}
	set := make(map[int64]struct{}, k)
	for len(set) < k {
		set[int64(rand.Intn(n))] = struct{}{}
	}
	out := make([]int64, 0, k)
	for id := range set {
		out = append(out, id)
	}
	return out
}

func joinInts(a []int64) string {
	s := make([]string, len(a))
	for i, v := range a {
		s[i] = fmt.Sprint(v)
	}
	return strings.Join(s, ",")
}

// ensureMonitoringDB always uses the default DB and creates it if missing.
func ensureMonitoringDB(ctx context.Context, e *MilvusEndpoint) error {
	db := e.Config.MonitoringDatabase
	tctx, cancel := context.WithTimeout(ctx, e.Config.CreateDatabaseTimeout)
	defer cancel()

	// Try to use it.
	if err := e.Client.UseDatabase(tctx, milvusclient.NewUseDatabaseOption(db)); err != nil {
		// Create then use.
		if cerr := e.Client.CreateDatabase(tctx, milvusclient.NewCreateDatabaseOption(db)); cerr != nil {
			level.Debug(e.Logger).Log("msg", "create database returned", "db", db, "err", cerr)
		}
		if uerr := e.Client.UseDatabase(tctx, milvusclient.NewUseDatabaseOption(db)); uerr != nil {
			return errors.Wrapf(uerr, "use database %q", db)
		}
	}

	return nil
}

// ensureCollection creates schema+index and loads the collection in current DB.
func ensureCollection(ctx context.Context, e *MilvusEndpoint, collectionName string) error {
	has, err := e.Client.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		return errors.Wrap(err, "failed to check if collection exists")
	}

	if has {
		ls, err := e.Client.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(collectionName))
		if err != nil {
			return errors.Wrap(err, "failed to get load state")
		}
		if ls.State == entity.LoadStateNotLoad {
			tctx, cancel := context.WithTimeout(ctx, e.Config.LoadTimeout)
			defer cancel()
			loadTask, err := e.Client.LoadCollection(tctx, milvusclient.NewLoadCollectionOption(collectionName))
			if err != nil {
				return errors.Wrap(err, "failed to load collection")
			}
			if err := loadTask.Await(tctx); err != nil {
				return errors.Wrap(err, "failed to await load task")
			}
		}
		return nil
	}

	schema := entity.NewSchema().
		WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(false)).
		WithField(entity.NewField().WithName("key").WithDataType(entity.FieldTypeVarChar).WithMaxLength(MAX_VARCHAR_LEN)).
		WithField(entity.NewField().WithName("value").WithDataType(entity.FieldTypeVarChar).WithMaxLength(MAX_VARCHAR_LEN)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(DIMENSION))

	if err := e.Client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema)); err != nil {
		return errors.Wrap(err, "failed to create collection")
	}
	level.Info(e.Logger).Log("msg", "Created collection", "collection", collectionName)

	{
		tctx, cancel := context.WithTimeout(ctx, e.Config.IndexTimeout)
		defer cancel()
		idx := mvindex.NewFlatIndex(METRIC_TYPE)
		createIdxTask, err := e.Client.CreateIndex(tctx, milvusclient.NewCreateIndexOption(collectionName, "vector", idx))
		if err != nil {
			return errors.Wrap(err, "failed to create index")
		}
		if err := createIdxTask.Await(tctx); err != nil {
			return errors.Wrap(err, "failed to await index creation")
		}
	}
	level.Info(e.Logger).Log("msg", "Created FLAT index", "collection", collectionName)

	{
		tctx, cancel := context.WithTimeout(ctx, e.Config.LoadTimeout)
		defer cancel()
		loadTask, err := e.Client.LoadCollection(tctx, milvusclient.NewLoadCollectionOption(collectionName))
		if err != nil {
			return errors.Wrap(err, "failed to load collection")
		}
		if err := loadTask.Await(tctx); err != nil {
			return errors.Wrap(err, "failed to await load task")
		}
	}
	level.Info(e.Logger).Log("msg", "Loaded collection", "collection", collectionName)

	return nil
}

// initCollectionIfNeeded populates a collection with INIT_ITEMS_PER_COL items once.
func initCollectionIfNeeded(ctx context.Context, e *MilvusEndpoint, collectionName, keyPrefix string) error {
	flagKey := fmt.Sprintf("%s%s", keyPrefix, e.Config.InitFlagKey)
	expectedFlagValue := fmt.Sprintf("v1:%d", e.Config.InitItemsPerCollection)

	qr, err := e.Client.Query(ctx, milvusclient.NewQueryOption(collectionName).
		WithFilter(fmt.Sprintf(`key == "%s"`, flagKey)).
		WithOutputFields("value").
		WithConsistencyLevel(entity.ClStrong))
	if err == nil {
		if vcol := qr.GetColumn("value"); vcol != nil {
			if vv := vcol.(*mvcol.ColumnVarChar).Data(); len(vv) > 0 && vv[0] == expectedFlagValue {
				level.Info(e.Logger).Log("msg", "Collection already initialized", "collection", collectionName)
				return nil
			}
		}
	}

	level.Info(e.Logger).Log("msg", "Initializing collection items", "collection", collectionName, "count", e.Config.InitItemsPerCollection)

	batchSize := 1000
	for base := 0; base < e.Config.InitItemsPerCollection; base += batchSize {
		end := base + batchSize
		if end > e.Config.InitItemsPerCollection {
			end = e.Config.InitItemsPerCollection
		}
		n := end - base

		ids := make([]int64, n)
		keys := make([]string, n)
		values := make([]string, n)
		vecs := make([][]float32, n)

		for i := 0; i < n; i++ {
			id := int64(base + i)
			k := fmt.Sprintf("%s%d", keyPrefix, base+i)
			ids[i] = id
			keys[i] = k
			values[i] = hash(k)
			vecs[i] = normalizeVector(generateRandomVector(DIMENSION))
		}

		if _, err := e.Client.Insert(ctx,
			milvusclient.NewColumnBasedInsertOption(collectionName).
				WithInt64Column("id", ids).
				WithVarcharColumn("key", keys).
				WithVarcharColumn("value", values).
				WithFloatVectorColumn("vector", DIMENSION, vecs),
		); err != nil {
			return errors.Wrapf(err, "insert batch %d-%d", base, end)
		}
	}

	{
		vec := normalizeVector(generateRandomVector(DIMENSION))
		if _, err := e.Client.Insert(ctx,
			milvusclient.NewColumnBasedInsertOption(collectionName).
				WithInt64Column("id", []int64{int64(e.Config.InitItemsPerCollection)}).
				WithVarcharColumn("key", []string{flagKey}).
				WithVarcharColumn("value", []string{expectedFlagValue}).
				WithFloatVectorColumn("vector", DIMENSION, [][]float32{vec}),
		); err != nil {
			return errors.Wrap(err, "insert init flag")
		}
	}

	// Manual flush is OK because this is only done once
	{
		tctx, cancel := context.WithTimeout(ctx, e.Config.InitialFlushTimeout)
		defer cancel()
		ft, err := e.Client.Flush(tctx, milvusclient.NewFlushOption(collectionName))
		if err != nil {
			return errors.Wrap(err, "initCollectionIfNeeded flush")
		}
		if err := ft.Await(tctx); err != nil {
			return errors.Wrap(err, "await initCollectionIfNeeded flush")
		}
	}

	return nil
}

// LatencyPrepare ensures DB and latency collections exist and initializes the RO latency collection.
func LatencyPrepare(p topology.ProbeableEndpoint) error {
	e, ok := p.(*MilvusEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not a milvus endpoint")
	}
	ctx := context.Background()

	if err := ensureMonitoringDB(ctx, e); err != nil {
		return err
	}
	for _, col := range []string{e.Config.MonitoringCollectionLatencyRW, e.Config.MonitoringCollectionLatencyRO} {
		if err := ensureCollection(ctx, e, col); err != nil {
			return errors.Wrapf(err, "ensure %s", col)
		}
		if err := initCollectionIfNeeded(ctx, e, col, e.Config.LatencyInitKeyPrefix); err != nil {
			return errors.Wrapf(err, "init latency %s", col)
		}
	}

	return nil
}

// LatencyCheck: RW collection insert/search/delete. Then search-only on latency RO.
func LatencyCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*MilvusEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not a milvus endpoint")
	}

	ctx := context.Background()
	if err := ensureMonitoringDB(ctx, e); err != nil {
		return err
	}

	// RW path
	{
		col := e.Config.MonitoringCollectionLatencyRW
		if err := ensureCollection(ctx, e, col); err != nil {
			return errors.Wrap(err, "ensure latency RW collection")
		}

		now := time.Now().UnixNano()
		insertCount := e.Config.LatencyRWInsertPerCheck
		ids := make([]int64, insertCount)
		vecs := make([][]float32, insertCount)
		keys := make([]string, insertCount)
		vals := make([]string, insertCount)
		for i := 0; i < insertCount; i++ {
			ids[i] = now + int64(i)
			vecs[i] = normalizeVector(generateRandomVector(DIMENSION))
			keys[i] = e.Config.LatencyRWKeyPrefix + utils.RandomHex(20)
			vals[i] = utils.RandomHex(INITIAL_VALUE_HEX_BYTES)
		}

		labels := []string{"insert", e.Name, e.Config.MonitoringDatabase, e.ClusterName, e.Name}
		opInsert := func() error {
			if _, err := e.Client.Insert(ctx,
				milvusclient.NewColumnBasedInsertOption(col).
					WithInt64Column("id", ids).
					WithVarcharColumn("key", keys).
					WithVarcharColumn("value", vals).
					WithFloatVectorColumn("vector", DIMENSION, vecs),
			); err != nil {
				return err
			}

			return nil
		}
		if err := ObserveOpLatency(opInsert, labels); err != nil {
			return errors.Wrap(err, "insert batch")
		}

		labels[0] = "search"
		opSearch := func() error {
			qvecs := make([]entity.Vector, len(vecs))
			for i := range vecs {
				qvecs[i] = entity.FloatVector(vecs[i])
			}
			tctx, cancel := context.WithTimeout(ctx, e.Config.SearchTimeout)
			defer cancel()
			rs, err := e.Client.Search(tctx,
				milvusclient.NewSearchOption(col, TOP_K, qvecs).
					WithANNSField("vector").
					WithOutputFields("id").
					WithConsistencyLevel(entity.ClStrong))
			if err != nil {
				return err
			}
			if len(rs) != len(ids) {
				return errors.Errorf("search result length mismatch: got %d want %d", len(rs), len(ids))
			}
			for i := range rs {
				if rs[i].IDs == nil || rs[i].IDs.Len() == 0 {
					return errors.Errorf("empty search result for i=%d", i)
				}
				topIDCol, ok := rs[i].IDs.(*mvcol.ColumnInt64)
				if !ok || len(topIDCol.Data()) == 0 {
					return errors.Errorf("unexpected id column for i=%d", i)
				}
				if topIDCol.Data()[0] != ids[i] {
					return errors.Errorf("top-1 mismatch for i=%d: got %d want %d", i, topIDCol.Data()[0], ids[i])
				}
			}
			return nil
		}
		if err := ObserveOpLatency(opSearch, labels); err != nil {
			return errors.Wrap(err, "search batch")
		}

		labels[0] = "delete"
		opDelete := func() error {
			dr, err := e.Client.Delete(ctx,
				milvusclient.NewDeleteOption(col).
					WithInt64IDs("id", ids),
			)
			if err != nil {
				return err
			}
			if dr.DeleteCount == 0 {
				return errors.New("delete reported 0 rows")
			}

			return nil
		}
		if err := ObserveOpLatency(opDelete, labels); err != nil {
			return errors.Wrap(err, "delete batch")
		}
	}

	// RO search latency
	{
		col := e.Config.MonitoringCollectionLatencyRO
		if err := ensureCollection(ctx, e, col); err != nil {
			return errors.Wrap(err, "ensure latency RO collection")
		}

		sampleIDs := sampleUniqueInts(e.Config.LatencyRWInsertPerCheck, e.Config.InitItemsPerCollection)
		q := fmt.Sprintf("id in [%s]", joinInts(sampleIDs))
		qr, err := e.Client.Query(ctx, milvusclient.NewQueryOption(col).
			WithFilter(q).
			WithOutputFields("id", "vector"))
		if err != nil {
			return errors.Wrap(err, "query latency RO vectors")
		}
		idColI := qr.GetColumn("id")
		vecColI := qr.GetColumn("vector")
		if idColI == nil || vecColI == nil {
			return errors.New("latency RO: missing id or vector in query result")
		}
		idCol := idColI.(*mvcol.ColumnInt64)
		vecCol := vecColI.(*mvcol.ColumnFloatVector)

		searchLabels := []string{"search_ro_latency", e.Name, e.Config.MonitoringDatabase, e.ClusterName, e.Name}
		for i := 0; i < idCol.Len(); i++ {
			id := idCol.Data()[i]
			vec := vecCol.Data()[i]
			opSearch := func() error {
				tctx, cancel := context.WithTimeout(ctx, e.Config.SearchTimeout)
				defer cancel()
				rs, err := e.Client.Search(tctx,
					milvusclient.NewSearchOption(col, TOP_K, []entity.Vector{entity.FloatVector(vec)}).
						WithANNSField("vector").
						WithOutputFields("id"))
				if err != nil {
					return err
				}
				if len(rs) == 0 || rs[0].IDs == nil || rs[0].IDs.Len() == 0 {
					return errors.New("latency RO: empty search result")
				}
				top := rs[0].IDs.(*mvcol.ColumnInt64).Data()[0]
				if top != id {
					return errors.Errorf("latency RO: top-1 mismatch got=%d want=%d", top, id)
				}
				return nil
			}
			if err := ObserveOpLatency(opSearch, searchLabels); err != nil {
				level.Warn(e.Logger).Log("msg", "latency RO search failed", "err", err)
			}
		}
	}

	return nil
}

// DurabilityPrepare ensures DB and durability collection exist and is initialized.
func DurabilityPrepare(p topology.ProbeableEndpoint) error {
	e, ok := p.(*MilvusEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not a milvus endpoint")
	}
	ctx := context.Background()

	if err := ensureMonitoringDB(ctx, e); err != nil {
		return err
	}
	if err := ensureCollection(ctx, e, e.Config.MonitoringCollectionDurability); err != nil {
		return errors.Wrap(err, "ensure durability")
	}
	if err := initCollectionIfNeeded(ctx, e, e.Config.MonitoringCollectionDurability, e.Config.DurabilityKeyPrefix); err != nil {
		return errors.Wrap(err, "init durability")
	}
	return nil
}

// DurabilityCheck placeholder (only durability metrics when implemented).
func DurabilityCheck(p topology.ProbeableEndpoint) error {
	e, ok := p.(*MilvusEndpoint)
	if !ok {
		return fmt.Errorf("error: given endpoint is not a milvus endpoint")
	}
	ctx := context.Background()
	if err := ensureMonitoringDB(ctx, e); err != nil {
		return err
	}

	level.Info(e.Logger).Log("msg", "Milvus durability check to be implemented")
	return nil
}

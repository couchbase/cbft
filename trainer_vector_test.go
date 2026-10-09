//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build vectors
// +build vectors

package cbft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/gocbcore/v10"
)

// ---------------------------------------------------------------------------
// waitForResponse
// ---------------------------------------------------------------------------

// mockPendingOp is a minimal gocbcore.PendingOp implementation for tests.
type mockPendingOp struct {
	cancelCh chan struct{}
}

func newMockPendingOp() *mockPendingOp {
	return &mockPendingOp{cancelCh: make(chan struct{}, 1)}
}

func (m *mockPendingOp) Cancel() {
	select {
	case m.cancelCh <- struct{}{}:
	default:
	}
}

func TestWaitForResponseSignalSuccess(t *testing.T) {
	signal := make(chan error, 1)
	closeCh := make(chan struct{})
	op := newMockPendingOp()

	signal <- nil // pre-load a success signal

	err := waitForResponse(signal, closeCh, op, 5*time.Second)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestWaitForResponseSignalError(t *testing.T) {
	signal := make(chan error, 1)
	closeCh := make(chan struct{})
	op := newMockPendingOp()
	sentinel := errors.New("kv error")

	signal <- sentinel

	err := waitForResponse(signal, closeCh, op, 5*time.Second)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
}

func TestWaitForResponseCloseCh(t *testing.T) {
	signal := make(chan error, 1)
	closeCh := make(chan struct{})
	op := newMockPendingOp()

	close(closeCh) // simulate index shutdown before any response

	err := waitForResponse(signal, closeCh, op, 5*time.Second)
	if !errors.Is(err, gocbcore.ErrRequestCanceled) {
		t.Fatalf("expected ErrRequestCanceled, got: %v", err)
	}
}

func TestWaitForResponseTimeout(t *testing.T) {
	signal := make(chan error, 1)
	closeCh := make(chan struct{})
	op := newMockPendingOp()

	// Feed a response into signal *after* Cancel() so the drain in
	// waitForResponse doesn't block.
	go func() {
		<-op.cancelCh
		signal <- fmt.Errorf("canceled")
	}()

	err := waitForResponse(signal, closeCh, op, 10*time.Millisecond)
	if !errors.Is(err, gocbcore.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// samplingWorkerRegistry.getOrCreateWorker
// ---------------------------------------------------------------------------

func TestGetOrCreateWorkerCreatesNew(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}

	w, existed := r.getOrCreateWorker("idx1")
	if existed {
		t.Fatal("expected existed=false for new worker")
	}
	if w == nil {
		t.Fatal("expected non-nil worker")
	}
}

func TestGetOrCreateWorkerReturnsExisting(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}

	w1, _ := r.getOrCreateWorker("idx1")

	w2, existed := r.getOrCreateWorker("idx1")
	if !existed {
		t.Fatal("expected existed=true for second call")
	}
	if w1 != w2 {
		t.Fatal("expected same worker pointer on second call")
	}
}

func TestGetOrCreateWorkerIndependentIndexes(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}

	w1, ex1 := r.getOrCreateWorker("idx1")
	w2, ex2 := r.getOrCreateWorker("idx2")

	if ex1 || ex2 {
		t.Fatal("both workers should be new")
	}
	if w1 == w2 {
		t.Fatal("expected distinct worker instances")
	}
	if len(r.workers) != 2 {
		t.Fatalf("expected 2 workers in registry, got %d", len(r.workers))
	}
}

func newTestTrainer() *vectorIndexTrainer {
	return &vectorIndexTrainer{
		doneCh:  make(chan struct{}),
		closeCh: make(chan struct{}),
		bleveDest: &BleveDest{
			stopCh: make(chan struct{}),
		},
	}
}

// ---------------------------------------------------------------------------
// vectorIndexTrainer.close (registry cleanup)
// ---------------------------------------------------------------------------

// A partition closing while training is still in progress must not retire
// the worker the creator is training.
func TestTrainerCloseKeepsWorkerDuringTraining(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	w, _ := r.getOrCreateWorker("idx1")

	tr := newTestTrainer()
	tr.indexName = "idx1"
	tr.worker = w

	_ = tr.close()

	r.m.Lock()
	defer r.m.Unlock()
	if _, stillPresent := r.workers["idx1"]; !stillPresent {
		t.Fatal("worker should still be in registry while training is in progress")
	}
	if isClosed(w.closeCh) {
		t.Fatal("worker should not be closed while training is in progress")
	}
}

// Once the last holder closes, the worker must be retired even though other
// partitions still use it, so later partitions don't copy from it.
func TestTrainerCloseRemovesWorkerWhenLastHolderCloses(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	creator := newTestTrainerForIndex("idx1")
	// a sibling whose copy failed, so it uses the worker but isn't a holder
	sibling := newTestTrainerForIndex("idx1")
	w := newTrainedTestWorker(r, creator, sibling)

	_ = creator.close()

	r.m.Lock()
	defer r.m.Unlock()
	if _, stillPresent := r.workers["idx1"]; stillPresent {
		t.Fatal("worker should be removed from registry once its last holder closes")
	}
	if !isClosed(w.closeCh) {
		t.Fatal("worker without holders should be closed")
	}
}

// ---------------------------------------------------------------------------
// extractIndexNameFromPath (string parsing)
// ---------------------------------------------------------------------------

func setupManagerWithPIndex(t *testing.T, uuidPath, partitionName string) (*cbgt.Manager, func()) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "trainer_test_*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := cbgt.NewCfgMem()
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":18094", tmpDir, "test", nil)

	if err := mgr.SetPIndexName(uuidPath, partitionName); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("SetPIndexName failed: %v", err)
	}

	return mgr, func() { os.RemoveAll(tmpDir) }
}

func TestExtractIndexNameFromPathNormalName(t *testing.T) {
	uuidPath := "abc123"
	// partitionName has form <indexName>_<indexUUID>_<pindexUUID>
	partitionName := "myVectorIndex_deadbeef_cafebabe"

	mgr, cleanup := setupManagerWithPIndex(t, uuidPath, partitionName)
	defer cleanup()

	tr := &vectorIndexTrainer{mgr: mgr, bleveDest: &BleveDest{stopCh: make(chan struct{})}}

	indexName, err := tr.extractIndexNameFromPath(filepath.Join("/some/path", uuidPath))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if indexName != "myVectorIndex" {
		t.Fatalf("expected 'myVectorIndex', got %q", indexName)
	}
	if tr.partitionName != partitionName {
		t.Fatalf("expected partitionName to be set to %q, got %q", partitionName, tr.partitionName)
	}
}

func TestExtractIndexNameFromPathMultiSegmentIndexName(t *testing.T) {
	uuidPath := "uuid001"
	partitionName := "stub_index_indexUUID_pindexUUID"

	mgr, cleanup := setupManagerWithPIndex(t, uuidPath, partitionName)
	defer cleanup()

	tr := &vectorIndexTrainer{mgr: mgr, bleveDest: &BleveDest{stopCh: make(chan struct{})}}

	indexName, err := tr.extractIndexNameFromPath(filepath.Join("/data", uuidPath))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if indexName != "stub_index" {
		t.Fatalf("expected 'stub_index', got %q", indexName)
	}
}

func TestExtractIndexNameFromPathUnknownPath(t *testing.T) {
	cfg := cbgt.NewCfgMem()
	tmpDir, _ := os.MkdirTemp("", "trainer_test_unknown_*")
	defer os.RemoveAll(tmpDir)

	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", tmpDir, "test-source", nil)

	tr := &vectorIndexTrainer{mgr: mgr, bleveDest: &BleveDest{stopCh: make(chan struct{})}}

	// No pindex registered → GetPIndexName returns "", nil (refresh=false).
	// extractIndexNameFromPath must error out rather than return an empty
	// indexName, which would be shared as a worker key across indexes.
	indexName, err := tr.extractIndexNameFromPath("/some/path/unknownFormat")
	if err == nil {
		t.Fatalf("expected error for unknown path, got indexName %q", indexName)
	}
	if indexName != "" {
		t.Fatalf("expected empty indexName for unknown path, got %q", indexName)
	}
}

// ---------------------------------------------------------------------------
// samplingWorkerRunConfig: concurrent registry access
// ---------------------------------------------------------------------------

func TestGetOrCreateWorkerConcurrentAccess(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}

	const goroutines = 20
	var wg sync.WaitGroup
	var created int64
	workers := make([]*samplingWorker, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			w, existed := r.getOrCreateWorker("sharedIndex")
			if !existed {
				atomic.AddInt64(&created, 1)
			}
			workers[i] = w
		}(i)
	}
	wg.Wait()

	r.m.Lock()
	defer r.m.Unlock()

	w, ok := r.workers["sharedIndex"]
	if !ok {
		t.Fatal("expected worker in registry")
	}
	if created != 1 {
		t.Fatalf("expected exactly one caller to create the worker, got %d", created)
	}
	for _, got := range workers {
		if got != w {
			t.Fatal("expected every caller to get the same worker")
		}
	}
}

// ---------------------------------------------------------------------------
// samplingWorker tests with mock kvCluster and sourceIterators
// ---------------------------------------------------------------------------

// mockSourceIterable replays a fixed list of docs then returns nil.
type mockSourceIterable struct {
	docs     []*kvDoc
	pos      int
	errAt    int // if >= 0, return this error at that position
	err      error
	shutdown func(pos int) error // optional shutdown function to simulate shutdown during iteration
}

func (m *mockSourceIterable) Next() (*kvDoc, error) {
	if m.errAt >= 0 && m.pos == m.errAt {
		return nil, m.err
	}
	if m.shutdown != nil {
		if err := m.shutdown(m.pos); err != nil {
			return nil, err
		}
	}
	if m.pos >= len(m.docs) {
		return nil, nil // exhausted
	}
	d := m.docs[m.pos]
	m.pos++
	return d, nil
}

func (m *mockSourceIterable) Close() error { return nil }

// mockKVCluster returns one mockSourceIterable per collection in params.
type mockKVCluster struct {
	iterators []sourceIterator
	fetchErr  error
}

func (c *mockKVCluster) fetchiterators(params *samplingParams) ([]sourceIterator, error) {
	if c.fetchErr != nil {
		return nil, c.fetchErr
	}
	return c.iterators, nil
}

// collectSamples drains sampleCh until it is closed.
// mocks a sink which is like an ideal bleve index
func collectSamples(ch chan *sample) []*sample {
	var out []*sample
	for s := range ch {
		out = append(out, s)
	}
	return out
}

func newWorkerWithMockCluster(cluster kvCluster, collections []string, limits []int) *samplingWorker {
	w := &samplingWorker{
		sampleCh: make(chan *sample, 64),
		doneCh:   make(chan struct{}),
		closeCh:  make(chan struct{}),
		copyCh:   make(chan struct{}),
	}
	w.configure(&samplingWorkerRunConfig{
		cluster: cluster,
		params: &samplingParams{
			collectionNames: collections,
			sampleLimit:     limits,
			scopeName:       "testScope",
			sourceName:      "testBucket",
		},
		trainStats: &TrainingStats{},
	})
	return w
}

func TestSamplingWorkerRunDeliversSamples(t *testing.T) {
	docs := []*kvDoc{
		{ID: "doc1", Val: json.RawMessage(`{"x":1}`)},
		{ID: "doc2", Val: json.RawMessage(`{"x":2}`)},
		{ID: "doc3", Val: json.RawMessage(`{"x":3}`)},
	}
	cluster := &mockKVCluster{
		iterators: []sourceIterator{&mockSourceIterable{docs: docs, errAt: -1}},
	}
	w := newWorkerWithMockCluster(cluster, []string{"col1"}, []int{len(docs)})

	go w.run()
	samples := collectSamples(w.sampleCh)

	if len(samples) != len(docs) {
		t.Fatalf("expected %d samples, got %d", len(docs), len(samples))
	}
	for i, s := range samples {
		if s.id != docs[i].ID {
			t.Errorf("sample[%d]: expected id %q, got %q", i, docs[i].ID, s.id)
		}
		if string(s.value) != string(docs[i].Val) {
			t.Errorf("sample[%d]: expected value %s, got %s", i, docs[i].Val, s.value)
		}
		if s.cid != 0 {
			t.Errorf("sample[%d]: expected cid=0, got %d", i, s.cid)
		}
	}

	// doneCh must be closed once run() finishes
	select {
	case <-w.doneCh:
	case <-time.After(time.Second):
		t.Fatal("doneCh not closed after run()")
	}
}

func TestSamplingWorkerRunMultipleCollections(t *testing.T) {
	col0 := []*kvDoc{{ID: "a"}, {ID: "b"}}
	col1 := []*kvDoc{{ID: "c"}}
	cluster := &mockKVCluster{
		iterators: []sourceIterator{
			&mockSourceIterable{docs: col0, errAt: -1},
			&mockSourceIterable{docs: col1, errAt: -1},
		},
	}
	w := newWorkerWithMockCluster(cluster, []string{"col0", "col1"}, []int{len(col0), len(col1)})

	go w.run()
	samples := collectSamples(w.sampleCh)

	if len(samples) != 3 {
		t.Fatalf("expected 3 samples across 2 collections, got %d", len(samples))
	}

	byID := make(map[string]int)
	for _, s := range samples {
		byID[s.id] = s.cid
	}
	if byID["a"] != 0 || byID["b"] != 0 {
		t.Errorf("docs a,b should have cid=0, got %v", byID)
	}
	if byID["c"] != 1 {
		t.Errorf("doc c should have cid=1, got %d", byID["c"])
	}
}

func TestSamplingWorkerRunIteratorExhaustedEarly(t *testing.T) {
	// sampleLimit is 10 but iterator only has 2 docs
	docs := []*kvDoc{{ID: "x"}, {ID: "y"}}
	cluster := &mockKVCluster{
		iterators: []sourceIterator{&mockSourceIterable{docs: docs, errAt: -1}},
	}
	w := newWorkerWithMockCluster(cluster, []string{"col1"}, []int{10})

	go w.run()
	samples := collectSamples(w.sampleCh)

	if len(samples) != 2 {
		t.Fatalf("expected 2 samples (iterator exhausted), got %d", len(samples))
	}
}

func TestSamplingWorkerRunFetchIteratorsError(t *testing.T) {
	cluster := &mockKVCluster{fetchErr: errors.New("connection refused")}
	w := newWorkerWithMockCluster(cluster, []string{"col1"}, []int{5})

	go w.run()
	samples := collectSamples(w.sampleCh)

	if len(samples) != 0 {
		t.Fatalf("expected 0 samples on fetch error, got %d", len(samples))
	}
	select {
	case <-w.doneCh:
	case <-time.After(time.Second):
		t.Fatal("doneCh not closed after fetch error")
	}
}

func TestSamplingWorkerRunIteratorNextError(t *testing.T) {
	docs := []*kvDoc{{ID: "ok1"}, {ID: "ok2"}}
	cluster := &mockKVCluster{
		iterators: []sourceIterator{&mockSourceIterable{
			docs:  docs,
			errAt: 1, // error on second Next() call
			err:   errors.New("scan error"),
		}},
	}
	w := newWorkerWithMockCluster(cluster, []string{"col1"}, []int{5})

	go w.run()
	// run() should stop the goroutine and close channels regardless of errors
	collectSamples(w.sampleCh)

	select {
	case <-w.doneCh:
	case <-time.After(time.Second):
		t.Fatal("doneCh not closed after Next() error")
	}
}

func TestSamplingWorkerRunStopsOnCloseCh(t *testing.T) {
	// Provide many docs but close the worker before it finishes
	docs := make([]*kvDoc, 100)
	for i := range docs {
		docs[i] = &kvDoc{ID: fmt.Sprintf("doc%d", i), Val: json.RawMessage(`{}`)}
	}
	cluster := &mockKVCluster{
		iterators: []sourceIterator{&mockSourceIterable{docs: docs, errAt: -1}},
	}
	// Use a small sampleCh buffer so the goroutine blocks quickly
	w := &samplingWorker{
		sampleCh: make(chan *sample, 1),
		doneCh:   make(chan struct{}),
		closeCh:  make(chan struct{}),
		copyCh:   make(chan struct{}),
	}
	w.configure(&samplingWorkerRunConfig{
		cluster: cluster,
		params: &samplingParams{
			collectionNames: []string{"col1"},
			sampleLimit:     []int{100},
		},
		trainStats: &TrainingStats{},
	})

	go w.run()

	// Let it produce at least one sample then signal close
	<-w.sampleCh
	close(w.closeCh)

	// drain remaining and wait for completion
	for range w.sampleCh {
	}

	select {
	case <-w.doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("doneCh not closed after closeCh signal")
	}
}

// ---------------------------------------------------------------------------
// Helpers shared by createTrainedIndex / shutdown tests
// ---------------------------------------------------------------------------

// buildTestVectorBleveIndex creates a trainable scorch index with
// IndexTrainedWithFastMerge enabled and a 3-D "vector" field.
// The returned cleanup func closes and removes the index directory.
func buildTestVectorBleveIndex(t *testing.T, partitionName string) (bleve.TrainableIndex, func()) {
	t.Helper()
	m := bleve.NewIndexMapping()
	vecMapping := bleve.NewVectorFieldMapping()
	vecMapping.Type = "vector"
	vecMapping.Dims = 3
	vecMapping.Similarity = index.DefaultVectorSimilarityMetric
	vecMapping.VectorIndexOptimizedFor = index.IndexOptimizedForRecall
	m.DefaultMapping.AddFieldMappingsAt("vector", vecMapping)

	kvconfig := map[string]interface{}{scorch.IndexTrainedWithFastMerge: true}
	_ = os.RemoveAll(partitionName)
	bindex, err := bleve.NewUsing(partitionName, m, scorch.Name, bleve.Config.DefaultKVStore, kvconfig)
	if err != nil {
		t.Fatal(err)
	}
	vecIndex, ok := bindex.(bleve.TrainableIndex)
	if !ok {
		bindex.Close()
		os.RemoveAll(partitionName)
		t.Fatal("bleve index does not implement TrainableIndex")
	}
	return vecIndex, func() {
		vecIndex.Close()
		os.RemoveAll(partitionName)
	}
}

// makeVectorTestDocs builds count kvDocs each containing a 3-D float vector field.
func makeVectorTestDocs(count int) []*kvDoc {
	docs := make([]*kvDoc, count)
	for j := 0; j < count; j++ {
		docs[j] = &kvDoc{
			ID:  fmt.Sprintf("doc%d", j),
			Val: json.RawMessage(fmt.Sprintf(`{"id":"doc%d","vector":[%f,%f,%f]}`, j, float64(j), float64(j+1), float64(j+2))),
		}
	}
	return docs
}

// makeSimpleMockIterables returns one plain mockSourceIterable per collection
// in params, each pre-loaded with 3-D vector docs.
func makeSimpleMockIterables(params *samplingParams) []sourceIterator {
	itrs := make([]sourceIterator, len(params.collectionNames))
	for i := range params.collectionNames {
		itrs[i] = &mockSourceIterable{docs: makeVectorTestDocs(params.sampleLimit[i]), errAt: -1}
	}
	return itrs
}

// newTestWorkerAndConfig wires up a samplingWorker and trainedIndexConfig backed
// by cluster. The worker lives in its own private registry so tests are isolated.
func newTestWorkerAndConfig(t *testing.T, tr *vectorIndexTrainer, vecIndex bleve.TrainableIndex,
	params *samplingParams, cluster kvCluster) (*samplingWorker, *trainedIndexConfig) {
	t.Helper()
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	worker, _ := r.getOrCreateWorker(tr.indexName)
	cfg := &trainedIndexConfig{
		worker:          worker,
		vecIndex:        vecIndex,
		scopeName:       params.scopeName,
		collectionNames: params.collectionNames,
		initClusterCallback: func() error {
			worker.configure(&samplingWorkerRunConfig{cluster: cluster, params: params, trainStats: &tr.trainStats})
			return nil
		},
	}
	return worker, cfg
}

// ---------------------------------------------------------------------------
// createTrainedIndex / shutdown integration tests
// ---------------------------------------------------------------------------

func TestCreateTrainedIndexUsingMockCluster(t *testing.T) {
	const (
		indexName     = "testVectorIndex"
		partitionName = "testVectorIndex_indexUUID_pindexUUID"
	)
	vecIndex, cleanup := buildTestVectorBleveIndex(t, partitionName)
	defer cleanup()

	tr := newTestTrainer()
	tr.indexName = indexName
	tr.partitionName = partitionName

	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}

	mockC := &mockKVCluster{iterators: makeSimpleMockIterables(params)}
	_, cfg := newTestWorkerAndConfig(t, tr, vecIndex, params, mockC)

	if err := tr.createTrainedIndex(cfg); err != nil {
		t.Fatalf("createTrainedIndex failed: %v", err)
	}

	if err := tr.markTrainingComplete(vecIndex); err != nil {
		t.Fatalf("markTrainingComplete failed: %v", err)
	}

	val, err := vecIndex.GetInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatal(err)
	}
	if val != nil {
		found, err := strconv.ParseBool(string(val))
		if err != nil {
			t.Fatal(err)
		}
		if found {
			stats := vecIndex.StatsMap()
			indexStats := stats["index"].(map[string]interface{})
			if v, ok := indexStats["TotTrainedSamples"].(uint64); !ok || v != uint64(params.sampleLimit[0]) {
				t.Fatalf("expected TotTrainedSamples=20, got %v", indexStats["TotTrainedSamples"])
			}
		}
	}
}

func TestShutdownUsingMockCluster(t *testing.T) {
	const (
		indexName     = "testVectorIndex"
		partitionName = "testVectorIndex_indexUUID_pindexUUID"
	)
	vecIndex, cleanup := buildTestVectorBleveIndex(t, partitionName)
	defer cleanup()

	tr := newTestTrainer()
	tr.indexName = indexName
	tr.partitionName = partitionName

	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}

	// Build iterables that simulate an index shutdown mid-iteration.
	itrs := make([]sourceIterator, len(params.collectionNames))
	for i := range params.collectionNames {
		itrs[i] = &mockSourceIterable{
			docs:  makeVectorTestDocs(params.sampleLimit[i]),
			errAt: -1,
			shutdown: func(pos int) error {
				if pos == 10 {
					time.Sleep(100 * time.Millisecond) // simulate some delay in shutdown handling
					close(tr.bleveDest.stopCh)
					tr.close()
				}
				return nil
			},
		}
	}

	mockC := &mockKVCluster{iterators: itrs}
	tw, cfg := newTestWorkerAndConfig(t, tr, vecIndex, params, mockC)
	tr.worker = tw // ensure trainer has reference to worker for shutdown
	err := tr.createTrainedIndex(cfg)
	if err == nil {
		t.Fatalf("createTrainedIndex should have failed")
	}

	// check shutdown was handled gracefully and training was stopped without marking complete
	select {
	case <-tw.closeCh:
		select {
		case <-tw.doneCh:
			// indicates that the sampler goroutine exited
		case <-time.After(2 * time.Second):
			t.Fatal("expected doneCh to be closed after shutdown")
		}

		if vecIndex != nil {
			stats := vecIndex.StatsMap()
			indexStats := stats["index"].(map[string]interface{})
			if v, ok := indexStats["TotTrainedSamples"].(uint64); !ok || v != 0 {
				t.Fatalf("index was shut down before training went through, expected TotTrainedSamples=0, got %v", indexStats["TotTrainedSamples"])
			}
		}
		// expected closeCh to be closed on shutdown
	case <-time.After(2 * time.Second):
		t.Fatal("expected closeCh to be closed on shutdown")
	}

}

// The creator must be registered as a holder of the trained index by the time
// the partitions waiting on copyCh are woken.
func TestCreateTrainedIndexRegistersCreatorAsHolder(t *testing.T) {
	const (
		indexName     = "testVectorIndex"
		partitionName = "testVectorIndex_indexUUID_pindexUUID"
	)
	vecIndex, cleanup := buildTestVectorBleveIndex(t, partitionName)
	defer cleanup()

	tr := newTestTrainer()
	tr.indexName = indexName
	tr.partitionName = partitionName

	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}
	mockC := &mockKVCluster{iterators: makeSimpleMockIterables(params)}
	worker, cfg := newTestWorkerAndConfig(t, tr, vecIndex, params, mockC)

	// what a waiting partition sees as soon as it's woken
	seen := make(chan bleve.TrainableIndex, 1)
	go func() {
		<-worker.copyCh
		workerRegistry.m.Lock()
		seen <- worker.holders[tr]
		workerRegistry.m.Unlock()
	}()

	if err := tr.createTrainedIndex(cfg); err != nil {
		t.Fatalf("createTrainedIndex failed: %v", err)
	}

	select {
	case holder := <-seen:
		if holder != vecIndex {
			t.Fatal("expected the creator to be a holder when copyCh is closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("copyCh wasn't closed")
	}
}

// A failed training must not register the creator as a holder.
func TestCreateTrainedIndexFailureRegistersNoHolder(t *testing.T) {
	tr := newTestTrainer()
	tr.indexName = "testVectorIndex"

	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	worker, _ := r.getOrCreateWorker(tr.indexName)
	err := tr.createTrainedIndex(&trainedIndexConfig{
		worker: worker,
		initClusterCallback: func() error {
			return fmt.Errorf("cluster unavailable")
		},
	})
	if err == nil {
		t.Fatal("expected createTrainedIndex to fail")
	}

	workerRegistry.m.Lock()
	defer workerRegistry.m.Unlock()
	if len(worker.holders) != 0 {
		t.Fatalf("expected no holders after a failed training, got %d",
			len(worker.holders))
	}
}

// A partition that copies the trained index must be registered as a holder,
// so it can serve later copies.
func TestCopyTrainedIndexRegistersHolder(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	// the creator trains the worker's trained index
	creatorIndex, cleanupCreator := buildTestVectorBleveIndex(t, "idx_indexUUID_p1")
	defer cleanupCreator()
	creator := newTestTrainer()
	creator.indexName = "idx"
	worker, _ := r.getOrCreateWorker(creator.indexName)
	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}
	mockC := &mockKVCluster{iterators: makeSimpleMockIterables(params)}
	err := creator.createTrainedIndex(&trainedIndexConfig{
		worker:          worker,
		vecIndex:        creatorIndex,
		scopeName:       params.scopeName,
		collectionNames: params.collectionNames,
		initClusterCallback: func() error {
			worker.configure(&samplingWorkerRunConfig{cluster: mockC, params: params,
				trainStats: &creator.trainStats, vecIndex: creatorIndex})
			return nil
		},
	})
	if err != nil {
		t.Fatalf("createTrainedIndex failed: %v", err)
	}

	// a sibling partition of the same index copies it
	const copierPath = "copierpindexuuid"
	mgr, cleanupMgr := setupManagerWithPIndex(t, copierPath, "idx_indexUUID_p2")
	defer cleanupMgr()
	copierIndex, cleanupCopier := buildTestVectorBleveIndex(t, copierPath)
	defer cleanupCopier()
	copier := newTestTrainer()
	copier.mgr = mgr
	copier.bleveDest.path = copierPath
	copier.bleveDest.bindex = copierIndex

	copier.acquireSamples()

	if _, err := os.Stat(filepath.Join(copierPath, "store",
		index.TrainedIndexFileName)); err != nil {
		t.Fatalf("expected the trained index to be copied: %v", err)
	}
	workerRegistry.m.Lock()
	defer workerRegistry.m.Unlock()
	if worker.holders[copier] != copierIndex {
		t.Fatal("expected the copier to be registered as a holder")
	}
	if worker.holders[creator] != creatorIndex {
		t.Fatal("expected the creator to remain a holder")
	}
}

// ---------------------------------------------------------------------------
// holder deregistration on close
// ---------------------------------------------------------------------------

// newTrainedTestWorker creates a worker in r whose training succeeded with
// creator as the only holder, and attaches the given siblings to it.
func newTrainedTestWorker(r *samplingWorkerRegistry, creator *vectorIndexTrainer,
	siblings ...*vectorIndexTrainer) *samplingWorker {
	w, _ := r.getOrCreateWorker(creator.indexName)
	creator.worker = w
	for _, s := range siblings {
		s.worker = w
	}
	w.success = true
	r.finishTraining(w, creator, nil)
	close(w.copyCh)
	return w
}

func newTestTrainerForIndex(indexName string) *vectorIndexTrainer {
	tr := newTestTrainer()
	tr.indexName = indexName
	return tr
}

// Closing a holder deregisters it, and the worker stays as long as another
// partition still holds the trained index.
func TestTrainerCloseKeepsWorkerWhileHoldersRemain(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	creator := newTestTrainerForIndex("idx")
	copier := newTestTrainerForIndex("idx")
	w := newTrainedTestWorker(r, creator, copier)
	r.addHolder(w, copier, nil)

	_ = creator.close()

	r.m.Lock()
	defer r.m.Unlock()
	if _, ok := w.holders[creator]; ok {
		t.Fatal("closed partition should be deregistered as a holder")
	}
	if _, ok := w.holders[copier]; !ok {
		t.Fatal("remaining holder should stay registered")
	}
	if r.workers["idx"] != w {
		t.Fatal("worker should stay registered while a holder remains")
	}
	if isClosed(w.closeCh) {
		t.Fatal("worker should not be closed while a holder remains")
	}
}

// If the creator closes before training finishes, the worker must be retired
// before the partitions waiting on it are woken.
func TestCreatorCloseDuringTrainingRetiresWorker(t *testing.T) {
	const (
		indexName     = "testVectorIndex"
		partitionName = "testVectorIndex_indexUUID_pindexUUID"
	)
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	vecIndex, cleanup := buildTestVectorBleveIndex(t, partitionName)
	defer cleanup()

	creator := newTestTrainerForIndex(indexName)
	creator.partitionName = partitionName
	waiter := newTestTrainerForIndex(indexName)
	w, _ := r.getOrCreateWorker(indexName)
	creator.worker = w
	r.getOrCreateWorker(indexName)
	waiter.worker = w

	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}
	// the creator's partition closes midway through sampling
	itrs := []sourceIterator{&mockSourceIterable{
		docs:  makeVectorTestDocs(params.sampleLimit[0]),
		errAt: -1,
		shutdown: func(pos int) error {
			if pos == 10 {
				creator.close()
			}
			return nil
		},
	}}
	mockC := &mockKVCluster{iterators: itrs}

	// what a waiting partition sees as soon as it's woken
	retired := make(chan bool, 1)
	go func() {
		<-w.copyCh
		r.m.Lock()
		_, registered := r.workers[indexName]
		retired <- !registered && len(w.holders) == 0
		r.m.Unlock()
	}()

	_ = creator.createTrainedIndex(&trainedIndexConfig{
		worker:          w,
		vecIndex:        vecIndex,
		scopeName:       params.scopeName,
		collectionNames: params.collectionNames,
		initClusterCallback: func() error {
			w.configure(&samplingWorkerRunConfig{cluster: mockC, params: params,
				trainStats: &creator.trainStats})
			return nil
		},
	})

	select {
	case ok := <-retired:
		if !ok {
			t.Fatal("expected the worker to be retired, without holders, " +
				"when copyCh is closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("copyCh wasn't closed")
	}
	r.m.Lock()
	defer r.m.Unlock()
	if !isClosed(w.closeCh) {
		t.Fatal("expected the sampler to be stopped")
	}
}

// Retiring a worker must not remove a newer worker registered for the index.
func TestRetireKeepsNewerWorker(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	old, _ := r.getOrCreateWorker("idx")
	delete(r.workers, "idx")
	newer, _ := r.getOrCreateWorker("idx")

	r.m.Lock()
	r.retireLOCKED("idx", old)
	r.m.Unlock()

	if r.workers["idx"] != newer {
		t.Fatal("retiring an old worker removed the newer one")
	}
	if !isClosed(old.closeCh) || isClosed(newer.closeCh) {
		t.Fatal("expected only the old worker to be closed")
	}
}

// A partition that closed must not be added back as a holder.
func TestAddHolderSkipsClosedPartition(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	tr := newTestTrainerForIndex("idx")
	w, _ := r.getOrCreateWorker("idx")
	close(tr.closeCh)

	r.addHolder(w, tr, nil)

	if len(w.holders) != 0 {
		t.Fatal("closed partition was registered as a holder")
	}
}

// ---------------------------------------------------------------------------
// copying from any holder
// ---------------------------------------------------------------------------

// newTestCopier builds a partition of index "idx" at path, registered with a
// manager so that acquireSamples can resolve its index name.
func newTestCopier(t *testing.T, path, partitionName string) (
	*vectorIndexTrainer, bleve.TrainableIndex, func()) {
	t.Helper()
	mgr, cleanupMgr := setupManagerWithPIndex(t, path, partitionName)
	vecIndex, cleanupIndex := buildTestVectorBleveIndex(t, path)
	tr := newTestTrainer()
	tr.mgr = mgr
	tr.bleveDest.path = path
	tr.bleveDest.bindex = vecIndex
	return tr, vecIndex, func() {
		cleanupIndex()
		cleanupMgr()
	}
}

// After the creator moves off the node, a partition arriving later must copy
// the trained index from a sibling that still holds it, rather than from the
// closed creator.
func TestCopyTrainedIndexFromHolderAfterCreatorCloses(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	// the creator trains the worker's trained index
	creatorIndex, cleanupCreator := buildTestVectorBleveIndex(t, "idx_indexUUID_p1")
	var closeCreatorOnce sync.Once
	closeCreator := func() { closeCreatorOnce.Do(cleanupCreator) }
	defer closeCreator()
	creator := newTestTrainerForIndex("idx")
	worker, _ := r.getOrCreateWorker(creator.indexName)
	creator.worker = worker
	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}
	mockC := &mockKVCluster{iterators: makeSimpleMockIterables(params)}
	err := creator.createTrainedIndex(&trainedIndexConfig{
		worker:          worker,
		vecIndex:        creatorIndex,
		scopeName:       params.scopeName,
		collectionNames: params.collectionNames,
		initClusterCallback: func() error {
			worker.configure(&samplingWorkerRunConfig{cluster: mockC, params: params,
				trainStats: &creator.trainStats, vecIndex: creatorIndex})
			return nil
		},
	})
	if err != nil {
		t.Fatalf("createTrainedIndex failed: %v", err)
	}

	// a sibling copies it and becomes a holder
	sibling, siblingIndex, cleanupSibling := newTestCopier(t,
		"siblingpindexuuid", "idx_indexUUID_p2")
	defer cleanupSibling()
	sibling.acquireSamples()

	// the creator moves off the node: its trainer closes, then its index
	_ = creator.close()
	closeCreator()

	// a partition of the same index arrives later
	arrival, _, cleanupArrival := newTestCopier(t,
		"arrivalpindexuuid", "idx_indexUUID_p3")
	defer cleanupArrival()
	arrival.acquireSamples()

	if _, err := os.Stat(filepath.Join(arrival.bleveDest.path, "store",
		index.TrainedIndexFileName)); err != nil {
		t.Fatalf("expected the trained index to be copied from the sibling: %v", err)
	}
	workerRegistry.m.Lock()
	defer workerRegistry.m.Unlock()
	if _, ok := worker.holders[arrival]; !ok {
		t.Fatal("expected the arrival to be registered as a holder")
	}
	if worker.holders[sibling] != siblingIndex {
		t.Fatal("expected the sibling to remain a holder")
	}
}

// ---------------------------------------------------------------------------
// retrying the copy with another holder
// ---------------------------------------------------------------------------

// trainTestCreator trains index "idx" on a new partition at path through r, as
// the creator of its worker. The returned func closes the creator's index and
// can be called more than once.
func trainTestCreator(t *testing.T, r *samplingWorkerRegistry, path string) (
	*vectorIndexTrainer, bleve.TrainableIndex, *samplingWorker, func()) {
	t.Helper()
	vecIndex, cleanup := buildTestVectorBleveIndex(t, path)
	var once sync.Once
	closeIndex := func() { once.Do(cleanup) }

	creator := newTestTrainerForIndex("idx")
	worker, _ := r.getOrCreateWorker(creator.indexName)
	creator.worker = worker
	params := &samplingParams{
		collectionNames: []string{"col1"},
		sampleLimit:     []int{20},
		scopeName:       "testScope",
		sourceName:      "testBucket",
	}
	mockC := &mockKVCluster{iterators: makeSimpleMockIterables(params)}
	err := creator.createTrainedIndex(&trainedIndexConfig{
		worker:          worker,
		vecIndex:        vecIndex,
		scopeName:       params.scopeName,
		collectionNames: params.collectionNames,
		initClusterCallback: func() error {
			worker.configure(&samplingWorkerRunConfig{cluster: mockC, params: params,
				trainStats: &creator.trainStats, vecIndex: vecIndex})
			return nil
		},
	})
	if err != nil {
		closeIndex()
		t.Fatalf("createTrainedIndex failed: %v", err)
	}
	return creator, vecIndex, worker, closeIndex
}

// A copier that picks a holder whose index has closed must move on to another
// holder instead of failing.
func TestCopyTrainedIndexSkipsClosedHolders(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	creator, creatorIndex, worker, closeCreator := trainTestCreator(t, r,
		"idx_indexUUID_p1")
	defer closeCreator()

	// holders whose index closed without them deregistering. with several of
	// them, the copier almost always picks a closed one first.
	var closedHolders []*vectorIndexTrainer
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("closedholderpindex%d", i)
		closedIndex, cleanup := buildTestVectorBleveIndex(t, path)
		cleanup()
		holder := newTestTrainerForIndex("idx")
		r.addHolder(worker, holder, closedIndex)
		closedHolders = append(closedHolders, holder)
	}

	arrival, _, cleanupArrival := newTestCopier(t, "arrivalpindexuuid",
		"idx_indexUUID_p2")
	defer cleanupArrival()
	arrival.acquireSamples()

	if _, err := os.Stat(filepath.Join(arrival.bleveDest.path, "store",
		index.TrainedIndexFileName)); err != nil {
		t.Fatalf("expected the trained index to be copied from the live holder: %v", err)
	}
	r.m.Lock()
	defer r.m.Unlock()
	if worker.holders[creator] != creatorIndex {
		t.Fatal("expected the live holder to remain a holder")
	}
	if _, ok := worker.holders[arrival]; !ok {
		t.Fatal("expected the arrival to be registered as a holder")
	}
	if r.workers["idx"] != worker {
		t.Fatal("worker should stay registered while a live holder remains")
	}
}

// If every holder turns out to be closed, the closed holders are dropped, the
// worker is retired and the copier rejoins the registry. With no worker left it
// becomes the creator of a new one, whose training fails here without KV, so
// that worker is retired too and no trained index is copied.
func TestCopyTrainedIndexAllHoldersClosed(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	creator, _, worker, closeCreator := trainTestCreator(t, r, "idx_indexUUID_p1")
	defer closeCreator()
	// the creator's index closes without its trainer deregistering
	closeCreator()

	arrival, _, cleanupArrival := newTestCopier(t, "arrivalpindexuuid",
		"idx_indexUUID_p2")
	defer cleanupArrival()

	done := make(chan struct{})
	go func() {
		arrival.acquireSamples()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("copier kept retrying closed holders or rejoining")
	}

	if _, err := os.Stat(filepath.Join(arrival.bleveDest.path, "store",
		index.TrainedIndexFileName)); !os.IsNotExist(err) {
		t.Fatalf("expected no trained index to be copied, stat err: %v", err)
	}
	r.m.Lock()
	defer r.m.Unlock()
	if _, ok := worker.holders[creator]; ok {
		t.Fatal("expected the closed holder to be dropped")
	}
	if _, ok := r.workers["idx"]; ok {
		t.Fatal("worker without holders should be removed from the registry")
	}
	if !isClosed(worker.closeCh) {
		t.Fatal("worker without holders should be closed")
	}
}

// Partitions waiting on a worker whose creator closed mid-training must rejoin
// the registry and wait on the worker registered for the index by then, rather
// than train themselves, and then copy its trained index.
func TestRejoinWaitsOnCurrentWorker(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	// the first creator, which never finishes training
	creator1 := newTestTrainerForIndex("idx")
	w1, _ := r.getOrCreateWorker("idx")
	creator1.worker = w1

	var copiers []*vectorIndexTrainer
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		copier, _, cleanup := newTestCopier(t, fmt.Sprintf("copierpindex%d", i),
			fmt.Sprintf("idx_indexUUID_p%d", i+2))
		defer cleanup()
		copiers = append(copiers, copier)
		wg.Add(1)
		go func() {
			defer wg.Done()
			copier.acquireSamples()
		}()
	}
	// give the copiers time to join w1 and wait on it. a copier joining
	// later lands on the current worker directly, which the checks below
	// accept as well.
	time.Sleep(500 * time.Millisecond)

	// a worker of the index's next creator is registered by the time the
	// copiers rejoin
	r.m.Lock()
	delete(r.workers, "idx")
	r.m.Unlock()
	w2, _ := r.getOrCreateWorker("idx")

	// the first creator closes mid-training: its training ends without a holder
	_ = creator1.close()
	r.finishTraining(w1, creator1, nil)
	close(w1.copyCh)

	// the next creator trains w2
	creator2, _, gotW2, closeCreator2 := trainTestCreator(t, r, "idx_indexUUID_p1")
	defer closeCreator2()
	if gotW2 != w2 {
		t.Fatal("expected the next creator to train the registered worker")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("copiers didn't finish")
	}

	r.m.Lock()
	defer r.m.Unlock()
	if _, ok := w2.holders[creator2]; !ok {
		t.Fatal("expected the next creator to hold the trained index")
	}
	for _, copier := range copiers {
		if atomic.LoadUint64(&copier.trainStats.TotTrainedIndexBuildTimeInNs) != 0 {
			t.Fatalf("copier %s trained instead of waiting", copier.bleveDest.path)
		}
		if _, err := os.Stat(filepath.Join(copier.bleveDest.path, "store",
			index.TrainedIndexFileName)); err != nil {
			t.Fatalf("expected copier %s to copy the trained index: %v",
				copier.bleveDest.path, err)
		}
		if _, ok := w2.holders[copier]; !ok {
			t.Fatalf("expected copier %s to be a holder", copier.bleveDest.path)
		}
	}
}

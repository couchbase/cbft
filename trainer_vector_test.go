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
	if w.ref != 1 {
		t.Fatalf("expected ref=1, got %d", w.ref)
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
	if w2.ref != 2 {
		t.Fatalf("expected ref=2, got %d", w2.ref)
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

func TestTrainerCloseDecrementsRef(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	w, _ := r.getOrCreateWorker("idx1")
	// Bump ref to 2 as if two partitions share the worker.
	r.m.Lock()
	w.ref++
	r.m.Unlock()

	tr := newTestTrainer()
	tr.indexName = "idx1"
	tr.worker = w

	_ = tr.close()

	r.m.Lock()
	defer r.m.Unlock()
	if w.ref != 1 {
		t.Fatalf("expected ref=1 after one trainer closes, got %d", w.ref)
	}
	if _, stillPresent := r.workers["idx1"]; !stillPresent {
		t.Fatal("worker should still be in registry while ref > 0")
	}
}

func TestTrainerCloseRemovesWorkerWhenRefZero(t *testing.T) {
	r := &samplingWorkerRegistry{workers: make(map[string]*samplingWorker)}
	origRegistry := workerRegistry
	workerRegistry = r
	defer func() { workerRegistry = origRegistry }()

	w, _ := r.getOrCreateWorker("idx1")
	if w.ref != 1 {
		t.Fatalf("expected ref=1, got %d", w.ref)
	}

	tr := newTestTrainer()
	tr.indexName = "idx1"
	tr.worker = w

	_ = tr.close()

	r.m.Lock()
	defer r.m.Unlock()
	if _, stillPresent := r.workers["idx1"]; stillPresent {
		t.Fatal("worker should be removed from registry when ref reaches 0")
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
	// extractIndexNameFromPath should return empty indexName without error.
	indexName, err := tr.extractIndexNameFromPath("/some/path/unknownFormat")
	if err != nil {
		t.Fatalf("unexpected error for unknown path: %v", err)
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
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			r.getOrCreateWorker("sharedIndex")
		}()
	}
	wg.Wait()

	r.m.Lock()
	defer r.m.Unlock()

	w, ok := r.workers["sharedIndex"]
	if !ok {
		t.Fatal("expected worker in registry")
	}
	if w.ref != int64(goroutines) {
		t.Fatalf("expected ref=%d, got %d", goroutines, w.ref)
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
			worker.configure(&samplingWorkerRunConfig{cluster: cluster, params: params})
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
		if tw.ref != 0 {
			t.Fatalf("expected worker ref=0 after shutdown, got %d", tw.ref)
		}

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

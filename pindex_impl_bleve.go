//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/rcrowley/go-metrics"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"

	_ "github.com/couchbaselabs/cbft/bleve_metrics"
)

const BLEVE_DEST_INITIAL_BUF_SIZE_BYTES = 2000000
const BLEVE_DEST_APPLY_BUF_SIZE_BYTES = 1800000

type BleveParams struct {
	Mapping bleve.IndexMapping     `json:"mapping"`
	Store   map[string]interface{} `json:"store"`
}

func NewBleveParams() *BleveParams {
	return &BleveParams{
		Mapping: *bleve.NewIndexMapping(),
	}
}

type BleveDest struct {
	path string

	// Invoked when mgr should restart this BleveDest, like on rollback.
	restart func()

	m          sync.Mutex // Protects the fields that follow.
	bindex     bleve.Index
	partitions map[string]*BleveDestPartition

	stats PIndexStoreStats
}

// Used to track state for a single partition.
type BleveDestPartition struct {
	bdest           *BleveDest
	bindex          bleve.Index
	partition       string
	partitionOpaque []byte // Key used to implement SetOpaque/GetOpaque().

	m           sync.Mutex   // Protects the fields that follow.
	seqMax      uint64       // Max seq # we've seen for this partition.
	seqMaxBuf   []byte       // For binary encoded seqMax uint64.
	seqMaxBatch uint64       // Max seq # that got through batch apply/commit.
	seqSnapEnd  uint64       // To track snapshot end seq # for this partition.
	buf         []byte       // The batch points to slices from buf, which we reuse.
	batch       *bleve.Batch // Batch is applied when too big or when we hit seqSnapEnd.

	lastOpaque []byte // Cache most recent value for SetOpaque()/GetOpaque().

	cwrCh    chan *ConsistencyWaitReq
	cwrQueue cwrQueue
}

type BleveQueryParams struct {
	Timeout     int64                `json:"timeout"`
	Consistency *ConsistencyParams   `json:"consistency"`
	Query       *bleve.SearchRequest `json:"query"`
}

func NewBleveDest(path string, bindex bleve.Index, restart func()) *BleveDest {
	return &BleveDest{
		path:       path,
		restart:    restart,
		bindex:     bindex,
		partitions: make(map[string]*BleveDestPartition),
		stats: PIndexStoreStats{
			TimerBatchStore: metrics.NewTimer(),
		},
	}
}

// ---------------------------------------------------------

func init() {
	RegisterPIndexImplType("bleve", &PIndexImplType{
		Validate: ValidateBlevePIndexImpl,

		New:   NewBlevePIndexImpl,
		Open:  OpenBlevePIndexImpl,
		Count: CountBlevePIndexImpl,
		Query: QueryBlevePIndexImpl,

		Description: "bleve - full-text index" +
			" powered by the bleve full-text-search engine",
		StartSample: NewBleveParams(),
	})

	RegisterPIndexImplType("bleve-mem", &PIndexImplType{
		Validate: ValidateBlevePIndexImpl,

		New:   NewBlevePIndexImpl,
		Open:  OpenBlevePIndexImpl,
		Count: CountBlevePIndexImpl,
		Query: QueryBlevePIndexImpl,

		Description: "bleve-mem - full-text index" +
			" powered by bleve (in memory only)",
		StartSample: NewBleveParams(),
	})
}

func ValidateBlevePIndexImpl(indexType, indexName, indexParams string) error {
	bleveParams := NewBleveParams()
	if len(indexParams) > 0 {
		return json.Unmarshal([]byte(indexParams), bleveParams)
	}
	return nil
}

func NewBlevePIndexImpl(indexType, indexParams, path string,
	restart func()) (PIndexImpl, Dest, error) {
	bleveParams := NewBleveParams()
	if len(indexParams) > 0 {
		err := json.Unmarshal([]byte(indexParams), bleveParams)
		if err != nil {
			return nil, nil, fmt.Errorf("bleve: parse params, err: %v", err)
		}
	}

	blevePath := path
	if indexType == "bleve-mem" {
		blevePath = "" // Force bleve to use memory-only storage.

		// For a normal, non-empty path, bleve will create the
		// directory (and also expects path not to exist yet
		// beforehand).  And, for an empty path, we need to create the
		// directory here because bleve won't do so.
		err := os.MkdirAll(path, 0700)
		if err != nil {
			return nil, nil, err
		}
	}

	kvStoreName, ok := bleveParams.Store["kvStoreName"].(string)
	if !ok || kvStoreName == "" {
		kvStoreName = bleve.Config.DefaultKVStore
	}
	kvConfig := map[string]interface{}{
		"create_if_missing": true,
		"error_if_exists":   true,
	}
	for k, v := range bleveParams.Store {
		kvConfig[k] = v
	}

	bindex, err :=
		bleve.NewUsing(blevePath, &bleveParams.Mapping, kvStoreName, kvConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: new index, path: %s,"+
			" kvStoreName: %s, kvConfig: %#v, err: %s",
			path, kvStoreName, kvConfig, err)
	}

	pathMeta := path + string(os.PathSeparator) + "PINDEX_BLEVE_META"
	err = ioutil.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		return nil, nil, err
	}

	return bindex, &DestForwarder{
		DestProvider: NewBleveDest(path, bindex, restart),
	}, nil
}

func OpenBlevePIndexImpl(indexType, path string,
	restart func()) (PIndexImpl, Dest, error) {
	if indexType == "bleve-mem" {
		return nil, nil, fmt.Errorf("bleve: cannot re-open bleve-mem, path: %s", path)
	}

	buf, err := ioutil.ReadFile(path + string(os.PathSeparator) + "PINDEX_BLEVE_META")
	if err != nil {
		return nil, nil, err
	}

	bleveParams := NewBleveParams()
	err = json.Unmarshal(buf, bleveParams)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: parse params: %v", err)
	}

	// TODO: boltdb sometimes locks on Open(), so need to investigate,
	// where perhaps there was a previous missing or race-y Close().
	bindex, err := bleve.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return bindex, &DestForwarder{
		DestProvider: NewBleveDest(path, bindex, restart),
	}, nil
}

// ---------------------------------------------------------------

func CountBlevePIndexImpl(mgr *Manager, indexName, indexUUID string) (
	uint64, error) {
	alias, err := bleveIndexAlias(mgr, indexName, indexUUID, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("bleve: CountBlevePIndexImpl indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	return alias.DocCount()
}

func QueryBlevePIndexImpl(mgr *Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	var bleveQueryParams BleveQueryParams
	err := json.Unmarshal(req, &bleveQueryParams)
	if err != nil {
		return fmt.Errorf("bleve: QueryBlevePIndexImpl parsing bleveQueryParams,"+
			" req: %s, err: %v", req, err)
	}

	cancelCh := TimeoutCancelChan(bleveQueryParams.Timeout)

	alias, err := bleveIndexAlias(mgr, indexName, indexUUID,
		bleveQueryParams.Consistency, cancelCh)
	if err != nil {
		return err
	}

	err = bleveQueryParams.Query.Query.Validate()
	if err != nil {
		return err
	}

	searchResponse, err := alias.Search(bleveQueryParams.Query)
	if err != nil {
		return err
	}

	mustEncode(res, searchResponse)

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) Dest(partition string) (Dest, error) {
	t.m.Lock()
	defer t.m.Unlock()

	return t.getPartitionUnlocked(partition)
}

func (t *BleveDest) getPartitionUnlocked(partition string) (
	*BleveDestPartition, error) {
	if t.bindex == nil {
		return nil, fmt.Errorf("bleve: BleveDest already closed")
	}

	bdp, exists := t.partitions[partition]
	if !exists || bdp == nil {
		bdp = &BleveDestPartition{
			bdest:           t,
			bindex:          t.bindex,
			partition:       partition,
			partitionOpaque: []byte("o:" + partition),
			seqMaxBuf:       make([]byte, 8), // Binary encoded seqMax uint64.
			batch:           bleve.NewBatch(),
			cwrCh:           make(chan *ConsistencyWaitReq, 1),
			cwrQueue:        cwrQueue{},
		}
		heap.Init(&bdp.cwrQueue)

		go RunConsistencyWaitQueue(bdp.cwrCh, &bdp.m, &bdp.cwrQueue,
			func() uint64 { return bdp.seqMaxBatch })

		t.partitions[partition] = bdp
	}

	return bdp, nil
}

// ---------------------------------------------------------

func (t *BleveDest) Close() error {
	t.m.Lock()
	defer t.m.Unlock()

	return t.closeUnlocked()
}

func (t *BleveDest) closeUnlocked() error {
	if t.bindex == nil {
		return nil // Already closed.
	}

	for _, bdp := range t.partitions {
		close(bdp.cwrCh)
	}
	t.partitions = make(map[string]*BleveDestPartition)

	t.bindex.Close()
	t.bindex = nil

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) Rollback(partition string, rollbackSeq uint64) error {
	log.Printf("bleve: dest rollback, partition: %s, rollbackSeq: %d",
		partition, rollbackSeq)

	t.m.Lock()
	defer t.m.Unlock()

	// NOTE: A rollback of any partition means a rollback of all
	// partitions, since they all share a single bleve.Index backend.
	// That's why we grab and keep BleveDest.m locked.
	//
	// TODO: Implement partial rollback one day.  Implementation
	// sketch: we expect bleve to one day to provide an additional
	// Snapshot() and Rollback() API, where Snapshot() returns some
	// opaque and persistable snapshot ID ("SID"), which cbft can
	// occasionally record into the bleve's Get/SetInternal() storage.
	// A stream rollback operation then needs to loop through
	// appropriate candidate SID's until a Rollback(SID) succeeds.
	// Else, we eventually devolve down to restarting/rebuilding
	// everything from scratch or zero.
	//
	// For now, always rollback to zero, in which we close the pindex,
	// erase files and have the janitor rebuild from scratch.

	err := t.closeUnlocked()
	if err != nil {
		return fmt.Errorf("bleve: BleveDest can't close during rollback,"+
			" err: %v", err)
	}

	os.RemoveAll(t.path)

	t.restart()

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	cwr := &ConsistencyWaitReq{
		ConsistencyLevel: consistencyLevel,
		ConsistencySeq:   consistencySeq,
		CancelCh:         cancelCh,
		DoneCh:           make(chan error),
	}

	t.m.Lock()

	bdp, err := t.getPartitionUnlocked(partition)
	if err != nil {
		t.m.Unlock()
		return err
	}

	// We want getPartitionUnlocked() & cwr send under the same lock
	// so that another goroutine can't concurrently close the cwrCh.
	bdp.cwrCh <- cwr

	t.m.Unlock()

	return ConsistencyWaitDone(partition, cancelCh, cwr.DoneCh,
		func() uint64 {
			bdp.m.Lock()
			defer bdp.m.Unlock()
			return bdp.seqMaxBatch
		})
}

// ---------------------------------------------------------

func (t *BleveDest) Count(pindex *PIndex, cancelCh <-chan bool) (uint64, error) {
	return t.bindex.DocCount()
}

// ---------------------------------------------------------

func (t *BleveDest) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	var bleveQueryParams BleveQueryParams
	err := json.Unmarshal(req, &bleveQueryParams)
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query parsing bleveQueryParams,"+
			" req: %s, err: %v", req, err)
	}

	err = ConsistencyWaitPIndex(pindex, t,
		bleveQueryParams.Consistency, cancelCh)
	if err != nil {
		return err
	}

	err = bleveQueryParams.Query.Query.Validate()
	if err != nil {
		return err
	}

	searchResponse, err := t.bindex.Search(bleveQueryParams.Query)
	if err != nil {
		return err
	}

	mustEncode(res, searchResponse)

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) Stats(w io.Writer) error {
	_, err := w.Write(prefixPIndexStoreStats)
	if err != nil {
		return err
	}
	t.stats.WriteJSON(w)
	_, err = w.Write(jsonCloseBrace)

	return err
}

// ---------------------------------------------------------

func (t *BleveDestPartition) Close() error {
	return t.bdest.Close()
}

func (t *BleveDestPartition) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	bufVal := t.appendToBufUnlocked(val)

	t.batch.Index(string(key), bufVal) // TODO: string(key) makes garbage?

	return t.updateSeqUnlocked(seq)
}

func (t *BleveDestPartition) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.batch.Delete(string(key)) // TODO: string(key) makes garbage?

	return t.updateSeqUnlocked(seq)
}

func (t *BleveDestPartition) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	t.m.Lock()
	defer t.m.Unlock()

	err := t.applyBatchUnlocked()
	if err != nil {
		return err
	}

	t.seqSnapEnd = snapEnd

	return nil
}

func (t *BleveDestPartition) SetOpaque(partition string, value []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.lastOpaque = append(t.lastOpaque[0:0], value...)

	t.batch.SetInternal(t.partitionOpaque, t.lastOpaque)

	return nil
}

func (t *BleveDestPartition) GetOpaque(partition string) ([]byte, uint64, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.lastOpaque == nil {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		value, err := t.bindex.GetInternal(t.partitionOpaque)
		if err != nil {
			return nil, 0, err
		}
		t.lastOpaque = append([]byte(nil), value...) // Note: copies value.
	}

	if t.seqMax <= 0 {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		buf, err := t.bindex.GetInternal([]byte(t.partition))
		if err != nil {
			return nil, 0, err
		}
		if len(buf) <= 0 {
			return t.lastOpaque, 0, nil // No seqMax buf is a valid case.
		}
		if len(buf) != 8 {
			return nil, 0, fmt.Errorf("bleve: unexpected size for seqMax bytes")
		}
		t.seqMax = binary.BigEndian.Uint64(buf[0:8])
		binary.BigEndian.PutUint64(t.seqMaxBuf, t.seqMax)
	}

	return t.lastOpaque, t.seqMax, nil
}

func (t *BleveDestPartition) Rollback(partition string, rollbackSeq uint64) error {
	return t.bdest.Rollback(partition, rollbackSeq)
}

func (t *BleveDestPartition) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	return t.bdest.ConsistencyWait(partition,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *BleveDestPartition) Count(pindex *PIndex, cancelCh <-chan bool) (
	uint64, error) {
	return t.bdest.Count(pindex, cancelCh)
}

func (t *BleveDestPartition) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	return t.bdest.Query(pindex, req, res, cancelCh)
}

func (t *BleveDestPartition) Stats(w io.Writer) error {
	return t.bdest.Stats(w)
}

// ---------------------------------------------------------

func (t *BleveDestPartition) updateSeqUnlocked(seq uint64) error {
	if t.seqMax < seq {
		t.seqMax = seq
		binary.BigEndian.PutUint64(t.seqMaxBuf, t.seqMax)

		// NOTE: No copy of partition to buf as it's immutatable string bytes.
		t.batch.SetInternal([]byte(t.partition), t.seqMaxBuf)
	}

	if len(t.buf) < BLEVE_DEST_APPLY_BUF_SIZE_BYTES &&
		seq < t.seqSnapEnd {
		return nil
	}

	return t.applyBatchUnlocked()
}

func (t *BleveDestPartition) applyBatchUnlocked() error {
	err := Timer(func() error {
		return t.bindex.Batch(t.batch)
	}, t.bdest.stats.TimerBatchStore)
	if err != nil {
		return err
	}

	t.seqMaxBatch = t.seqMax

	for t.cwrQueue.Len() > 0 &&
		t.cwrQueue[0].ConsistencySeq <= t.seqMaxBatch {
		cwr := heap.Pop(&t.cwrQueue).(*ConsistencyWaitReq)
		if cwr != nil &&
			cwr.DoneCh != nil {
			close(cwr.DoneCh)
		}
	}

	// TODO: would good to reuse batch; ask for a public Reset() kind
	// of method on bleve.Batch?
	t.batch = bleve.NewBatch()

	if t.buf != nil {
		t.buf = t.buf[0:0] // Reset t.buf via re-slice.
	}

	// NOTE: Leave t.seqSnapEnd unchanged in case we're applying the
	// batch because t.buf got too big.

	return nil
}

// Appends b to end of t.buf, and returns that suffix slice of t.buf
// that has the appended copy of the input b.
func (t *BleveDestPartition) appendToBufUnlocked(b []byte) []byte {
	if len(b) <= 0 {
		return b
	}
	if t.buf == nil {
		// TODO: parameterize initial buf capacity.
		t.buf = make([]byte, 0, BLEVE_DEST_INITIAL_BUF_SIZE_BYTES)
	}
	t.buf = append(t.buf, b...)

	return t.buf[len(t.buf)-len(b):]
}

// ---------------------------------------------------------

// Returns a bleve.IndexAlias that represents all the PIndexes for the
// index, including perhaps bleve remote client PIndexes.
//
// TODO: Perhaps need a tighter check around indexUUID, as the current
// implementation might have a race where old pindexes with a matching
// (but invalid) indexUUID might be hit.
//
// TODO: If this returns an error, perhaps the caller somewhere up the
// chain should close the cancelCh to help stop any other inflight
// activities.
func bleveIndexAlias(mgr *Manager, indexName, indexUUID string,
	consistencyParams *ConsistencyParams,
	cancelCh <-chan bool) (bleve.IndexAlias, error) {
	localPIndexes, remotePlanPIndexes, err :=
		mgr.CoveringPIndexes(indexName, indexUUID, PlanPIndexNodeCanRead)
	if err != nil {
		return nil, fmt.Errorf("bleve: bleveIndexAlias, err: %v", err)
	}

	alias := bleve.NewIndexAlias()

	for _, remotePlanPIndex := range remotePlanPIndexes {
		baseURL := "http://" + remotePlanPIndex.NodeDef.HostPort +
			"/api/pindex/" + remotePlanPIndex.PlanPIndex.Name
		alias.Add(&PIndexClient{
			QueryURL:    baseURL + "/query",
			CountURL:    baseURL + "/count",
			Consistency: consistencyParams,
			// TODO: Propagate auth to remote client.
		})
	}

	// TODO: Should kickoff remote queries concurrently before we wait.

	err = ConsistencyWaitGroup(indexName, consistencyParams,
		cancelCh, localPIndexes,
		func(localPIndex *PIndex) error {
			bindex, ok := localPIndex.Impl.(bleve.Index)
			if !ok || bindex == nil ||
				!strings.HasPrefix(localPIndex.IndexType, "bleve") {
				return fmt.Errorf("bleve: wrong type, localPIndex: %#v",
					localPIndex)
			}
			alias.Add(bindex)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return alias, nil
}

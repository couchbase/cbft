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
	"bytes"
	"container/heap"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	log "github.com/couchbaselabs/clog"

	"github.com/dustin/go-jsonpointer"

	"github.com/steveyen/gkvlite"
)

// TODO: Compaction!
// TODO: Snapshots, so that queries don't see mutations until commited/flushed.
// TODO: Partial rollback.

var VLiteFileService = NewFileService(30)

type VLiteParams struct {
	// Path is a jsonpointer path used to retrieve the indexed
	// secondary value from each document.  When Path is "" (empty
	// string), then instead of behaving like a secondary index, then
	// the VLite will use the original source document id as its
	// stored key and the document bytes are used as the stored value.
	Path string `json:"path"`
}

type VLite struct {
	params *VLiteParams
	path   string
	file   FileLike

	// Called when we want mgr to restart the VLite, like on rollback.
	restart func()

	m          sync.Mutex // Protects the fields that follow.
	partitions map[string]*VLitePartition

	store      *gkvlite.Store
	mainColl   *gkvlite.Collection // Keyed by $secondaryIndexValue\xff$docId.
	backColl   *gkvlite.Collection // Keyed by docId.
	opaqueColl *gkvlite.Collection // Keyed by partitionId.
	seqColl    *gkvlite.Collection // Keyed by partitionId.
}

// Used to track state for a single partition.
type VLitePartition struct {
	vlite        *VLite
	partition    string
	partitionKey []byte // Key used for opaqueColl and seqColl.

	// The parent vlite.m protects the following fields.
	seqMax      uint64 // Max seq # we've seen for this partition.
	seqMaxBatch uint64 // Max seq # that got through batch apply/commit.
	seqSnapEnd  uint64 // To track snapshot end seq # for this partition.

	cwrCh    chan *ConsistencyWaitReq
	cwrQueue cwrQueue
}

type VLiteQueryParams struct {
	Timeout     int64              `json:"timeout"`
	Consistency *ConsistencyParams `json:"consistency"`
	Limit       uint64             `json:"limit"`
	Skip        uint64             `json:"skip"`

	Key            string `json:"key"`
	StartInclusive string `json:"startInclusive"`
	EndExclusive   string `json:"endExclusive"`
}

func NewVLiteQueryParams() *VLiteQueryParams { return &VLiteQueryParams{} }

type VLiteQueryResults struct {
	Results []*VLiteQueryResult `json:"results"`
}

type VLiteQueryResult struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type VLiteGatherer struct {
	localVLites   []*VLite
	remoteClients []*PIndexClient
}

var EMPTY_BYTES = []byte{}

func NewVLite(vliteParams *VLiteParams, path string, file FileLike,
	restart func()) (*VLite, error) {
	store, err := gkvlite.NewStore(file)
	if err != nil {
		return nil, err
	}

	return &VLite{
		params:     vliteParams,
		path:       path,
		file:       file,
		store:      store,
		mainColl:   store.SetCollection("main", nil),
		backColl:   store.SetCollection("back", nil),
		opaqueColl: store.SetCollection("opaque", nil),
		seqColl:    store.SetCollection("seq", nil),
		restart:    restart,
		partitions: make(map[string]*VLitePartition),
	}, nil
}

// ---------------------------------------------------------

func init() {
	RegisterPIndexImplType("vlite", &PIndexImplType{
		Validate: ValidateVLitePIndexImpl,

		New:   NewVLitePIndexImpl,
		Open:  OpenVLitePIndexImpl,
		Count: CountVLitePIndexImpl,
		Query: QueryVLitePIndexImpl,

		Description: "vlite - lightweight, view-like index",
		StartSample: VLiteParams{},
	})

	RegisterPIndexImplType("vlite-mem", &PIndexImplType{
		Validate: ValidateVLitePIndexImpl,

		New:   NewVLitePIndexImpl,
		Open:  OpenVLitePIndexImpl,
		Count: CountVLitePIndexImpl,
		Query: QueryVLitePIndexImpl,

		Description: "vlite-mem - lightweight, view-like index (in memory only)",
		StartSample: VLiteParams{},
	})
}

func ValidateVLitePIndexImpl(indexType, indexName, indexParams string) error {
	vliteParams := VLiteParams{}
	if len(indexParams) > 0 {
		return json.Unmarshal([]byte(indexParams), &vliteParams)
	}
	return nil
}

func NewVLitePIndexImpl(indexType, indexParams, path string,
	restart func()) (PIndexImpl, Dest, error) {
	vliteParams := VLiteParams{}
	if len(indexParams) > 0 {
		err := json.Unmarshal([]byte(indexParams), &vliteParams)
		if err != nil {
			return nil, nil, fmt.Errorf("error: parse vlite index params: %v", err)
		}
	}

	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, nil, err
	}

	pathMeta := path + string(os.PathSeparator) + "VLITE_META"
	err = ioutil.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		return nil, nil, err
	}

	var pathStore string
	var f FileLike

	if indexType != "vlite-mem" {
		pathStore = path + string(os.PathSeparator) + "store.gkvlite"
		f, err = VLiteFileService.OpenFile(pathStore,
			os.O_RDWR|os.O_CREATE|os.O_EXCL)
		if err != nil {
			os.Remove(pathMeta)
			return nil, nil, err
		}
	}

	vlite, err := NewVLite(&vliteParams, path, f, restart)
	if err != nil {
		if f != nil {
			f.Close()
		}
		if pathStore != "" {
			os.Remove(pathStore)
		}
		os.Remove(pathMeta)
		return nil, nil, err
	}

	return vlite, &DestForwarder{DestProvider: vlite}, nil
}

func OpenVLitePIndexImpl(indexType, path string,
	restart func()) (PIndexImpl, Dest, error) {
	if indexType == "vlite-mem" {
		return nil, nil, fmt.Errorf("error: cannot re-open vlite-mem, path: %s", path)
	}

	buf, err := ioutil.ReadFile(path + string(os.PathSeparator) + "VLITE_META")
	if err != nil {
		return nil, nil, err
	}

	vliteParams := VLiteParams{}
	err = json.Unmarshal(buf, &vliteParams)
	if err != nil {
		return nil, nil, fmt.Errorf("error: parse vlite index params: %v", err)
	}

	pathStore := path + string(os.PathSeparator) + "store.gkvlite"
	f, err := VLiteFileService.OpenFile(pathStore, os.O_RDWR)
	if err != nil {
		return nil, nil, err
	}

	vlite, err := NewVLite(&vliteParams, path, f, restart)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	return vlite, &DestForwarder{DestProvider: vlite}, nil
}

// ---------------------------------------------------------------

func CountVLitePIndexImpl(mgr *Manager, indexName, indexUUID string) (
	uint64, error) {
	vg, err := vliteGatherer(mgr, indexName, indexUUID, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("CountVLitePIndexImpl indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	return vg.Count(nil)
}

func QueryVLitePIndexImpl(mgr *Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	vliteQueryParams := NewVLiteQueryParams()
	err := json.Unmarshal(req, vliteQueryParams)
	if err != nil {
		return fmt.Errorf("QueryVLitePIndexImpl parsing vliteQueryParams,"+
			" req: %s, err: %v", req, err)
	}

	cancelCh := TimeoutCancelChan(vliteQueryParams.Timeout)

	vg, err := vliteGatherer(mgr, indexName, indexUUID,
		vliteQueryParams.Consistency, cancelCh)
	if err != nil {
		return err
	}

	return vg.Query(vliteQueryParams, res, cancelCh)
}

// ---------------------------------------------------------

func (t *VLite) Dest(partition string) (Dest, error) {
	t.m.Lock()
	defer t.m.Unlock()

	return t.getPartitionUnlocked(partition)
}

func (t *VLite) getPartitionUnlocked(partition string) (*VLitePartition, error) {
	if t.store == nil {
		return nil, fmt.Errorf("VLite already closed")
	}

	bdp, exists := t.partitions[partition]
	if !exists || bdp == nil {
		bdp = &VLitePartition{
			vlite:        t,
			partition:    partition,
			partitionKey: []byte(partition),
			cwrCh:        make(chan *ConsistencyWaitReq, 1),
			cwrQueue:     cwrQueue{},
		}
		heap.Init(&bdp.cwrQueue)

		go RunConsistencyWaitQueue(bdp.cwrCh, &t.m, &bdp.cwrQueue,
			func() uint64 { return bdp.seqMaxBatch })

		t.partitions[partition] = bdp
	}

	return bdp, nil
}

// ---------------------------------------------------------

func (t *VLite) Close() error {
	t.m.Lock()
	defer t.m.Unlock()

	return t.closeUnlocked()
}

func (t *VLite) closeUnlocked() error {
	if t.store == nil {
		return nil // Already closed.
	}

	for _, bdp := range t.partitions {
		close(bdp.cwrCh)
	}
	t.partitions = make(map[string]*VLitePartition)

	t.store.Close()
	t.store = nil

	return nil
}

// ---------------------------------------------------------

func (t *VLite) Rollback(partition string, rollbackSeq uint64) error {
	log.Printf("vlite dest rollback, partition: %s, rollbackSeq: %d",
		partition, rollbackSeq)

	t.m.Lock()
	defer t.m.Unlock()

	// NOTE: A rollback of any partition means a rollback of all
	// partitions, since they all share a single VLite store.  That's
	// why we grab and keep VLite.m locked.
	//
	// TODO: Implement partial rollback one day.  Implementation
	// sketch: leverage additional gkvlite rollback features where
	// we'd loop through rollback attempts until we reach the
	// rollbackSeq, or stop once we've rollback'ed to zero.
	//
	// For now, always rollback to zero, in which we close the pindex,
	// erase files and have the janitor rebuild from scratch.

	err := t.closeUnlocked()
	if err != nil {
		return fmt.Errorf("VLite can't close during rollback, err: %v", err)
	}

	os.RemoveAll(t.path)

	t.restart()

	return nil
}

// ---------------------------------------------------------

func (t *VLite) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh chan string) error {
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
			t.m.Lock()
			seqMaxBatch := bdp.seqMaxBatch
			t.m.Unlock()
			return seqMaxBatch
		})
}

// ---------------------------------------------------------

func (t *VLite) Count(pindex *PIndex, cancelCh chan string) (uint64, error) {
	return t.CountMainColl(cancelCh)
}

// ---------------------------------------------------------

var entryKeyPrefix = []byte("{\"key\":")
var entryKeyPrefixSep = append([]byte("\n,"), entryKeyPrefix...)
var entryValPrefix = []byte(", \"val\":")
var entrySuffix = []byte("}")

func (t *VLite) Query(pindex *PIndex, req []byte, w io.Writer,
	cancelCh chan string) error {
	vliteQueryParams := NewVLiteQueryParams()
	err := json.Unmarshal(req, vliteQueryParams)
	if err != nil {
		return fmt.Errorf("VLite.Query parsing vliteQueryParams,"+
			" req: %s, err: %v", req, err)
	}

	err = ConsistencyWaitPIndex(pindex, t,
		vliteQueryParams.Consistency, cancelCh)
	if err != nil {
		return err
	}

	w.Write([]byte(`{"results":[`))

	first := true

	err = t.QueryMainColl(vliteQueryParams, cancelCh, func(i *gkvlite.Item) bool {
		if first {
			w.Write(entryKeyPrefix)
			first = false
		} else {
			w.Write(entryKeyPrefixSep)
		}
		buf, _ := json.Marshal(string(i.Key))
		w.Write(buf)
		w.Write(entryValPrefix)
		buf, _ = json.Marshal(string(i.Val))
		w.Write(buf)
		w.Write(entrySuffix)

		return true
	})

	w.Write([]byte("]}"))

	return err
}

// ---------------------------------------------------------

func (t *VLite) CountMainColl(cancelCh chan string) (uint64, error) {
	t.m.Lock()
	storeRO := t.store.Snapshot()
	t.m.Unlock()
	defer storeRO.Close()

	mainCollRO := storeRO.GetCollection("main")

	numItems, _, err := mainCollRO.GetTotals()
	if err != nil {
		return 0, fmt.Errorf("VLite.Count get totals err: %v", err)
	}

	return numItems, nil
}

func (t *VLite) QueryMainColl(p *VLiteQueryParams, cancelCh chan string,
	cb func(*gkvlite.Item) bool) error {
	startInclusive := []byte(p.StartInclusive)
	endExclusive := []byte(p.EndExclusive)

	if p.Key != "" {
		if t.params.Path != "" {
			startInclusive = []byte(p.Key + "\xff")
			endExclusive = []byte(p.Key + "\xff\xff")
		} else {
			startInclusive = []byte(p.Key)
			endExclusive = []byte(p.Key + "\xff")
		}
	}

	log.Printf("QueryMain startInclusive: %s, endExclusive: %s",
		startInclusive, endExclusive)

	totVisits := uint64(0)

	t.m.Lock()
	storeRO := t.store.Snapshot()
	t.m.Unlock()
	defer storeRO.Close()

	mainCollRO := storeRO.GetCollection("main")

	return mainCollRO.VisitItemsAscend(startInclusive, true,
		func(item *gkvlite.Item) bool {
			ok := len(endExclusive) <= 0 ||
				bytes.Compare(item.Key, endExclusive) < 0
			if !ok {
				return false
			}

			totVisits++
			if totVisits > p.Skip {
				if !cb(item) {
					return false
				}
			}

			return p.Limit <= 0 || (totVisits < p.Skip+p.Limit)
		})
}

// ---------------------------------------------------------

func (t *VLitePartition) Close() error {
	return t.vlite.Close()
}

func (t *VLitePartition) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	storeKey := append([]byte(nil), key...)
	storeVal := append([]byte(nil), val...)

	if t.vlite.params.Path != "" {
		secVal, err := jsonpointer.Find(val, t.vlite.params.Path)
		if err != nil {
			log.Printf("jsonpointer path: %s, key: %s, val: %s, err: %v",
				t.vlite.params.Path, key, val, err)
			return nil // TODO: Return or report error here?
		}
		if len(secVal) <= 0 {
			log.Printf("no matching path: %s, key: %s, val: %s",
				t.vlite.params.Path, key, val)
			return nil // TODO: Return or report error here?
		}
		if len(secVal) >= 2 && secVal[0] == '"' && secVal[len(secVal)-1] == '"' {
			var s string
			err := json.Unmarshal(secVal, &s)
			if err != nil {
				return nil // TODO: Return or report error here?
			}
			secVal = []byte(s)
		}

		storeKey = []byte(string(secVal) + "\xff" + string(key))
		storeVal = EMPTY_BYTES
	}

	log.Printf("OnDataUpdate, storeKey: %s", storeKey)

	t.vlite.m.Lock()
	defer t.vlite.m.Unlock()

	if t.vlite.params.Path != "" {
		backKey, err := t.vlite.backColl.Get(key)
		if err != nil && len(backKey) > 0 {
			_, err := t.vlite.mainColl.Delete(backKey)
			if err != nil {
				log.Printf("mainColl.Delete err: %v", err)
			}
		}

		err = t.vlite.backColl.Set(key, storeKey)
		if err != nil {
			log.Printf("backColl.Set err: %v", err)
		}
	}

	err := t.vlite.mainColl.Set(storeKey, storeVal)
	if err != nil {
		log.Printf("mainColl.Set err: %v", err)
	}

	return t.updateSeqUnlocked(seq)
}

func (t *VLitePartition) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	t.vlite.m.Lock()
	defer t.vlite.m.Unlock()

	if t.vlite.params.Path != "" {
		backKey, err := t.vlite.backColl.Get(key)
		if err != nil && len(backKey) > 0 {
			t.vlite.mainColl.Delete(backKey)
			t.vlite.backColl.Delete(key)
		}
	} else {
		t.vlite.mainColl.Delete(key)
	}

	return t.updateSeqUnlocked(seq)
}

func (t *VLitePartition) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	t.vlite.m.Lock()
	defer t.vlite.m.Unlock()

	err := t.applyBatchUnlocked()
	if err != nil {
		return err
	}

	t.seqSnapEnd = snapEnd

	return nil
}

func (t *VLitePartition) SetOpaque(partition string, value []byte) error {
	t.vlite.m.Lock()
	defer t.vlite.m.Unlock()

	return t.vlite.opaqueColl.Set(t.partitionKey, append([]byte(nil), value...))
}

func (t *VLitePartition) GetOpaque(partition string) ([]byte, uint64, error) {
	t.vlite.m.Lock()
	defer t.vlite.m.Unlock()

	opaqueBuf, err := t.vlite.opaqueColl.Get(t.partitionKey)
	if err != nil {
		return nil, 0, err
	}

	if t.seqMax <= 0 {
		seqBuf, err := t.vlite.seqColl.Get(t.partitionKey)
		if err != nil {
			return nil, 0, err
		}
		if len(seqBuf) <= 0 {
			return opaqueBuf, 0, nil // No seqMax buf is a valid case.
		}
		if len(seqBuf) != 8 {
			return nil, 0, fmt.Errorf("unexpected size for seqMax bytes")
		}
		t.seqMax = binary.BigEndian.Uint64(seqBuf[0:8])
	}

	return opaqueBuf, t.seqMax, nil
}

func (t *VLitePartition) Rollback(partition string, rollbackSeq uint64) error {
	return t.vlite.Rollback(partition, rollbackSeq)
}

func (t *VLitePartition) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh chan string) error {
	return t.vlite.ConsistencyWait(partition,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *VLitePartition) Count(pindex *PIndex, cancelCh chan string) (
	uint64, error) {
	return t.vlite.Count(pindex, cancelCh)
}

func (t *VLitePartition) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh chan string) error {
	return t.vlite.Query(pindex, req, res, cancelCh)
}

// ---------------------------------------------------------

func (t *VLitePartition) updateSeqUnlocked(seq uint64) error {
	if t.seqMax < seq {
		t.seqMax = seq

		seqMaxBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(seqMaxBuf, t.seqMax)

		t.vlite.seqColl.Set(t.partitionKey, seqMaxBuf)
	}

	if seq < t.seqSnapEnd {
		return nil
	}

	return t.applyBatchUnlocked()
}

func (t *VLitePartition) applyBatchUnlocked() error {
	// TODO: Locking!  What if store == nil!

	if t.vlite.file != nil { // When not memory-only.
		err := t.vlite.store.Flush()
		if err != nil {
			return err
		}
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

	return nil
}

// ---------------------------------------------------------

// Returns a VLiteGatherer that represents all the PIndexes for the
// index, including perhaps VLite remote client PIndexes.
//
// TODO: Perhaps need a tighter check around indexUUID, as the current
// implementation might have a race where old pindexes with a matching
// (but invalid) indexUUID might be hit.
//
// TODO: If this returns an error, perhaps the caller somewhere up the
// chain should close the cancelCh to help stop any other inflight
// activities.
func vliteGatherer(mgr *Manager, indexName, indexUUID string,
	consistencyParams *ConsistencyParams,
	cancelCh chan string) (*VLiteGatherer, error) {
	localPIndexes, remotePlanPIndexes, err :=
		mgr.CoveringPIndexes(indexName, indexUUID, PlanPIndexNodeCanRead)
	if err != nil {
		return nil, fmt.Errorf("vliteGatherer, err: %v", err)
	}

	rv := &VLiteGatherer{}

	for _, remotePlanPIndex := range remotePlanPIndexes {
		baseURL := "http://" + remotePlanPIndex.NodeDef.HostPort +
			"/api/pindex/" + remotePlanPIndex.PlanPIndex.Name
		rv.remoteClients = append(rv.remoteClients, &PIndexClient{
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
			vlite, ok := localPIndex.Impl.(*VLite)
			if !ok || vlite == nil ||
				!strings.HasPrefix(localPIndex.IndexType, "vlite") {
				return fmt.Errorf("wrong type, localPIndex: %#v", localPIndex)
			}
			rv.localVLites = append(rv.localVLites, vlite)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return rv, nil
}

func (vg *VLiteGatherer) Count(cancelCh chan string) (uint64, error) {
	var totalM sync.Mutex
	var totalErr error
	var total uint64

	var wg sync.WaitGroup

	for _, localVLite := range vg.localVLites {
		wg.Add(1)
		go func(localVLite *VLite) {
			defer wg.Done()

			t, err := localVLite.CountMainColl(cancelCh)
			totalM.Lock()
			if err == nil {
				total += t
			} else {
				totalErr = err
			}
			totalM.Unlock()
		}(localVLite)
	}

	for _, remoteClient := range vg.remoteClients {
		wg.Add(1)
		go func(remoteClient *PIndexClient) {
			defer wg.Done()

			t, err := remoteClient.Count()
			totalM.Lock()
			if err == nil {
				total += t
			} else {
				totalErr = err
			}
			totalM.Unlock()
		}(remoteClient)
	}

	wg.Wait()

	return total, totalErr
}

func (vg *VLiteGatherer) Query(p *VLiteQueryParams, w io.Writer,
	cancelCh chan string) error {
	pBuf, err := json.Marshal(p)
	if err != nil {
		return err
	}

	n := len(vg.localVLites) + len(vg.remoteClients)
	errCh := make(chan error, n)
	doneCh := make(chan struct{})

	scanCursors := ScanCursors{}
	heap.Init(&scanCursors)

	for _, localVLite := range vg.localVLites {
		resultCh := make(chan *gkvlite.Item, 1)

		go func(resultCh chan *gkvlite.Item, localVLite *VLite) {
			defer close(resultCh)

			err := localVLite.QueryMainColl(p, cancelCh,
				func(item *gkvlite.Item) bool {
					select {
					case <-doneCh:
						return false
					case resultCh <- item:
					}
					return true
				})
			if err != nil {
				errCh <- err
			}
		}(resultCh, localVLite)

		scanCursor := &VLiteScanCursor{resultCh: resultCh}
		if scanCursor.Next() {
			heap.Push(&scanCursors, scanCursor)
		}
	}

	for _, remoteClient := range vg.remoteClients {
		resultCh := make(chan *gkvlite.Item, 1)

		go func(resultCh chan *gkvlite.Item, remoteClient *PIndexClient) {
			defer close(resultCh)

			respBuf, err := remoteClient.Query(pBuf)
			if err != nil {
				errCh <- err
				return
			}

			results := &VLiteQueryResults{}
			err = json.Unmarshal(respBuf, results)
			if err != nil {
				errCh <- err
				return
			}

			for _, result := range results.Results {
				item := &gkvlite.Item{
					Key: []byte(result.Key),
					Val: []byte(result.Val),
				}

				select {
				case <-doneCh:
					return
				case resultCh <- item:
				}
			}
		}(resultCh, remoteClient)

		scanCursor := &VLiteScanCursor{resultCh: resultCh}
		if scanCursor.Next() {
			heap.Push(&scanCursors, scanCursor)
		}
	}

	w.Write([]byte(`{"results":[`))

	first := true

	for len(scanCursors) > 0 {
		// TODO: Limit and skip.  Need to use 0 skip/limit in child
		// QueryMainColl()'s and do the skip/limit processing here.
		scanCursor := heap.Pop(&scanCursors).(ScanCursor)
		if !scanCursor.Done() {
			if first {
				w.Write(entryKeyPrefix)
				first = false
			} else {
				w.Write(entryKeyPrefixSep)
			}
			buf, _ := json.Marshal(string(scanCursor.Key()))
			w.Write(buf)
			w.Write(entryValPrefix)
			buf, _ = json.Marshal(string(scanCursor.Val()))
			w.Write(buf)
			w.Write(entrySuffix)

			if scanCursor.Next() {
				heap.Push(&scanCursors, scanCursor)
			}
		}
	}

	w.Write([]byte("]}"))

	close(doneCh)

	go func() {
		for _, scanCursor := range scanCursors {
			for scanCursor.Next() {
				// Eat results to clear out QueryMainColl() goroutines.
			}
		}
	}()

	select {
	case err = <-errCh:
	default:
	}

	return err
}

// ---------------------------------------------------------

type VLiteScanCursor struct {
	resultCh chan *gkvlite.Item
	done     bool
	curr     *gkvlite.Item
}

func (c *VLiteScanCursor) Done() bool {
	return c.done
}

func (c *VLiteScanCursor) Key() []byte {
	if c.curr == nil {
		return nil
	}
	return c.curr.Key
}

func (c *VLiteScanCursor) Val() []byte {
	if c.curr == nil {
		return nil
	}
	return c.curr.Val
}

func (c *VLiteScanCursor) Next() bool {
	c.curr = nil
	if c.done {
		return false
	}
	i, ok := <-c.resultCh
	if !ok {
		c.done = true
		return false
	}
	c.curr = i
	return true
}

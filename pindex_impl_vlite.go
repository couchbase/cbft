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
// TODO: Scatter/gather against local vlite PIndexes.
// TODO: Remote client vlite.
// TODO: Snapshots, so that snapshots aren't visible yet until commited/flushed.
// TODO: Partial rollback.

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

// ---------------------------------------------------------

type VLiteParams struct {
	// Path is a jsonpointer path used to retrieve the indexed
	// secondary value from each document.
	Path string `json:"path"`
}

type VLite struct {
	params     *VLiteParams
	path       string
	file       *os.File
	store      *gkvlite.Store
	mainColl   *gkvlite.Collection // Keyed by $secondaryIndexValue\xff$docId.
	backColl   *gkvlite.Collection // Keyed by docId.
	opaqueColl *gkvlite.Collection // Keyed by partitionId.
	seqColl    *gkvlite.Collection // Keyed by partitionId.

	// Called when we want mgr to restart the VLite, like on rollback.
	restart func()

	m          sync.Mutex // Protects the fields that follow.
	partitions map[string]*VLitePartition
}

// Used to track state for a single partition.
type VLitePartition struct {
	partition    string
	partitionBuf []byte // Key used for opaqueColl and seqColl.

	m           sync.Mutex // Protects the fields that follow.
	seqMax      uint64     // Max seq # we've seen for this partition.
	seqMaxBatch uint64     // Max seq # that got through batch apply/commit.
	seqSnapEnd  uint64     // To track snapshot end seq # for this partition.

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

type VLiteGatherer struct {
	localVLites []*VLite
}

// ---------------------------------------------------------

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

	if vliteParams.Path == "" {
		return nil, nil, fmt.Errorf("error: missing path for vlite index params")
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
	var f *os.File

	if indexType != "vlite-mem" {
		pathStore = path + string(os.PathSeparator) + "store.gkvlite"
		f, err = os.OpenFile(pathStore, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
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

	return vlite, vlite, nil
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
	f, err := os.OpenFile(pathStore, os.O_RDWR, 0666)
	if err != nil {
		return nil, nil, err
	}

	vlite, err := NewVLite(&vliteParams, path, f, restart)
	if err != nil {
		f.Close()
		return nil, nil, err
	}

	return vlite, vlite, nil
}

// ---------------------------------------------------------------

func NewVLite(vliteParams *VLiteParams, path string, file *os.File,
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

// ---------------------------------------------------------------

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

	return vg.Query(vliteQueryParams, res)
}

// ---------------------------------------------------------

func (t *VLite) getPartition(partition string) (*VLitePartition, error) {
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
			partition:    partition,
			partitionBuf: []byte(partition),
			cwrCh:        make(chan *ConsistencyWaitReq, 1),
			cwrQueue:     cwrQueue{},
		}
		heap.Init(&bdp.cwrQueue)

		go bdp.run()

		t.partitions[partition] = bdp
	}

	return bdp, nil
}

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

func (t *VLite) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	bdp, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.OnDataUpdate(t, key, seq, val)
}

func (t *VLite) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	bdp, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.OnDataDelete(t, key, seq)
}

func (t *VLite) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	bdp, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.OnSnapshotStart(t, snapStart, snapEnd)
}

func (t *VLite) SetOpaque(partition string, value []byte) error {
	bdp, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.SetOpaque(t, value)
}

func (t *VLite) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	bdp, err := t.getPartition(partition)
	if err != nil {
		return nil, 0, err
	}

	return bdp.GetOpaque(t)
}

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
			bdp.m.Lock()
			defer bdp.m.Unlock()
			return bdp.seqMaxBatch
		})
}

func (t *VLite) Count(pindex *PIndex, cancelCh chan string) (uint64, error) {
	if pindex == nil ||
		pindex.Impl == nil ||
		!strings.HasPrefix(pindex.IndexType, "vlite") {
		return 0, fmt.Errorf("VLite.Count bad pindex: %#v", pindex)
	}

	vlite, ok := pindex.Impl.(*VLite)
	if !ok || vlite == nil {
		return 0, fmt.Errorf("VLite.Count pindex not a vlite.Index: %#v", pindex)
	}

	return vlite.CountStore(cancelCh)
}

func (t *VLite) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh chan string) error {
	if pindex == nil ||
		pindex.Impl == nil ||
		!strings.HasPrefix(pindex.IndexType, "vlite") {
		return fmt.Errorf("VLite.Query bad pindex: %#v", pindex)
	}

	vlite, ok := pindex.Impl.(*VLite)
	if !ok || vlite == nil {
		return fmt.Errorf("VLite.Query pindex not a vlite.Index: %#v", pindex)
	}

	vliteQueryParams := NewVLiteQueryParams()
	err := json.Unmarshal(req, vliteQueryParams)
	if err != nil {
		return fmt.Errorf("VLite.Query parsing vliteQueryParams,"+
			" req: %s, err: %v", req, err)
	}

	consistencyParams := vliteQueryParams.Consistency
	if consistencyParams != nil &&
		consistencyParams.Level != "" &&
		consistencyParams.Vectors != nil {
		consistencyVector := consistencyParams.Vectors[pindex.IndexName]
		if consistencyVector != nil {
			err := ConsistencyWaitPartitions(t, pindex.sourcePartitionsArr,
				consistencyParams.Level, consistencyVector, cancelCh)
			if err != nil {
				return err
			}
		}
	}

	return vlite.QueryStore(vliteQueryParams, res)
}

// ---------------------------------------------------------

func (t *VLite) CountStore(cancelCh chan string) (uint64, error) {
	numItems, _, err := t.backColl.GetTotals()
	if err != nil {
		return 0, fmt.Errorf("VLite.Count get totals err: %v", err)
	}

	return numItems, nil
}

func (t *VLite) QueryStore(p *VLiteQueryParams, w io.Writer) error {
	startInclusive := []byte(p.StartInclusive)
	endExclusive := []byte(p.EndExclusive)

	if p.Key != "" {
		startInclusive = []byte(p.Key + "\xff")
		endExclusive = []byte(p.Key + "\xff\xff")
	}

	log.Printf("QueryStore startInclusive: %s, endExclusive: %s",
		startInclusive, endExclusive)

	entryBefore := []byte("{\"key\":\"")
	entryBeforeSep := append([]byte("\n,"), entryBefore...)
	entryMiddle := []byte("\", \"id\":\"")
	entryAfter := []byte("\"}")

	w.Write([]byte(`{"results":[`))

	totVisits := uint64(0)

	err := t.mainColl.VisitItemsAscend(startInclusive, false,
		func(i *gkvlite.Item) bool {
			ok := len(endExclusive) <= 0 ||
				bytes.Compare(i.Key, endExclusive) < 0
			if !ok {
				return false
			}

			totVisits++
			if totVisits > p.Skip {
				if totVisits <= p.Skip+1 {
					w.Write(entryBefore)
				} else {
					w.Write(entryBeforeSep)
				}
				parts := bytes.Split(i.Key, []byte{0xff})
				w.Write(parts[0]) // TODO: Proper encoding of secKey.
				w.Write(entryMiddle)
				w.Write(parts[1]) // TODO: Proper encoding of docId.
				w.Write(entryAfter)
			}

			return p.Limit <= 0 || (totVisits < p.Skip+p.Limit)
		})

	w.Write([]byte("]}"))

	return err
}

// ---------------------------------------------------------

func (t *VLitePartition) run() {
	for cwr := range t.cwrCh {
		t.m.Lock()

		if cwr.ConsistencyLevel == "" {
			close(cwr.DoneCh) // We treat "" like stale=ok, so we're done.
		} else if cwr.ConsistencyLevel == "at_plus" {
			if cwr.ConsistencySeq > t.seqMaxBatch {
				heap.Push(&t.cwrQueue, cwr)
			} else {
				close(cwr.DoneCh)
			}
		} else {
			cwr.DoneCh <- fmt.Errorf("consistency wait unsupported level: %s,"+
				" cwr: %#v", cwr.ConsistencyLevel, cwr)
			close(cwr.DoneCh)
		}

		t.m.Unlock()
	}

	// If we reach here, then we're closing down so cancel/error any
	// callers waiting for consistency.
	t.m.Lock()
	defer t.m.Unlock()

	err := fmt.Errorf("consistency wait closed")

	for _, cwr := range t.cwrQueue {
		cwr.DoneCh <- err
		close(cwr.DoneCh)
	}
}

// ---------------------------------------------------------

var EMPTY_BYTES = []byte{}

func (t *VLitePartition) OnDataUpdate(vlite *VLite,
	key []byte, seq uint64, val []byte) error {
	secVal, err := jsonpointer.Find(val, vlite.params.Path)
	if err != nil {
		log.Printf("jsonpointer path: %s, key: %s, val: %s, err: %v",
			vlite.params.Path, key, val, err)
		return nil // TODO: Return or report error here?
	}
	if len(secVal) <= 0 {
		log.Printf("no matching path: %s, key: %s, val: %s",
			vlite.params.Path, key, val)
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

	secKey := []byte(string(secVal) + "\xff" + string(key))

	log.Printf("OnDataUpdate, secKey: %s", secKey)

	t.m.Lock()
	defer t.m.Unlock()

	// TODO: All these deletes and insert need to be atomic?

	backKey, err := vlite.backColl.Get(key)
	if err != nil && len(backKey) > 0 {
		_, err := vlite.mainColl.Delete(backKey)
		if err != nil {
			log.Printf("mainColl.Delete err: %v", err)
		}
		_, err = vlite.backColl.Delete(key)
		if err != nil {
			log.Printf("backColl.Delete err: %v", err)
		}
	}

	err = vlite.mainColl.Set(secKey, EMPTY_BYTES)
	if err != nil {
		log.Printf("mainColl.Set err: %v", err)
	}
	err = vlite.backColl.Set(key, secKey)
	if err != nil {
		log.Printf("backColl.Set err: %v", err)
	}

	return t.updateSeqUnlocked(vlite, seq)
}

func (t *VLitePartition) OnDataDelete(vlite *VLite,
	key []byte, seq uint64) error {
	t.m.Lock()
	defer t.m.Unlock()

	// TODO: All these deletes need to be atomic?

	backKey, err := vlite.backColl.Get(key)
	if err != nil && len(backKey) > 0 {
		vlite.mainColl.Delete(backKey)
		vlite.backColl.Delete(key)
	}

	return t.updateSeqUnlocked(vlite, seq)
}

func (t *VLitePartition) OnSnapshotStart(vlite *VLite,
	snapStart, snapEnd uint64) error {
	t.m.Lock()
	defer t.m.Unlock()

	err := t.applyBatchUnlocked(vlite)
	if err != nil {
		return err
	}

	t.seqSnapEnd = snapEnd

	return nil
}

func (t *VLitePartition) SetOpaque(vlite *VLite, value []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	return vlite.opaqueColl.Set(t.partitionBuf, append([]byte(nil), value...))
}

func (t *VLitePartition) GetOpaque(vlite *VLite) ([]byte, uint64, error) {
	t.m.Lock()
	defer t.m.Unlock()

	opaqueBuf, err := vlite.opaqueColl.Get(t.partitionBuf)
	if err != nil {
		return nil, 0, err
	}

	if t.seqMax <= 0 {
		seqBuf, err := vlite.seqColl.Get(t.partitionBuf)
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

// ---------------------------------------------------------

func (t *VLitePartition) updateSeqUnlocked(vlite *VLite,
	seq uint64) error {
	if t.seqMax < seq {
		t.seqMax = seq

		seqMaxBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(seqMaxBuf, t.seqMax)

		vlite.seqColl.Set(t.partitionBuf, seqMaxBuf)
	}

	if seq < t.seqSnapEnd {
		return nil
	}

	return t.applyBatchUnlocked(vlite)
}

func (t *VLitePartition) applyBatchUnlocked(vlite *VLite) error {
	// TODO: Locking!  What if store == nil!

	if vlite.file != nil { // When not memory-only.
		err := vlite.store.Flush()
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
// TODO: Need to implement remote part of this.
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

	for _, localPIndex := range localPIndexes {
		vlite, ok := localPIndex.Impl.(*VLite)
		if ok && vlite != nil &&
			strings.HasPrefix(localPIndex.IndexType, "vlite") {
			rv.localVLites = append(rv.localVLites, vlite)
		} else {
			return nil, fmt.Errorf("vliteGatherer,"+
				" localPIndex is not vlite: %#v", localPIndex)
		}
	}

	for _, remotePIndex := range remotePlanPIndexes {
		fmt.Printf("do something with remotePIndex: %v", remotePIndex) // TODO.
	}

	return rv, nil
}

func (vg *VLiteGatherer) Count(cancelCh chan string) (uint64, error) {
	// TODO: Implement scatter/gather.
	return vg.localVLites[0].CountStore(cancelCh)
}

func (vg *VLiteGatherer) Query(p *VLiteQueryParams, w io.Writer) error {
	// TODO: Implement scatter/gather.
	return vg.localVLites[0].QueryStore(p, w)
}

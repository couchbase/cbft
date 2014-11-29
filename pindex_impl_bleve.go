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

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"
)

func init() {
	RegisterPIndexImplType("bleve", &PIndexImplType{
		New:   NewBlevePIndexImpl,
		Open:  OpenBlevePIndexImpl,
		Count: CountBlevePIndexImpl,
		Query: QueryBlevePIndexImpl,

		Description: "bleve - full-text index powered by the bleve full-text-search engine",
		StartSample: bleve.NewIndexMapping(),
	})
}

func NewBlevePIndexImpl(indexType, indexParams, path string, restart func()) (
	PIndexImpl, Dest, error) {
	bindexMapping := bleve.NewIndexMapping()
	if len(indexParams) > 0 {
		if err := json.Unmarshal([]byte(indexParams), &bindexMapping); err != nil {
			return nil, nil, fmt.Errorf("error: parse bleve index mapping: %v", err)
		}
	}

	bindex, err := bleve.New(path, bindexMapping)
	if err != nil {
		return nil, nil, fmt.Errorf("error: new bleve index, path: %s, err: %s",
			path, err)
	}

	return bindex, NewBleveDest(path, bindex, restart), err
}

func OpenBlevePIndexImpl(indexType, path string, restart func()) (PIndexImpl, Dest, error) {
	// TODO: boltdb sometimes locks on Open(), so need to investigate,
	// where perhaps there was a previous missing or race-y Close().
	bindex, err := bleve.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return bindex, NewBleveDest(path, bindex, restart), err
}

func CountBlevePIndexImpl(mgr *Manager, indexName, indexUUID string) (uint64, error) {
	alias, err := bleveIndexAlias(mgr, indexName, indexUUID, nil)
	if err != nil {
		return 0, fmt.Errorf("CountBlevePIndexImpl indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	return alias.DocCount()
}

type BleveQueryParams struct {
	Query       *bleve.SearchRequest `json:"query"`
	Consistency *ConsistencyParams   `json:"consistency"`
}

func QueryBlevePIndexImpl(mgr *Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	var bleveQueryParams BleveQueryParams
	err := json.Unmarshal(req, &bleveQueryParams)
	if err != nil {
		return fmt.Errorf("QueryBlevePIndexImpl parsing bleveQueryParams,"+
			" req: %s, err: %v", req, err)
	}

	alias, err := bleveIndexAlias(mgr, indexName, indexUUID,
		bleveQueryParams.Consistency)
	if err != nil {
		return fmt.Errorf("QueryBlevePIndexImpl indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
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

const BLEVE_DEST_INITIAL_BUF_SIZE_BYTES = 20000
const BLEVE_DEST_APPLY_BUF_SIZE_BYTES = 200000

type BleveDest struct {
	path    string
	restart func() // Invoked when caller should restart this BleveDest, like on rollback.

	m          sync.Mutex // Protects the fields that follow.
	bindex     bleve.Index
	partitions map[string]*BleveDestPartition
}

// Used to track state for a single partition.
type BleveDestPartition struct {
	partition       string
	partitionOpaque string // Key used to implement SetOpaque/GetOpaque().

	m          sync.Mutex   // Protects the fields that follow.
	seqMax     uint64       // Max seq # we've seen for this partition.
	seqMaxBuf  []byte       // For binary encoded seqMax uint64.
	seqSnapEnd uint64       // To track snapshot end seq # for this partition.
	buf        []byte       // The batch points to slices from buf, which we reuse.
	batch      *bleve.Batch // Batch is applied when too big or when we hit seqSnapEnd.

	lastOpaque []byte // Cache most recent value for SetOpaque()/GetOpaque().
}

func NewBleveDest(path string, bindex bleve.Index, restart func()) Dest {
	return &BleveDest{
		path:       path,
		restart:    restart,
		bindex:     bindex,
		partitions: make(map[string]*BleveDestPartition),
	}
}

func (t *BleveDest) getPartition(partition string) (
	*BleveDestPartition, bleve.Index, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.bindex == nil {
		return nil, nil, fmt.Errorf("BleveDest already closed")
	}

	bdp, exists := t.partitions[partition]
	if !exists || bdp == nil {
		bdp = &BleveDestPartition{
			partition:       partition,
			partitionOpaque: "o:" + partition,
			seqMaxBuf:       make([]byte, 8), // For binary encoded seqMax uint64.
			batch:           bleve.NewBatch(),
		}
		t.partitions[partition] = bdp
	}

	return bdp, t.bindex, nil
}

func (t *BleveDest) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	log.Printf("bleve dest update, partition: %s, key: %s, seq: %d",
		partition, key, seq)

	bdp, bindex, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.OnDataUpdate(bindex, key, seq, val)
}

func (t *BleveDest) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	log.Printf("bleve dest delete, partition: %s, key: %s, seq: %d",
		partition, key, seq)

	bdp, bindex, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.OnDataDelete(bindex, key, seq)
}

func (t *BleveDest) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	log.Printf("bleve dest snapshot-start, partition: %s, snapStart: %d, snapEnd: %d",
		partition, snapStart, snapEnd)

	bdp, bindex, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.OnSnapshotStart(bindex, snapStart, snapEnd)
}

func (t *BleveDest) SetOpaque(partition string, value []byte) error {
	log.Printf("bleve dest set-opaque, partition: %s, value: %s",
		partition, value)

	bdp, bindex, err := t.getPartition(partition)
	if err != nil {
		return err
	}

	return bdp.SetOpaque(bindex, value)
}

func (t *BleveDest) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	log.Printf("bleve dest get-opaque, partition: %s", partition)

	bdp, bindex, err := t.getPartition(partition)
	if err != nil {
		return nil, 0, err
	}

	return bdp.GetOpaque(bindex)
}

func (t *BleveDest) Rollback(partition string, rollbackSeq uint64) error {
	log.Printf("bleve dest rollback, partition: %s, rollbackSeq: %d",
		partition, rollbackSeq)

	// NOTE: A rollback of any partition means a rollback of all
	// partitions, since they all share a single bleve.Index backend.
	// That's why we grab and keep BleveDest.m locked.
	t.m.Lock()
	defer t.m.Unlock()

	if t.bindex == nil {
		return fmt.Errorf("BleveDest already closed, can't rollback")
	}

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
	//
	t.partitions = make(map[string]*BleveDestPartition)

	// Use t.bindex == nil to check any late calls to BleveDest.
	t.bindex.Close()
	t.bindex = nil

	os.RemoveAll(t.path)

	t.restart()

	return nil
}

func (t *BleveDest) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh chan struct{}) error {
	// TODO.
	return nil
}

// ---------------------------------------------------------

func (t *BleveDestPartition) OnDataUpdate(bindex bleve.Index,
	key []byte, seq uint64, val []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	bufVal := t.appendToBufUnlocked(val)

	t.batch.Index(string(key), bufVal) // TODO: string(key) makes garbage?

	return t.updateSeqUnlocked(bindex, seq)
}

func (t *BleveDestPartition) OnDataDelete(bindex bleve.Index,
	key []byte, seq uint64) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.batch.Delete(string(key)) // TODO: string(key) makes garbage?

	return t.updateSeqUnlocked(bindex, seq)
}

func (t *BleveDestPartition) OnSnapshotStart(bindex bleve.Index,
	snapStart, snapEnd uint64) error {
	t.m.Lock()
	defer t.m.Unlock()

	err := t.applyBatchUnlocked(bindex)
	if err != nil {
		return err
	}

	t.seqSnapEnd = snapEnd

	return nil
}

func (t *BleveDestPartition) SetOpaque(bindex bleve.Index, value []byte) error {
	t.m.Lock()
	defer t.m.Unlock()

	t.lastOpaque = append(t.lastOpaque[0:0], value...)

	t.batch.SetInternal([]byte(t.partitionOpaque), t.lastOpaque)

	return nil
}

func (t *BleveDestPartition) GetOpaque(bindex bleve.Index) ([]byte, uint64, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.lastOpaque == nil {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		value, err := bindex.GetInternal([]byte(t.partitionOpaque))
		if err != nil {
			return nil, 0, err
		}
		t.lastOpaque = append([]byte(nil), value...) // Note: copies value.
	}

	if t.seqMax <= 0 {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		buf, err := bindex.GetInternal([]byte(t.partition))
		if err != nil {
			return nil, 0, err
		}
		if len(buf) <= 0 {
			return t.lastOpaque, 0, nil // No seqMax buf is a valid case.
		}
		if len(buf) != 8 {
			return nil, 0, fmt.Errorf("unexpected size for seqMax bytes")
		}
		t.seqMax = binary.BigEndian.Uint64(buf[0:8])
		binary.BigEndian.PutUint64(t.seqMaxBuf, t.seqMax)
	}

	return t.lastOpaque, t.seqMax, nil
}

// ---------------------------------------------------------

func (t *BleveDestPartition) updateSeqUnlocked(bindex bleve.Index,
	seq uint64) error {
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

	return t.applyBatchUnlocked(bindex)
}

func (t *BleveDestPartition) applyBatchUnlocked(bindex bleve.Index) error {
	if err := bindex.Batch(t.batch); err != nil {
		return err
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
func bleveIndexAlias(mgr *Manager, indexName, indexUUID string,
	consistencyParams *ConsistencyParams) (bleve.IndexAlias, error) {
	localPIndexes, remotePlanPIndexes, err :=
		mgr.CoveringPIndexes(indexName, indexUUID, PlanPIndexNodeCanRead)
	if err != nil {
		return nil, fmt.Errorf("bleveIndexAlias, err: %v", err)
	}

	alias := bleve.NewIndexAlias()

	var wg sync.WaitGroup
	var cancelCh chan struct{}

	for _, localPIndex := range localPIndexes {
		bindex, ok := localPIndex.Impl.(bleve.Index)
		if ok && bindex != nil && localPIndex.IndexType == "bleve" {
			alias.Add(bindex)

			if localPIndex.Dest != nil &&
				consistencyParams != nil &&
				consistencyParams.Level != "" {
				consistencyVector := consistencyParams.Vectors[indexName]
				if consistencyVector != nil {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for _, partition := range localPIndex.sourcePartitionsArr {
							consistencySeq := consistencyVector[partition]
							if consistencySeq > 0 {
								localPIndex.Dest.ConsistencyWait(partition,
									consistencyParams.Level,
									consistencySeq,
									cancelCh)
							}
						}
					}()
				}
			}
		} else {
			return nil, fmt.Errorf("bleveIndexAlias localPIndex wasn't bleve")
		}
	}

	for _, remotePlanPIndex := range remotePlanPIndexes {
		baseURL := "http://" + remotePlanPIndex.NodeDef.HostPort +
			"/api/pindex/" + remotePlanPIndex.PlanPIndex.Name
		// TODO: Propagate auth to bleve client.
		alias.Add(&BleveClient{
			SearchURL:         baseURL + "/search",
			DocCountURL:       baseURL + "/count",
			ConsistencyParams: consistencyParams,
		})
	}

	// TODO: Should kickoff remote queries concurrently before we wait.
	wg.Wait()

	return alias, nil
}

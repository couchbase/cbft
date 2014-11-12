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
	"os"
	"sync"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"
)

func init() {
	RegisterPIndexImplType("bleve", &PIndexImplType{
		New:  NewBlevePIndexImpl,
		Open: OpenBlevePIndexImpl,
	})
}

func NewBlevePIndexImpl(indexType, indexSchema, path string, restart func()) (
	PIndexImpl, Dest, error) {
	bindexMapping := bleve.NewIndexMapping()
	if len(indexSchema) > 0 {
		if err := json.Unmarshal([]byte(indexSchema), &bindexMapping); err != nil {
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
	bindex, err := bleve.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return bindex, NewBleveDest(path, bindex, restart), err
}

// ---------------------------------------------------------

const BLEVE_DEST_INITIAL_BUF_SIZE_BYTES = 2000000
const BLEVE_DEST_APPLY_BUF_SIZE_BYTES = 1500000

type BleveDest struct {
	path    string
	restart func() // Invoked when caller should restart this BleveDest, like on rollback.

	m      sync.Mutex
	bindex bleve.Index
	seqs   map[string]uint64 // To track max seq #'s we saw per partition.

	// TODO: Maybe should have a buf & batch per partition?
	buf      []byte            // The batch points to slices from buf.
	batch    *bleve.Batch      // Batch applied when too large or hit snapshot end.
	snapEnds map[string]uint64 // To track snapshot end seq #'s per partition.
}

func NewBleveDest(path string, bindex bleve.Index, restart func()) Dest {
	return &BleveDest{
		path:     path,
		restart:  restart,
		bindex:   bindex,
		seqs:     map[string]uint64{},
		snapEnds: map[string]uint64{},
	}
}

func (t *BleveDest) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	log.Printf("bleve dest update, partition: %s, key: %s, seq: %d",
		partition, key, seq)

	t.m.Lock()
	defer t.m.Unlock()

	bufVal := t.appendToBufUnlocked(val)
	if t.batch == nil {
		t.batch = bleve.NewBatch()
	}
	t.batch.Index(string(key), bufVal) // TODO: The string(key) makes garbage?

	err := t.maybeApplyBatchUnlocked(partition, seq)
	if err != nil {
		return err
	}

	return t.updateSeqUnlocked(partition, seq)
}

func (t *BleveDest) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	log.Printf("bleve dest delete, partition: %s, key: %s, seq: %d",
		partition, key, seq)

	t.m.Lock()
	defer t.m.Unlock()

	if t.batch == nil {
		t.batch = bleve.NewBatch()
	}
	t.batch.Delete(string(key)) // TODO: The string(key) makes garbage?

	err := t.maybeApplyBatchUnlocked(partition, seq)
	if err != nil {
		return err
	}

	return t.updateSeqUnlocked(partition, seq)
}

func (t *BleveDest) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	log.Printf("bleve dest snapshot-start, partition: %s, snapStart: %d, snapEnd: %d",
		partition, snapStart, snapEnd)

	t.m.Lock()
	defer t.m.Unlock()

	err := t.applyBatchUnlocked()
	if err != nil {
		return err
	}

	t.snapEnds[partition] = snapEnd

	return nil
}

var opaquePrefix = []byte("o:")

func (t *BleveDest) SetOpaque(partition string,
	value []byte) error {
	log.Printf("bleve dest set-opaque, partition: %s, value: %s",
		partition, value)

	t.m.Lock()
	defer t.m.Unlock()

	// TODO: The o:key makes garbage, so perhaps use lookup table?
	return t.bindex.SetInternal([]byte("o:"+partition), value)
}

func (t *BleveDest) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	log.Printf("bleve dest get-opaque, partition: %s", partition)

	t.m.Lock()
	defer t.m.Unlock()

	// TODO: The o:key makes garbage, so perhaps use lookup table?
	value, err = t.bindex.GetInternal([]byte("o:" + partition))
	if err != nil || value == nil {
		return nil, 0, err
	}

	// TODO: Need way to control memory alloc during GetInternal(),
	// perhaps with optional memory allocator func() parameter?
	buf, err := t.bindex.GetInternal([]byte(partition))
	if err != nil || buf == nil {
		return value, 0, err
	}
	if len(buf) < 8 {
		return nil, 0, err
	}

	return value, binary.BigEndian.Uint64(buf[0:8]), nil
}

func (t *BleveDest) Rollback(partition string, rollbackSeq uint64) error {
	log.Printf("bleve dest get-opaque, partition: %s, rollbackSeq: %d",
		partition, rollbackSeq)

	t.m.Lock()
	defer t.m.Unlock()

	// TODO: Implement partial rollback one day.  Implementation
	// sketch: we expect bleve to one day to provide an additional
	// Snapshot() and Rollback() API, where Snapshot() returns some
	// opaque and persistable snapshot ID ("SID"), which cbft can
	// occasionally record into the bleve's Get/SetInternal() "side"
	// storage.  A stream rollback operation then needs to loop
	// through appropriate candidate SID's until a Rollback(SID)
	// succeeds.  Else, we eventually devolve down to
	// restarting/rebuilding everything from scratch or zero.
	//
	// For now, always rollback to zero, in which we close the
	// pindex and have the janitor rebuild from scratch.
	t.seqs = map[string]uint64{}
	t.buf = nil
	t.batch = nil
	t.snapEnds = map[string]uint64{}

	t.bindex.Close()

	os.RemoveAll(t.path)

	t.restart()

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) maybeApplyBatchUnlocked(partition string, seq uint64) error {
	if t.batch == nil || len(t.batch.IndexOps) <= 0 {
		return nil
	}

	if len(t.buf) < BLEVE_DEST_APPLY_BUF_SIZE_BYTES &&
		seq < t.snapEnds[partition] {
		return nil
	}

	return t.applyBatchUnlocked()
}

func (t *BleveDest) applyBatchUnlocked() error {
	if t.batch != nil {
		if err := t.bindex.Batch(t.batch); err != nil {
			return err
		}

		// TODO: would good to reuse batch; we could just clear its
		// public maps (IndexOPs & InternalOps), but sounds brittle.
		t.batch = nil
	}

	if t.buf != nil {
		t.buf = t.buf[0:0] // Reset t.buf via re-slice.
	}

	for partition, _ := range t.snapEnds {
		delete(t.snapEnds, partition)
	}

	return nil
}

var eightBytes = make([]byte, 8)

func (t *BleveDest) updateSeqUnlocked(partition string, seq uint64) error {
	if t.seqs[partition] < seq {
		t.seqs[partition] = seq

		// TODO: use re-slicing if there's capacity to get the eight bytes.
		bufSeq := t.appendToBufUnlocked(eightBytes)

		binary.BigEndian.PutUint64(bufSeq, seq)

		// TODO: Only the last SetInternal() matters for a partition,
		// so we can reuse the bufSeq memory rather than wasting eight
		// bytes in t.buf on every mutation.
		//
		// NOTE: No copy of partition to buf as it's immutatable string bytes.
		return t.bindex.SetInternal([]byte(partition), bufSeq)
	}

	return nil
}

// Appends b to end of t.buf, and returns that suffix slice of t.buf
// that has the appended copy of the input b.
func (t *BleveDest) appendToBufUnlocked(b []byte) []byte {
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

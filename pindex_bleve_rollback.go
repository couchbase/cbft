//  Copyright (c) 2016 Couchbase, Inc.
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
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/upsidedown"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/moss"

	log "github.com/couchbase/clog"
)

type LowerLevelStoreHolder interface {
	LowerLevelStore() store.KVStore
}

type MossStoreActualHolder interface {
	Actual() *moss.Store
}

func (t *BleveDest) Rollback(partition string, vBucketUUID uint64, rollbackSeq uint64) error {
	t.AddError("dest rollback", partition, nil, rollbackSeq, nil, nil)

	// NOTE: A rollback of any partition means a rollback of all the
	// partitions in the bindex, so lock the entire BleveDest.
	t.m.Lock()
	defer t.m.Unlock()

	wasClosed, wasPartial, err := t.partialRollbackLOCKED(partition,
		vBucketUUID, rollbackSeq)

	log.Printf("pindex_bleve_rollback: path: %s,"+
		" wasClosed: %t, wasPartial: %t, err: %v",
		t.path, wasClosed, wasPartial, err)

	if !wasClosed {
		t.closeLOCKED()
	}

	if !wasPartial {
		os.RemoveAll(t.path) // Full rollback to zero.
	}

	// Whether partial or full rollback, restart the BleveDest so that
	// feeds are restarted.
	t.restart()

	return nil
}

// Attempt partial rollback.  Implementation sketch: walk through
// previous mossStore snapshots until we reach to a point at or before
// the wanted rollbackSeq and vBucketUUID.
// If found, revert to that prevous snapshot.
func (t *BleveDest) partialRollbackLOCKED(partition string,
	vBucketUUID uint64, rollbackSeq uint64) (bool, bool, error) {
	if t.bindex == nil {
		return false, false, nil
	}
	_, kvstore, err := t.bindex.Advanced()
	if err != nil {
		return false, false, err
	}

	llsh, ok := kvstore.(LowerLevelStoreHolder)
	if !ok {
		return false, false, fmt.Errorf("kvstore not a llsh, kvstore: %#v", kvstore)
	}

	lls := llsh.LowerLevelStore()
	if lls == nil {
		return false, false, fmt.Errorf("lls nil")
	}

	msah, ok := lls.(MossStoreActualHolder)
	if !ok {
		return false, false, fmt.Errorf("llsh not a msah, llsh: %#v", llsh)
	}

	store := msah.Actual()
	if store == nil {
		return false, false, nil // No moss store, so no partial rollback.
	}

	store.AddRef()
	defer store.Close()

	// TODO: Handle non-upsidedown bleve index types some day.
	seqMaxKey := upsidedown.NewInternalRow([]byte(partition), nil).Key()

	// get vBucketMap/Opaque key
	var vBucketMapKey []byte
	if t.partitions[partition] != nil {
		po := t.partitions[partition].partitionOpaque
		vBucketMapKey = upsidedown.NewInternalRow(po, nil).Key()
	}

	totSnapshotsExamined := 0
	defer func() {
		log.Printf("pindex_bleve_rollback: path: %s, totSnapshotsExamined: %d",
			t.path, totSnapshotsExamined)
	}()

	var ss, ssPrev moss.Snapshot

	ss, err = store.Snapshot()
	for err == nil && ss != nil {
		totSnapshotsExamined++

		var tryRevert bool
		tryRevert, err = snapshotAtOrBeforeSeq(t.path, ss, seqMaxKey,
			vBucketMapKey, rollbackSeq, vBucketUUID)
		if err != nil {
			ss.Close()
			return false, false, err
		}

		if tryRevert {
			log.Printf("pindex_bleve_rollback: trying revert, path: %s", t.path)

			// Close the bleve index, but keep our ref-counts on the
			// underlying store and snapshot until after the revert.
			t.closeLOCKED()

			err = store.SnapshotRevert(ss)

			ss.Close()

			return true, err == nil, err
		}

		ssPrev, err = store.SnapshotPrevious(ss)
		ss.Close()
		ss = ssPrev
	}

	return false, false, err
}

// snapshotAtOrBeforeSeq returns true if the snapshot represents a seq
// number at or before the given seq number with a matching vBucket UUID.
func snapshotAtOrBeforeSeq(path string, ss moss.Snapshot,
	seqMaxKey []byte, vBucketMapKey []byte,
	seqMaxWant uint64, vBucketUUIDWant uint64) (bool, error) {
	// Equivalent of bleve.Index.GetInternal(seqMaxKey).
	v, err := ss.Get(seqMaxKey, moss.ReadOptions{})
	if err != nil {
		return false, err
	}
	if v == nil {
		return false, nil
	}
	if len(v) != 8 {
		return false, fmt.Errorf("wrong len seqMaxKey: %s, v: %s", seqMaxKey, v)
	}
	seqMaxCurr := binary.BigEndian.Uint64(v[0:8])

	// when no vBucketUUIDWant is given from Rollback
	// then fallback to seqMaxCurr checks
	if vBucketUUIDWant == 0 {
		log.Printf("pindex_bleve_rollback: examining snapshot, path: %s,"+
			" seqMaxKey: %s, seqMaxCurr: %d, seqMaxWant: %d",
			path, seqMaxKey, seqMaxCurr, seqMaxWant)
		return seqMaxCurr <= seqMaxWant, nil
	}
	// get the vBucketUUID
	vbMap, err := ss.Get(vBucketMapKey, moss.ReadOptions{})
	if err != nil {
		log.Printf("pindex_bleve_rollback: snapshot Get failed,"+
			" for vBucketMapKey: %s, err: %v", vBucketMapKey, err)
		return false, err
	}
	if vbMap == nil {
		log.Printf("pindex_bleve_rollback: No vBucketMap for vBucketMapKey: %s",
			vBucketMapKey)
		return false, nil
	}
	vBucketUUIDCur, err := strconv.ParseUint(cbgt.ParseOpaqueToUUID(vbMap), 10, 64)
	if err != nil {
		log.Printf("pindex_bleve_rollback: ParseOpaqueToUUID failed for "+
			"vbMap: %s, err: %s", vbMap, err)
		return false, err
	}

	log.Printf("pindex_bleve_rollback: examining snapshot, path: %s,"+
		" seqMaxKey: %s, seqMaxCurr: %d, seqMaxWant: %d"+
		" vBucketMapKey: %s, vBucketUUIDCur: %d, vBucketUUIDWant: %d",
		path, seqMaxKey, seqMaxCurr, seqMaxWant, vBucketMapKey,
		vBucketUUIDCur, vBucketUUIDWant)

	return (seqMaxCurr <= seqMaxWant && vBucketUUIDCur == vBucketUUIDWant), nil
}

func (t *BleveDestPartition) Rollback(partition string,
	rollbackSeq uint64) error {
	// placeholder implementation
	return t.bdest.Rollback(partition, 0, rollbackSeq)
}

func (t *BleveDestPartition) RollbackEx(partition string,
	vBucketUUID uint64, rollbackSeq uint64) error {
	return t.bdest.Rollback(partition, vBucketUUID, rollbackSeq)
}

//  Copyright (c) 2018 Couchbase, Inc.
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
	"strconv"

	"github.com/blevesearch/bleve/v2/index/upsidedown"
	store "github.com/blevesearch/upsidedown_store_api"

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

// UpsideDown/Moss implementation sketch: walk through previous mossStore
// snapshots until we reach to a point at or before the wanted rollbackSeq
// and vBucketUUID. If found, revert to that prevous snapshot.
func (t *BleveDest) partialMossRollbackLOCKED(kvstore store.KVStore,
	partition string, vBucketUUID, rollbackSeq uint64) (bool, bool, error) {
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

	seqMaxKey := upsidedown.NewInternalRow([]byte(partition), nil).Key()

	partitions, _ := t.partitions.Load().(map[string]*BleveDestPartition)
	// get vBucketMap/Opaque key
	var vBucketMapKey []byte
	if partitions[partition] != nil {
		po := partitions[partition].partitionOpaque
		vBucketMapKey = upsidedown.NewInternalRow(po, nil).Key()
	}

	totSnapshotsExamined := 0
	defer func() {
		log.Printf("pindex_bleve_moss_rollback: path: %s, totSnapshotsExamined: %d",
			t.path, totSnapshotsExamined)
	}()

	var ss, ssPrev moss.Snapshot
	var err error

	ss, err = store.Snapshot()
	for err == nil && ss != nil {
		totSnapshotsExamined++

		var tryRevert bool
		tryRevert, err = mossSnapshotAtOrBeforeSeq(t.path, ss, seqMaxKey,
			vBucketMapKey, rollbackSeq, vBucketUUID)
		if err != nil {
			ss.Close()
			return false, false, err
		}

		if tryRevert {
			log.Printf("pindex_bleve_moss_rollback: trying revert, path: %s", t.path)

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

// mossSnapshotAtOrBeforeSeq returns true if the snapshot represents a seq
// number at or before the given seq number with a matching vBucket UUID.
func mossSnapshotAtOrBeforeSeq(path string, ss moss.Snapshot,
	seqMaxKey, vBucketMapKey []byte,
	seqMaxWant, vBucketUUIDWant uint64) (bool, error) {
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
		log.Printf("pindex_bleve_moss_rollback: examining snapshot, path: %s,"+
			" seqMaxKey: %s, seqMaxCurr: %d, seqMaxWant: %d",
			path, seqMaxKey, seqMaxCurr, seqMaxWant)
		return seqMaxCurr <= seqMaxWant, nil
	}
	// get the vBucketUUID
	vbMap, err := ss.Get(vBucketMapKey, moss.ReadOptions{})
	if err != nil {
		log.Printf("pindex_bleve_moss_rollback: snapshot Get failed,"+
			" for vBucketMapKey: %s, err: %v", vBucketMapKey, err)
		return false, err
	}
	if vbMap == nil {
		log.Printf("pindex_bleve_moss_rollback: No vBucketMap for vBucketMapKey: %s",
			vBucketMapKey)
		return false, nil
	}
	vBucketUUIDCurr, err := strconv.ParseUint(cbgt.ParseOpaqueToUUID(vbMap), 10, 64)
	if err != nil {
		log.Printf("pindex_bleve_moss_rollback: ParseOpaqueToUUID failed for "+
			"vbMap: %s, err: %s", vbMap, err)
		return false, err
	}

	log.Printf("pindex_bleve_moss_rollback: examining snapshot, path: %s,"+
		" seqMaxKey: %s, seqMaxCurr: %d, seqMaxWant: %d"+
		" vBucketMapKey: %s, vBucketUUIDCurr: %d, vBucketUUIDWant: %d",
		path, seqMaxKey, seqMaxCurr, seqMaxWant, vBucketMapKey,
		vBucketUUIDCurr, vBucketUUIDWant)

	return (seqMaxCurr <= seqMaxWant && vBucketUUIDCurr == vBucketUUIDWant), nil
}

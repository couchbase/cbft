//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/blevesearch/bleve/v2/index/scorch"

	"github.com/couchbase/cbgt"

	log "github.com/couchbase/clog"
)

// Scorch implementation sketch: fetch all available rollback points.
// Determine which point is of relevance to get to at or before the
// wanted rollbackSeq and vBucketUUID. If found, rollback to that
// particular point.
func (t *BleveDest) partialScorchRollbackLOCKED(sh *scorch.Scorch,
	partition string, vBucketUUID, rollbackSeq uint64) (bool, bool, error) {
	seqMaxKey := []byte(partition)

	// get vBucketMap/Opaque key
	var vBucketMapKey []byte
	if t.partitions[partition] != nil {
		vBucketMapKey = t.partitions[partition].partitionOpaque
	}

	totSnapshotsExamined := 0
	defer func() {
		log.Printf("pindex_bleve_scorch_rollback: path: %s, totSnapshotExamined: %d",
			t.path, totSnapshotsExamined)
	}()

	// close the scorch index as rollback works in offline.
	err := t.closeLOCKED()
	if err != nil {
		return false, false, err
	}

	idxPath := t.path + string(os.PathSeparator) + "store"
	rollbackPoints, err := scorch.RollbackPoints(idxPath)
	if err != nil {
		return true, false, err
	}

	for _, rollbackPoint := range rollbackPoints {
		totSnapshotsExamined++

		var tryRevert bool
		tryRevert, err = scorchSnapshotAtOrBeforeSeq(t.path, rollbackPoint, seqMaxKey,
			vBucketMapKey, rollbackSeq, vBucketUUID)
		if err != nil {
			return true, false, err
		}

		if tryRevert {
			log.Printf("pindex_bleve_scorch_rollback: trying revert, path: %s", t.path)

			if ServerlessMode {
				v := rollbackPoint.GetInternal([]byte("TotBytesWritten"))
				if v == nil {
					// what's the right thing to do here?
					return false, err == nil, err
				}

				if t.bindex != nil {
					RollbackRefund(t.bindex.Name(), t.sourceName, binary.LittleEndian.Uint64(v))
				}
			}
			err = scorch.Rollback(idxPath, rollbackPoint)
			if err != nil {
				log.Printf("pindex_bleve_scorch_rollback: Rollback failed, err: %v", err)
			}

			return true, err == nil, err
		}
	}

	return true, false, err
}

// scorchSnapshotAtOrBeforeSeq returns true if the snapshot represents a seq
// number at or before the given seq number with a matching vBucket UUID.
func scorchSnapshotAtOrBeforeSeq(path string, rbp *scorch.RollbackPoint,
	seqMaxKey, vBucketMapKey []byte,
	seqMaxWant, vBucketUUIDWant uint64) (bool, error) {

	v := rbp.GetInternal(seqMaxKey)
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
		log.Printf("pindex_bleve_scorch_rollback: examining snapshot, path: %s,"+
			" seqMaxKey: %s, seqMaxCurr: %d, seqMaxWant: %d",
			path, seqMaxKey, seqMaxCurr, seqMaxWant)
		return seqMaxCurr <= seqMaxWant, nil
	}
	// get the vBucketUUID
	vbMap := rbp.GetInternal(vBucketMapKey)
	if vbMap == nil {
		log.Printf("pindex_bleve_scorch_rollback: No vBucketMap for vBucketMapKey: %s",
			vBucketMapKey)
		return false, nil
	}
	vBucketUUIDCurr, err := strconv.ParseUint(cbgt.ParseOpaqueToUUID(vbMap), 10, 64)
	if err != nil {
		log.Printf("pindex_bleve_scorch_rollback: ParseOpaqueToUUID failed for "+
			"vbMap: %s, err: %s", vbMap, err)
		return false, err
	}

	log.Printf("pindex_bleve_scorch_rollback: examining snapshot, path: %s,"+
		" seqMaxKey: %s, seqMaxCurr: %d, seqMaxWant: %d"+
		" vBucketMapKey: %s, vBucketUUIDCurr: %d, vBucketUUIDWant: %d",
		path, seqMaxKey, seqMaxCurr, seqMaxWant, vBucketMapKey,
		vBucketUUIDCurr, vBucketUUIDWant)

	return (seqMaxCurr <= seqMaxWant && vBucketUUIDCurr == vBucketUUIDWant), nil
}

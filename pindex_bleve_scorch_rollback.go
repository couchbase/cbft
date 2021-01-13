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

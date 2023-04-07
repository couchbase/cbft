//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown"

	log "github.com/couchbase/clog"
)

func (t *BleveDest) Rollback(partition string, vBucketUUID uint64, rollbackSeq uint64) error {
	t.AddError("dest rollback", partition, nil, rollbackSeq, nil, nil)

	// NOTE: A rollback of any partition means a rollback of all the
	// partitions in the bindex, so lock the entire BleveDest.
	t.m.Lock()
	defer t.m.Unlock()

	// The BleveDest may be closed due to another partition(BleveDestPartition) of
	// the same pindex being rolled back earlier.
	if t.bindex != nil {
		wasClosed, wasPartial, err := t.partialRollbackLOCKED(partition,
			vBucketUUID, rollbackSeq)

		log.Printf("pindex_bleve_rollback: path: %s,"+
			" wasClosed: %t, wasPartial: %t, err: %v",
			t.path, wasClosed, wasPartial, err)

		if !wasClosed {
			t.closeLOCKED()
		}

		if !wasPartial {
			atomic.AddUint64(&TotRollbackFull, 1)
			os.RemoveAll(t.path) // Full rollback to zero.
		} else {
			atomic.AddUint64(&TotRollbackPartial, 1)
		}

		// Whether partial or full rollback, restart the BleveDest so that
		// feeds are restarted.
		t.rollback()
	}

	return nil
}

// Attempt partial rollback.
func (t *BleveDest) partialRollbackLOCKED(partition string,
	vBucketUUID uint64, rollbackSeq uint64) (bool, bool, error) {
	if t.bindex == nil {
		return false, false, nil
	}

	index, err := t.bindex.Advanced()
	if err != nil {
		return false, false, err
	}

	if udc, ok := index.(*upsidedown.UpsideDownCouch); ok {
		kvstore, err := udc.Advanced()
		if err != nil {
			return false, false, err
		}
		return t.partialMossRollbackLOCKED(kvstore,
			partition, vBucketUUID, rollbackSeq)
	}

	if sh, ok := index.(*scorch.Scorch); ok {
		return t.partialScorchRollbackLOCKED(sh,
			partition, vBucketUUID, rollbackSeq)
	}

	return false, false, fmt.Errorf("unknown index type")
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

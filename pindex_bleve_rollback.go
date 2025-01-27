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
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/index/scorch"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

func init() {
	cbgt.RollbackHook = func(pindex *cbgt.PIndex, phase cbgt.RollbackPhase) (bool, error) {
		df, ok := pindex.Dest.(*cbgt.DestForwarder)
		if !ok || df == nil {
			return false, fmt.Errorf("rollbackhook: invalid dest for pindex:%s",
				pindex.Name)
		}
		bd, ok := df.DestProvider.(*BleveDest)
		if !ok || bd == nil {
			return false, fmt.Errorf("rollbackhook: invalid destProvider for pindex:%s",
				pindex.Name)
		}

		bd.m.Lock()
		defer bd.m.Unlock()

		switch phase {
		case cbgt.RollbackInit:
			pindexName := bd.bindex.Name()
			wasClosed, wasPartial, err := bd.partialRollbackLOCKED()

			log.Printf("pindex_bleve_rollback: path: %s, wasClosed: %t, "+
				"wasPartial: %t, err: %v", bd.path, wasClosed, wasPartial, err)

			if !wasClosed {
				bd.closeLOCKED(false)
			}

			if !wasPartial {
				atomic.AddUint64(&TotRollbackFull, 1)
				if ServerlessMode {
					// this is a full rollback, so the paritition is going to be
					// rebuilt a fresh. The reason we are refunding over here is
					// because this is not a end-user problem, but rather a
					// couchbase cluster problem. So, once the partition is built
					// afresh, we would essentially any loss of cost by charging
					// for 0 - original high seq no. and after that we will
					// actually start costing the user.
					RollbackRefund(pindexName, bd.sourceName, 0)
				}
			} else {
				atomic.AddUint64(&TotRollbackPartial, 1)
			}

			return wasPartial, nil
		case cbgt.RollbackCompleted:
			bd.isRollbackInProgress = false
		}

		return false, nil
	}
}

func (t *BleveDest) Rollback(partition string, vBucketUUID uint64, rollbackSeq uint64) error {
	t.AddError("dest rollback", partition, nil, rollbackSeq, nil, nil)

	// NOTE: A rollback of any partition means a rollback of all the
	// partitions in the bindex, so lock the entire BleveDest.
	t.m.Lock()
	defer t.m.Unlock()

	// The BleveDest may be closed due to another partition(BleveDestPartition) of
	// the same pindex being rolled back earlier.
	if t.bindex == nil || t.isRollbackInProgress {
		return nil
	}

	t.isRollbackInProgress = true

	t.rollbackInfo = &RollbackInfo{
		partition:   partition,
		vBucketUUID: vBucketUUID,
		rollbackSeq: rollbackSeq,
	}

	// Whether partial or full rollback, restart the BleveDest so that
	// feeds are restarted.
	t.rollback()

	return nil
}

// Attempt partial rollback.
func (t *BleveDest) partialRollbackLOCKED() (bool, bool, error) {
	if t.bindex == nil {
		return false, false, nil
	}

	index, err := t.bindex.Advanced()
	if err != nil {
		return false, false, err
	}

	if sh, ok := index.(*scorch.Scorch); ok {
		return t.partialScorchRollbackLOCKED(sh)
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

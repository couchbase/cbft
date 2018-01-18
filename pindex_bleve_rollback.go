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
	"fmt"
	"os"

	"github.com/blevesearch/bleve/index/scorch"
	"github.com/blevesearch/bleve/index/upsidedown"

	log "github.com/couchbase/clog"
)

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

// Attempt partial rollback.
func (t *BleveDest) partialRollbackLOCKED(partition string,
	vBucketUUID uint64, rollbackSeq uint64) (bool, bool, error) {
	if t.bindex == nil {
		return false, false, nil
	}

	index, kvstore, err := t.bindex.Advanced()
	if err != nil {
		return false, false, err
	}

	if _, ok := index.(*upsidedown.UpsideDownCouch); ok {
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

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
	"fmt"
	"io"
)

type Dest interface {
	// Invoked by PIndex.Close().
	Close() error

	// Invoked when there's a new mutation from a data source for a
	// partition.  Dest implementation is responsible for making its
	// own copies of the key and val data.
	OnDataUpdate(partition string, key []byte, seq uint64, val []byte) error

	// Invoked by the data source when there's a data deletion in a
	// partition.  Dest implementation is responsible for making its
	// own copies of the key data.
	OnDataDelete(partition string, key []byte, seq uint64) error

	// An callback invoked by the data source when there's a start of
	// a new snapshot for a partition.  The Receiver implementation,
	// for example, might choose to optimize persistence perhaps by
	// preparing a batch write to application-specific storage.
	OnSnapshotStart(partition string, snapStart, snapEnd uint64) error

	// The Dest implementation should persist the value parameter of
	// SetOpaque() for retrieval during some future call to
	// GetOpaque() by the system.  The metadata value should be
	// considered "in-stream", or as part of the sequence history of
	// mutations.  That is, a later Rollback() to some previous
	// sequence number for a particular partition should rollback
	// both persisted metadata and regular data.  The Dest
	// implementation should make its own copy of the value data.
	SetOpaque(partition string, value []byte) error

	// GetOpaque() should return the opaque value previously
	// provided by an earlier call to SetOpaque().  If there was no
	// previous call to SetOpaque(), such as in the case of a brand
	// new instance of a Dest (as opposed to a restarted or reloaded
	// Dest), the Dest should return (nil, 0, nil) for (value,
	// lastSeq, err), respectively.  The lastSeq should be the last
	// sequence number received and persisted during calls to the
	// Dest's OnDataUpdate() & OnDataDelete() methods.
	GetOpaque(partition string) (value []byte, lastSeq uint64, err error)

	// Invoked by when the datasource signals a rollback during dest
	// initialization.  Note that both regular data and opaque data
	// should be rolled back to at a maximum of the rollbackSeq.  Of
	// note, the Dest is allowed to rollback even further, even all
	// the way back to the start or to zero.
	Rollback(partition string, rollbackSeq uint64) error

	// Blocks until the Dest has reached the desired consistency for
	// the partition or until the cancelCh is written to (with a
	// reason string) or closed by some goroutine related to the
	// calling goroutine.  The seqStart is the seq number when the
	// operation started waiting and the seqEnd is the seq number at
	// the end of operation (even when cancelled or error), so that
	// the caller can get a rough idea of ingest velocity.
	ConsistencyWait(partition string,
		consistencyLevel string,
		consistencySeq uint64,
		cancelCh chan string) error

	// Counts the underlying pindex implementation.
	Count(pindex *PIndex, cancelCh chan string) (uint64, error)

	// Queries the underlying pindex implementation, blocking if
	// needed for the Dest to reach the desired consistency.
	Query(pindex *PIndex, req []byte, w io.Writer,
		cancelCh chan string) error

	Stats(io.Writer) error
}

type DestStats struct {
	TotError           uint64
	TotOnDataUpdate    uint64
	TotOnDataDelete    uint64
	TotOnSnapshotStart uint64
	TotSetOpaque       uint64
	TotGetOpaque       uint64
	TotRollback        uint64

	TimeOnDataUpdate    uint64
	TimeOnDataDelete    uint64
	TimeOnSnapshotStart uint64
	TimeSetOpaque       uint64
	TimeGetOpaque       uint64
	TimeRollback        uint64
}

type DestPartitionFunc func(partition string, key []byte,
	dests map[string]Dest) (Dest, error)

// This basic partition func first tries a direct lookup by partition
// string, else it tries the "" partition.
func BasicPartitionFunc(partition string, key []byte,
	dests map[string]Dest) (Dest, error) {
	dest, exists := dests[partition]
	if exists {
		return dest, nil
	}
	dest, exists = dests[""]
	if exists {
		return dest, nil
	}
	return nil, fmt.Errorf("error: no dest for key: %s,"+
		" partition: %s, dests: %#v", key, partition, dests)
}

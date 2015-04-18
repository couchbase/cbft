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
	"sync/atomic"

	"github.com/rcrowley/go-metrics"
)

type Dest interface {
	// Invoked by PIndex.Close().
	Close() error

	// Invoked when there's a new mutation from a data source for a
	// partition.  Dest implementation is responsible for making its
	// own copies of the key and val data.
	DataUpdate(partition string, key []byte, seq uint64, val []byte) error

	// Invoked by the data source when there's a data deletion in a
	// partition.  Dest implementation is responsible for making its
	// own copies of the key data.
	DataDelete(partition string, key []byte, seq uint64) error

	// An callback invoked by the data source when there's a start of
	// a new snapshot for a partition.  The Receiver implementation,
	// for example, might choose to optimize persistence perhaps by
	// preparing a batch write to application-specific storage.
	SnapshotStart(partition string, snapStart, snapEnd uint64) error

	// OpaqueGet() should return the opaque value previously
	// provided by an earlier call to OpaqueSet().  If there was no
	// previous call to OpaqueSet(), such as in the case of a brand
	// new instance of a Dest (as opposed to a restarted or reloaded
	// Dest), the Dest should return (nil, 0, nil) for (value,
	// lastSeq, err), respectively.  The lastSeq should be the last
	// sequence number received and persisted during calls to the
	// Dest's DataUpdate() & DataDelete() methods.
	OpaqueGet(partition string) (value []byte, lastSeq uint64, err error)

	// The Dest implementation should persist the value parameter of
	// OpaqueSet() for retrieval during some future call to
	// OpaqueGet() by the system.  The metadata value should be
	// considered "in-stream", or as part of the sequence history of
	// mutations.  That is, a later Rollback() to some previous
	// sequence number for a particular partition should rollback
	// both persisted metadata and regular data.  The Dest
	// implementation should make its own copy of the value data.
	OpaqueSet(partition string, value []byte) error

	// Invoked by when the datasource signals a rollback during dest
	// initialization.  Note that both regular data and opaque data
	// should be rolled back to at a maximum of the rollbackSeq.  Of
	// note, the Dest is allowed to rollback even further, even all
	// the way back to the start or to zero.
	Rollback(partition string, rollbackSeq uint64) error

	// Blocks until the Dest has reached the desired consistency for
	// the partition or until the cancelCh is readable or closed by
	// some goroutine related to the calling goroutine.  The error
	// response might be a ErrorConsistencyWait instance, which has
	// StartEndSeqs information.  The seqStart is the seq number when
	// the operation started waiting and the seqEnd is the seq number
	// at the end of operation (even when cancelled or error), so that
	// the caller might get a rough idea of ingest velocity.
	ConsistencyWait(partition, partitionUUID string,
		consistencyLevel string,
		consistencySeq uint64,
		cancelCh <-chan bool) error

	// Counts the underlying pindex implementation.
	Count(pindex *PIndex, cancelCh <-chan bool) (uint64, error)

	// Queries the underlying pindex implementation, blocking if
	// needed for the Dest to reach the desired consistency.
	Query(pindex *PIndex, req []byte, w io.Writer,
		cancelCh <-chan bool) error

	Stats(io.Writer) error
}

type DestStats struct {
	TotError uint64

	TimerDataUpdate    metrics.Timer
	TimerDataDelete    metrics.Timer
	TimerSnapshotStart metrics.Timer
	TimerOpaqueGet     metrics.Timer
	TimerOpaqueSet     metrics.Timer
	TimerRollback      metrics.Timer
}

func NewDestStats() *DestStats {
	return &DestStats{
		TimerDataUpdate:    metrics.NewTimer(),
		TimerDataDelete:    metrics.NewTimer(),
		TimerSnapshotStart: metrics.NewTimer(),
		TimerOpaqueGet:     metrics.NewTimer(),
		TimerOpaqueSet:     metrics.NewTimer(),
		TimerRollback:      metrics.NewTimer(),
	}
}

func (d *DestStats) WriteJSON(w io.Writer) {
	t := atomic.LoadUint64(&d.TotError)
	fmt.Fprintf(w, `{"TotError":%d`, t)

	w.Write([]byte(`,"TimerDataUpdate":`))
	WriteTimerJSON(w, d.TimerDataUpdate)
	w.Write([]byte(`,"TimerDataDelete":`))
	WriteTimerJSON(w, d.TimerDataDelete)
	w.Write([]byte(`,"TimerSnapshotStart":`))
	WriteTimerJSON(w, d.TimerSnapshotStart)
	w.Write([]byte(`,"TimerOpaqueGet":`))
	WriteTimerJSON(w, d.TimerOpaqueGet)
	w.Write([]byte(`,"TimerOpaqueSet":`))
	WriteTimerJSON(w, d.TimerOpaqueSet)
	w.Write([]byte(`,"TimerRollback":`))
	WriteTimerJSON(w, d.TimerRollback)

	w.Write(jsonCloseBrace)
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
	return nil, fmt.Errorf("dest: no dest for key: %s,"+
		" partition: %s, dests: %#v", key, partition, dests)
}

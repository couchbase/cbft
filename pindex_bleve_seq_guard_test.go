//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//  Unit tests for the MB-70770 per-partition monotonic seq guard, which
//  skips non-OSO ops with seq <= seqMax before they reach the batch.

package cbft

import (
	"sync/atomic"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
)

// newSeqGuardTestPartition builds the minimal BleveDestPartition the DCP
// data paths need. No batch workers are started; the tests keep every op
// below seqSnapEnd/BleveMaxOpsPerBatch so nothing is ever submitted.
func newSeqGuardTestPartition(t *testing.T) *BleveDestPartition {
	t.Helper()

	bindex, err := bleve.NewMemOnly(bleve.NewIndexMapping())
	if err != nil {
		t.Fatalf("bleve.NewMemOnly: %v", err)
	}
	t.Cleanup(func() { bindex.Close() })

	return &BleveDestPartition{
		bdest: &BleveDest{
			bleveDocConfig: BleveDocumentConfig{
				Mode:      "type_field",
				TypeField: "type",
			},
		},
		bindex:     bindex,
		partition:  "0",
		batch:      bindex.NewBatch(),
		seqSnapEnd: 1000,
	}
}

func aggregateStat(t *testing.T, name string) uint64 {
	t.Helper()
	v, ok := AggregateBleveDestPartitionStats()[name]
	if !ok {
		t.Fatalf("AggregateBleveDestPartitionStats missing %q", name)
	}
	return v.(uint64)
}

var seqGuardTestDoc = []byte(`{"type":"t","name":"x"}`)

// TestSeqGuardSkipsStaleDataOps: ops at or below seqMax are skipped (not
// batched, seq bookkeeping untouched) and counted; in-order ops are not.
func TestSeqGuardSkipsStaleDataOps(t *testing.T) {
	bdp := newSeqGuardTestPartition(t)

	skippedBefore := aggregateStat(t, "TotDataStaleOpsSkipped")

	// In-order ops are accepted.
	if err := bdp.DataUpdate("0", []byte("k1"), 5, seqGuardTestDoc,
		0, cbgt.DEST_EXTRAS_TYPE_NIL, nil); err != nil {
		t.Fatalf("DataUpdate(seq 5): %v", err)
	}
	if got := atomic.LoadUint64(&bdp.seqMax); got != 5 {
		t.Fatalf("seqMax = %d, want 5", got)
	}
	if got := bdp.batch.Size(); got != 1 {
		t.Fatalf("batch size = %d, want 1", got)
	}

	// A stale (lower-seq) update is skipped: batch untouched, seqMax kept.
	if err := bdp.DataUpdate("0", []byte("k2"), 3, seqGuardTestDoc,
		0, cbgt.DEST_EXTRAS_TYPE_NIL, nil); err != nil {
		t.Fatalf("DataUpdate(stale seq 3): %v", err)
	}
	// An equal-seq delete is skipped too.
	if err := bdp.DataDelete("0", []byte("k1"), 5,
		0, cbgt.DEST_EXTRAS_TYPE_NIL, nil); err != nil {
		t.Fatalf("DataDelete(stale seq 5): %v", err)
	}
	if got := bdp.batch.Size(); got != 1 {
		t.Errorf("batch size after stale ops = %d, want 1 (untouched)", got)
	}
	if got := atomic.LoadUint64(&bdp.seqMax); got != 5 {
		t.Errorf("seqMax after stale ops = %d, want 5", got)
	}
	if got := atomic.LoadUint64(&bdp.staleOpsSkipped); got != 2 {
		t.Errorf("partition staleOpsSkipped = %d, want 2", got)
	}
	if got := aggregateStat(t, "TotDataStaleOpsSkipped") - skippedBefore; got != 2 {
		t.Errorf("TotDataStaleOpsSkipped delta = %d, want 2", got)
	}

	// The next in-order op is unaffected. Use a different doc: bleve
	// batches are doc-ID-keyed, so deleting k1 would overwrite its
	// pending update in place.
	if err := bdp.DataDelete("0", []byte("k9"), 6,
		0, cbgt.DEST_EXTRAS_TYPE_NIL, nil); err != nil {
		t.Fatalf("DataDelete(seq 6): %v", err)
	}
	if got := atomic.LoadUint64(&bdp.seqMax); got != 6 {
		t.Errorf("seqMax = %d, want 6", got)
	}
	if got := bdp.batch.Size(); got != 2 {
		t.Errorf("batch size = %d, want 2 (update + delete)", got)
	}
}

// TestSeqGuardInitialSeqMaxZero: with seqMax == 0 (fresh partition) the
// guard must not fire, whatever the seq.
func TestSeqGuardInitialSeqMaxZero(t *testing.T) {
	bdp := newSeqGuardTestPartition(t)

	skippedBefore := aggregateStat(t, "TotDataStaleOpsSkipped")

	if err := bdp.DataUpdate("0", []byte("k1"), 1, seqGuardTestDoc,
		0, cbgt.DEST_EXTRAS_TYPE_NIL, nil); err != nil {
		t.Fatalf("DataUpdate(seq 1): %v", err)
	}
	if got := bdp.batch.Size(); got != 1 {
		t.Errorf("batch size = %d, want 1", got)
	}
	if got := aggregateStat(t, "TotDataStaleOpsSkipped") - skippedBefore; got != 0 {
		t.Errorf("TotDataStaleOpsSkipped delta = %d, want 0", got)
	}
}

// TestSeqGuardOSOBypass: OSO seqs arrive out of order by design, so the
// guard must be bypassed entirely.
func TestSeqGuardOSOBypass(t *testing.T) {
	bdp := newSeqGuardTestPartition(t)

	// Baseline: seqMax 5, as if loaded/streamed earlier.
	atomic.StoreUint64(&bdp.seqMax, 5)

	// Enter OSO mode as OSOSnapshot(start) does, minus the batch submit.
	bdp.m.Lock()
	bdp.osoSnapshot = true
	bdp.osoSeqMax = atomic.LoadUint64(&bdp.seqMax)
	bdp.m.Unlock()

	skippedBefore := aggregateStat(t, "TotDataStaleOpsSkipped")

	// Out-of-order seqs below seqMax must all be applied.
	for _, seq := range []uint64{7, 3, 8, 2} {
		if err := bdp.DataUpdate("0", []byte("k"+string(rune('a'+seq))), seq,
			seqGuardTestDoc, 0, cbgt.DEST_EXTRAS_TYPE_NIL, nil); err != nil {
			t.Fatalf("DataUpdate(OSO seq %d): %v", seq, err)
		}
	}
	if got := bdp.batch.Size(); got != 4 {
		t.Errorf("batch size = %d, want 4 (guard bypassed in OSO)", got)
	}
	if got := aggregateStat(t, "TotDataStaleOpsSkipped") - skippedBefore; got != 0 {
		t.Errorf("TotDataStaleOpsSkipped delta = %d, want 0", got)
	}
	bdp.m.Lock()
	osoSeqMax := bdp.osoSeqMax
	bdp.m.Unlock()
	if osoSeqMax != 8 {
		t.Errorf("osoSeqMax = %d, want 8", osoSeqMax)
	}
	// seqMax itself only advances at OSO end (existing behavior).
	if got := atomic.LoadUint64(&bdp.seqMax); got != 5 {
		t.Errorf("seqMax during OSO = %d, want 5", got)
	}
}

// TestSeqGuardIgnoresStaleSnapshotStart: a marker whose snapEnd is at or
// below seqMax must be ignored so it cannot drag seqSnapEnd backwards.
func TestSeqGuardIgnoresStaleSnapshotStart(t *testing.T) {
	bdp := newSeqGuardTestPartition(t)

	atomic.StoreUint64(&bdp.seqMax, 10)
	bdp.m.Lock()
	bdp.seqSnapEnd = 15
	bdp.m.Unlock()

	markersBefore := aggregateStat(t, "TotSnapshotStaleMarkersSkipped")

	if err := bdp.SnapshotStart("0", 1, 10); err != nil { // snapEnd 10 <= seqMax 10
		t.Fatalf("SnapshotStart(stale): %v", err)
	}

	bdp.m.Lock()
	seqSnapEnd := bdp.seqSnapEnd
	bdp.m.Unlock()
	if seqSnapEnd != 15 {
		t.Errorf("seqSnapEnd = %d, want 15 (stale marker ignored)", seqSnapEnd)
	}
	if got := aggregateStat(t, "TotSnapshotStaleMarkersSkipped") - markersBefore; got != 1 {
		t.Errorf("TotSnapshotStaleMarkersSkipped delta = %d, want 1", got)
	}
}

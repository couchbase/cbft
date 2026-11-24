//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build enterprise
// +build enterprise

package cbft

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
)

// returns a BleveDest backed by a real bleve index. Batch workers are not
// started, so nothing else takes the dest's lock during the test.
func newKeysInUseTestDest(t *testing.T) (*BleveDest, bleve.Index) {
	t.Helper()

	bindex, err := bleve.New(t.TempDir()+"/idx", bleve.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	if err = bindex.Index("doc1", map[string]string{"f": "hello"}); err != nil {
		t.Fatal(err)
	}

	bdest := &BleveDest{
		bindex:     bindex,
		partitions: make(map[string]*BleveDestPartition),
		stats:      cbgt.NewPIndexStoreStats(),
		copyStats:  &CopyPartitionStats{},
		stopCh:     make(chan struct{}),
		removeCh:   make(chan struct{}),
	}
	t.Cleanup(func() { _ = bdest.Close(false) })

	return bdest, bindex
}

// alias so the embedded field is not named Index, which would clash with the
// bleve.Index method of the same name
type wrappedBleveIndex = bleve.Index

// janitorAtReadIndex wraps a real bleve index. When asked for its file writer
// ids, it first plays the janitor and closes the dest, the same interleaving
// that crashed in MB-75560, but only if the dest's lock lets it. That keeps
// the test deterministic: either the lock is held for the read and the close
// cannot happen, or it is not and the close always happens.
type janitorAtReadIndex struct {
	wrappedBleveIndex
	bdest         *BleveDest
	closedMidRead bool
}

func (j *janitorAtReadIndex) FileWriterIDsInUse() (map[string]struct{}, error) {
	if j.bdest.m.TryLock() {
		// nothing stops BleveDest.Close from running right now
		j.closedMidRead = true
		_ = j.bdest.closeLOCKED(false)
		j.bdest.m.Unlock()
		return nil, nil
	}

	// with bleve v2.6.3 this panics if the index was just closed
	return j.wrappedBleveIndex.(bleve.IndexWithCallbacks).FileWriterIDsInUse()
}

func (j *janitorAtReadIndex) DropFileWriterIDs(ids map[string]struct{}) error {
	return j.wrappedBleveIndex.(bleve.IndexWithCallbacks).DropFileWriterIDs(ids)
}

// MB-75560: the keys in use refresh read the bleve index after the dest's
// lock was released, so the janitor could close the pindex mid-read (a
// rebalance moving it off the node) and the read panicked on the closed
// index. The dest must stay locked for the whole read.
func TestFileWriterIDsInUseBlocksConcurrentClose(t *testing.T) {
	bdest, bindex := newKeysInUseTestDest(t)

	racing := &janitorAtReadIndex{wrappedBleveIndex: bindex, bdest: bdest}
	bdest.m.Lock()
	bdest.bindex = racing
	bdest.m.Unlock()

	ids, err := bdest.fileWriterIDsInUse()

	if racing.closedMidRead {
		t.Fatal("dest was closed while its bleve index was being read; " +
			"the dest lock must be held for the whole read")
	}
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("expected file writer ids from the open index")
	}
}

// A dest the janitor has already closed must be skipped without touching the
// closed bleve index.
func TestFileWriterIDsInUseClosedDest(t *testing.T) {
	bdest, _ := newKeysInUseTestDest(t)

	if err := bdest.Close(false); err != nil {
		t.Fatal(err)
	}

	ids, err := bdest.fileWriterIDsInUse()
	if err != nil || len(ids) != 0 {
		t.Fatalf("expected no ids and no error for a closed dest, "+
			"got ids: %v, err: %v", ids, err)
	}
}

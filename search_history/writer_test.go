//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package search_history

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// syncToDisk is a test helper that forces a compaction sync to disk.
func (w *logWriter) syncToDisk() error {
	w.mu.RLock()
	records := w.readRecordsChronoLOCKED()
	gen := w.needsCompactGen
	w.mu.RUnlock()

	written, err := w.compactFile(records)

	w.mu.Lock()
	defer w.mu.Unlock()
	if err == nil {
		w.fileRecordCount = written
		w.unsyncedCount = 0
		if w.needsCompactGen == gen {
			w.needsCompact = false
		}
		w.lastSyncTime = time.Now()
	}
	return err
}

func TestWriterBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "searchhistory_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := newLogWriter(tmpDir, 200)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Append record
	record := &record{
		Timestamp: time.Now().UTC(),
		Index:     "test-index",
		Request:   `{"query":"test"}`,
		TookMs:    50,
		TotalHits: 10,
		Status:    "success",
	}

	if err := writer.append(record); err != nil {
		t.Fatalf("failed to append record: %v", err)
	}

	// Verify record stored
	if writer.ringSize != 1 {
		t.Errorf("expected ringSize=1, got %d", writer.ringSize)
	}

	// Manually sync to disk for testing
	err = writer.syncToDisk()
	if err != nil {
		t.Fatalf("failed to sync to disk: %v", err)
	}

	// Verify file created after sync
	filePath := filepath.Join(tmpDir, "search_history", "request_log.jsonl")
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("expected file to exist after sync")
	}
}

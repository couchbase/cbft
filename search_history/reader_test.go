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
	"testing"
	"time"
)

func TestReaderBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "searchhistory_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create writer and write test records
	writer, err := newLogWriter(tmpDir, 200)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	// Write test records with unique timestamps
	baseTime := time.Now().UTC()
	for i := 0; i < 3; i++ {
		record := &record{
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Index:     "test-index",
			Request:   `{"query":"test"}`,
			TookMs:    int64(50 + i),
			TotalHits: uint64(10 + i),
			Status:    "success",
		}
		if err := writer.append(record); err != nil {
			t.Fatalf("failed to append record: %v", err)
		}
	}

	// Manually sync to disk for testing
	err = writer.syncToDisk()
	if err != nil {
		t.Fatalf("failed to sync to disk: %v", err)
	}

	// Read records from disk (Service is nil, so it falls back to disk)
	results, total, err := read(tmpDir, 100, 0, "", 0, "")
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 records, got %d", len(results))
	}
	if total != 3 {
		t.Errorf("expected total 3, got %d", total)
	}

	// Test pagination
	results, total, err = read(tmpDir, 2, 0, "", 0, "")
	if err != nil {
		t.Fatalf("failed to read with limit: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 records with limit, got %d", len(results))
	}
	if total != 3 {
		t.Errorf("expected total 3, got %d", total)
	}

	// Test offset
	results, total, err = read(tmpDir, 2, 1, "", 0, "")
	if err != nil {
		t.Fatalf("failed to read with offset: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 records with offset, got %d", len(results))
	}
}

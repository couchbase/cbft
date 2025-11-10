//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package search_history

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	log "github.com/couchbase/clog"
)

// Manages search history with in-memory ring buffer and periodic disk sync.
type logWriter struct {
	// mu guards in-memory ring buffer state.
	// Uses RWMutex to allow many concurrent readers (getRecords API calls).
	mu              sync.RWMutex
	maxCapacity     int // can be changed via resizeCapacity
	ringBuffer      []*record
	ringHead        int
	ringSize        int
	unsyncedCount   int
	lastSyncTime    time.Time
	fileRecordCount int // approximate number of records in file (lines)
	needsCompact    bool
	needsCompactGen uint64 // increments whenever needsCompact is set (e.g., resize)

	// ioMu guards disk I/O resources.
	// Held during file open/close/write/flush operations to serialize disk I/O.
	// Kept separate from mu so slow disk operations don't block fast ring buffer reads.
	ioMu         sync.Mutex
	appendFile   *os.File
	appendWriter *bufio.Writer

	baseDir string
}

// Creates a new global log writer with in-memory ring buffer.
func newLogWriter(baseDir string, maxCapacity int) (*logWriter, error) {
	logDir := filepath.Join(baseDir, "search_history")
	if err := os.MkdirAll(logDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create search_history dir: %v", err)
	}

	w := &logWriter{
		baseDir:       logDir,
		maxCapacity:   maxCapacity,
		ringBuffer:    make([]*record, maxCapacity),
		ringHead:      0,
		ringSize:      0,
		unsyncedCount: 0,
		lastSyncTime:  time.Now(),
	}

	if err := w.loadFromDisk(); err != nil {
		log.Warnf("search_history: failed to load from disk: %v", err)
	}

	if err := w.openAppendFile(); err != nil {
		log.Warnf("search_history: failed to open append file: %v", err)
	}

	return w, nil
}

// openAppendFile opens the log file in append mode.
func (w *logWriter) openAppendFile() error {
	w.ioMu.Lock()
	defer w.ioMu.Unlock()
	return w.openAppendFileLocked()
}

// openAppendFileLocked opens the log file (assumes ioMu is already held).
func (w *logWriter) openAppendFileLocked() error {
	// Close existing handles if any.
	_ = w.closeAppendFileLocked()

	filePath := filepath.Join(w.baseDir, "request_log.jsonl")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to open append file: %v", err)
	}
	w.appendFile = f
	w.appendWriter = bufio.NewWriterSize(f, 256*1024)
	return nil
}

// Adds a record to the in-memory ring buffer and triggers sync if needed.
// Disk I/O is performed outside the lock using a snapshot.
func (w *logWriter) append(rec *record) error {
	// --- mutate ring + decide whether to sync (under lock)
	w.mu.Lock()

	w.ringBuffer[w.ringHead] = rec
	w.ringHead = (w.ringHead + 1) % w.maxCapacity
	if w.ringSize < w.maxCapacity {
		w.ringSize++
	}

	w.unsyncedCount++

	batch := syncBatchThreshold
	if w.maxCapacity < batch {
		batch = w.maxCapacity
	}
	needsSync := w.unsyncedCount >= batch ||
		(w.unsyncedCount > 0 && time.Since(w.lastSyncTime) >= time.Second)

	if !needsSync {
		w.mu.Unlock()
		return nil
	}

	needCompact := (w.fileRecordCount > 2*w.maxCapacity) || w.needsCompact
	compactGen := w.needsCompactGen

	// Snapshot records to write.
	var recordsToWrite []*record
	if needCompact {
		// MUST write chronological (oldest -> newest) to keep loadFromDisk() correct.
		recordsToWrite = w.readRecordsChronoLOCKED()
	} else {
		// Returns chronological (oldest -> newest) for just the unsynced tail.
		recordsToWrite = w.getUnsyncedRecordsLOCKED()
	}

	// Mark the unsynced work as "in flight" so concurrent setters won't be clobbered.
	// During I/O, append() is still single-threaded in practice (called by logWorker),
	// but this also prevents losing increments done by resizeCapacity() etc.
	pendingUnsynced := w.unsyncedCount
	w.unsyncedCount = 0

	w.mu.Unlock()

	// --- do disk I/O outside lock
	var (
		written int
		err     error
	)

	if needCompact {
		written, err = w.compactFile(recordsToWrite)
	} else {
		written, err = w.appendRecords(recordsToWrite)
	}

	// --- update counters (under lock)
	w.mu.Lock()
	defer w.mu.Unlock()

	if err != nil {
		// Restore pending unsynced work (plus anything accrued while we were unlocked).
		w.unsyncedCount += pendingUnsynced
		log.Warnf("search_history: sync failed: %v", err)
		return nil
	}

	if needCompact {
		// After compaction, file now contains only ring snapshot.
		w.fileRecordCount = written
		// Clear needsCompact only if no new generation appeared during I/O.
		if w.needsCompactGen == compactGen {
			w.needsCompact = false
		}
	} else {
		w.fileRecordCount += written
	}

	w.lastSyncTime = time.Now()
	return nil
}

// getUnsyncedRecordsLOCKED returns the unsynced tail in chronological order.
func (w *logWriter) getUnsyncedRecordsLOCKED() []*record {
	if w.unsyncedCount == 0 || w.ringSize == 0 {
		return nil
	}
	count := w.unsyncedCount
	if count > w.ringSize {
		count = w.ringSize
	}

	records := make([]*record, 0, count)
	for i := 0; i < count; i++ {
		idx := (w.ringHead - 1 - i + w.maxCapacity) % w.maxCapacity
		if w.ringBuffer[idx] != nil {
			records = append(records, w.ringBuffer[idx]) // newest -> oldest
		}
	}
	reverseRecords(records) // oldest -> newest
	return records
}

// getRecords acquires read lock and returns records (latest first, as before).
func (w *logWriter) getRecords() []*record {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.readRecordsLOCKED()
}

// readRecordsLOCKED returns records newest -> oldest (public behavior unchanged).
func (w *logWriter) readRecordsLOCKED() []*record {
	records := make([]*record, 0, w.ringSize)
	for i := 0; i < w.ringSize; i++ {
		idx := (w.ringHead - 1 - i + w.maxCapacity) % w.maxCapacity
		if w.ringBuffer[idx] != nil {
			records = append(records, w.ringBuffer[idx])
		}
	}
	return records
}

// readRecordsChronoLOCKED returns records oldest -> newest (for file writes).
func (w *logWriter) readRecordsChronoLOCKED() []*record {
	records := w.readRecordsLOCKED()
	reverseRecords(records)
	return records
}

func reverseRecords(rs []*record) {
	for i, j := 0, len(rs)-1; i < j; i, j = i+1, j-1 {
		rs[i], rs[j] = rs[j], rs[i]
	}
}

// resizeCapacity changes the ring capacity.
func (w *logWriter) resizeCapacity(newCapacity int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if newCapacity <= 0 {
		return fmt.Errorf("invalid capacity: %d", newCapacity)
	}
	if newCapacity == w.maxCapacity {
		return nil
	}

	current := w.readRecordsLOCKED() // newest -> oldest
	if len(current) > newCapacity {
		current = current[:newCapacity]
	}
	// Convert to chronological order before copying into new ring.
	reverseRecords(current) // now oldest -> newest

	newBuf := make([]*record, newCapacity)
	copy(newBuf, current)

	w.ringBuffer = newBuf
	w.maxCapacity = newCapacity
	w.ringSize = len(current)
	w.ringHead = w.ringSize % w.maxCapacity

	// Trigger compaction to sync disk file with new capacity.
	w.needsCompact = true
	w.needsCompactGen++
	w.unsyncedCount++ // ensures next append triggers sync
	return nil
}

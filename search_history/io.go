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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	log "github.com/couchbase/clog"
)

// loadFromDisk loads the most recent maxCapacity records from request_log.jsonl.
// It also initializes fileRecordCount to the total number of valid JSON lines read.
func (w *logWriter) loadFromDisk() error {
	filePath := filepath.Join(w.baseDir, "request_log.jsonl")

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer f.Close()

	// Keep only last maxCapacity records while scanning (bounded memory).
	buf := make([]*record, w.maxCapacity)
	start, count := 0, 0
	totalLines := 0

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		var rec record
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			continue
		}
		totalLines++

		if w.maxCapacity == 0 {
			continue
		}
		if count < w.maxCapacity {
			buf[(start+count)%w.maxCapacity] = &rec
			count++
		} else {
			// Overwrite oldest
			buf[start] = &rec
			start = (start + 1) % w.maxCapacity
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log file: %v", err)
	}

	// Rebuild ringBuffer in chronological order [0:count) = oldest..newest.
	if count > 0 {
		for i := 0; i < count; i++ {
			w.ringBuffer[i] = buf[(start+i)%w.maxCapacity]
		}
		w.ringSize = count
		w.ringHead = count % w.maxCapacity
	}

	w.fileRecordCount = totalLines
	return nil
}

// closeAppendFile closes the append file handle.
func (w *logWriter) closeAppendFile() error {
	w.ioMu.Lock()
	defer w.ioMu.Unlock()
	return w.closeAppendFileLocked()
}

func (w *logWriter) closeAppendFileLocked() error {
	if w.appendWriter != nil {
		if err := w.appendWriter.Flush(); err != nil {
			return err
		}
		w.appendWriter = nil
	}
	if w.appendFile != nil {
		err := w.appendFile.Close()
		w.appendFile = nil
		return err
	}
	return nil
}

// appendRecords appends records to the file (called without holding w.mu).
// Returns number of records successfully written.
func (w *logWriter) appendRecords(records []*record) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	w.ioMu.Lock()

	// Auto-recovery: try to open file if it's not open (e.g., after failed initialization).
	if w.appendWriter == nil {
		if err := w.openAppendFileLocked(); err != nil {
			w.ioMu.Unlock()
			return 0, fmt.Errorf("failed to open append file: %v", err)
		}
	}

	written := 0
	for _, r := range records {
		line, err := json.Marshal(r)
		if err != nil {
			w.ioMu.Unlock()
			return written, fmt.Errorf("failed to marshal record: %v", err)
		}
		line = append(line, '\n')
		if _, err := w.appendWriter.Write(line); err != nil {
			w.ioMu.Unlock()
			return written, fmt.Errorf("failed to write record: %v", err)
		}
		written++
	}

	if err := w.appendWriter.Flush(); err != nil {
		w.ioMu.Unlock()
		return written, fmt.Errorf("failed to flush: %v", err)
	}
	w.ioMu.Unlock()
	return written, nil
}

// compactFile rewrites the entire file with the provided records (chronological).
// Called without holding w.mu. Returns number of records written.
func (w *logWriter) compactFile(records []*record) (int, error) {
	filePath := filepath.Join(w.baseDir, "request_log.jsonl")
	tmpPath := filePath + ".tmp"

	// Close append file before compaction.
	_ = w.closeAppendFile()

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return 0, fmt.Errorf("failed to open temp file: %v", err)
	}

	shouldCleanup := true
	defer func() {
		if shouldCleanup {
			_ = f.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(f, 256*1024)

	written := 0
	for _, r := range records {
		line, err := json.Marshal(r)
		if err != nil {
			return written, fmt.Errorf("failed to marshal record: %v", err)
		}
		line = append(line, '\n')
		if _, err := bw.Write(line); err != nil {
			return written, fmt.Errorf("failed to write record: %v", err)
		}
		written++
	}

	if err := bw.Flush(); err != nil {
		return written, fmt.Errorf("failed to flush: %v", err)
	}
	if err := f.Close(); err != nil {
		return written, fmt.Errorf("failed to close temp file: %v", err)
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		return written, fmt.Errorf("failed to rename temp file: %v", err)
	}

	shouldCleanup = false

	// Reopen append file after compaction.
	if err := w.openAppendFile(); err != nil {
		log.Warnf("search_history: failed to reopen append file: %v", err)
	}
	return written, nil
}

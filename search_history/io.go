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
	"time"

	log "github.com/couchbase/clog"
)

// writerCallbackTTL bounds how long a cached writer callback is reused before
// being refreshed via WriterHook. The refresh picks up newer active keys after
// rotation; one hour balances key-rotation latency against avoiding repeated
// (CGO-heavy) callback derivation on the hot append path.
const writerCallbackTTL = time.Hour

// cachedWriter holds the encryption callback returned by WriterHook along
// with the keyId its ciphertext is produced under. The callback closes over
// its own nonce state, so a single cachedWriter must not be invoked from
// multiple goroutines concurrently.
type cachedWriter struct {
	keyId         string
	callback      func([]byte) []byte
	initTimestamp time.Time
}

// logFilePath returns the absolute path of the on-disk request log.
func (w *logWriter) logFilePath() string {
	return filepath.Join(w.baseDir, "request_log.jsonl")
}

// getOrRefreshWriter returns the current cached writer, refreshing via
// WriterHook on TTL expiry. When WriterHook is unset the slot caches a
// negative result so subsequent writes during the TTL window skip the check.
func (w *logWriter) getOrRefreshWriter() (cachedWriter, error) {
	if !w.writer.initTimestamp.IsZero() &&
		time.Since(w.writer.initTimestamp) <= writerCallbackTTL {
		return w.writer, nil
	}
	if WriterHook == nil {
		w.writer = cachedWriter{initTimestamp: time.Now()}
		return w.writer, nil
	}
	keyId, callback, err := WriterHook([]byte(w.logFilePath()))
	if err != nil {
		return cachedWriter{}, err
	}
	w.writer = cachedWriter{
		keyId:         keyId,
		callback:      callback,
		initTimestamp: time.Now(),
	}
	return w.writer, nil
}

// getOrLoadReader returns the cached decryption callback for keyId, fetching
// via ReaderHook on miss. Returns a nil callback (with no error) when
// ReaderHook is unset; the caller then surfaces the appropriate error.
func (w *logWriter) getOrLoadReader(keyId string) (func([]byte) ([]byte, error), error) {
	w.readersMu.Lock()
	callback, ok := w.readers[keyId]
	w.readersMu.Unlock()
	if ok {
		return callback, nil
	}
	if ReaderHook == nil {
		return nil, nil
	}
	cb, err := ReaderHook(keyId, []byte(w.logFilePath()))
	if err != nil {
		return nil, err
	}
	w.readersMu.Lock()
	w.readers[keyId] = cb
	w.readersMu.Unlock()
	return cb, nil
}

// invalidateReaderEntries removes cached reader callbacks for any keyId
// in keysToDrop. Called after a successful reencryptFile so the cache
// doesn't retain dead entries under dropped keys.
func (w *logWriter) invalidateReaderEntries(keysToDrop map[string]struct{}) {
	w.readersMu.Lock()
	for id := range keysToDrop {
		delete(w.readers, id)
	}
	w.readersMu.Unlock()
}

// encodeRecordLine returns the on-disk byte representation of a single
// record (without trailing newline). When the cached writer holds a non-empty
// key id, the record JSON is wrapped in an envelope. Otherwise the raw record
// JSON is returned so the format stays compatible with unencrypted deployments.
func (w *logWriter) encodeRecordLine(r *record) ([]byte, error) {
	plain, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal record: %v", err)
	}
	slot, err := w.getOrRefreshWriter()
	if err != nil {
		return nil, fmt.Errorf("failed to obtain encryption callback: %v", err)
	}
	if slot.callback == nil || slot.keyId == "" {
		return plain, nil
	}
	cipher := slot.callback(plain)
	line, err := json.Marshal(envelope{K: slot.keyId, D: cipher})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal envelope: %v", err)
	}
	return line, nil
}

// readerResolver fetches the decryption callback for a given keyId. The
// concrete implementation either consults a per-logWriter cache or calls
// ReaderHook directly; a nil callback signals that no decryption hook is
// available and the line cannot be read.
type readerResolver func(keyId string) (func([]byte) ([]byte, error), error)

// parseRecordLine implements the envelope-or-plaintext decode shared between
// the cached (method) and uncached (free-function) entry points. It is the
// single place that knows the on-disk line format.
func parseRecordLine(line []byte, resolve readerResolver) (*record, string, error) {
	var env envelope
	if err := json.Unmarshal(line, &env); err == nil && env.K != "" && len(env.D) > 0 {
		callback, err := resolve(env.K)
		if err != nil {
			return nil, env.K, fmt.Errorf("failed to obtain decryption callback: %v", err)
		}
		if callback == nil {
			return nil, env.K, fmt.Errorf("encrypted record but no decryption hook")
		}
		plain, err := callback(env.D)
		if err != nil {
			return nil, env.K, fmt.Errorf("failed to decrypt record: %v", err)
		}
		var rec record
		if err := json.Unmarshal(plain, &rec); err != nil {
			return nil, env.K, fmt.Errorf("failed to unmarshal record: %v", err)
		}
		return &rec, env.K, nil
	}

	var rec record
	if err := json.Unmarshal(line, &rec); err != nil {
		return nil, "", fmt.Errorf("failed to parse line: %v", err)
	}
	return &rec, "", nil
}

// decodeRecordLine parses a single on-disk line using the cached reader
// callbacks on the logWriter.
func (w *logWriter) decodeRecordLine(line []byte) (*record, string, error) {
	return parseRecordLine(line, w.getOrLoadReader)
}

// decodeRecordLine parses a single on-disk line by calling ReaderHook
// directly (no cache). Used by the cold-path readFile that runs without a
// live logWriter.
func decodeRecordLine(filePath string, line []byte) (*record, string, error) {
	return parseRecordLine(line, func(keyId string) (func([]byte) ([]byte, error), error) {
		if ReaderHook == nil {
			return nil, nil
		}
		return ReaderHook(keyId, []byte(filePath))
	})
}

// keyIDFromLine parses a single on-disk line and returns its key id without
// decrypting record payloads. Encrypted lines return envelope.k; plaintext
// lines return the empty key id.
func keyIDFromLine(line []byte) (string, error) {
	var env envelope
	if err := json.Unmarshal(line, &env); err == nil && env.K != "" {
		return env.K, nil
	}

	var rec record
	if err := json.Unmarshal(line, &rec); err == nil {
		return "", nil
	}

	return "", fmt.Errorf("failed to parse line")
}

// loadFromDisk loads the most recent maxCapacity records from request_log.jsonl.
// It also initializes fileRecordCount to the total number of valid lines read.
func (w *logWriter) loadFromDisk() error {
	filePath := w.logFilePath()

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
		rec, _, err := w.decodeRecordLine(scanner.Bytes())
		if err != nil {
			continue
		}
		totalLines++

		if w.maxCapacity == 0 {
			continue
		}
		if count < w.maxCapacity {
			buf[(start+count)%w.maxCapacity] = rec
			count++
		} else {
			// Overwrite oldest
			buf[start] = rec
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
		line, err := w.encodeRecordLine(r)
		if err != nil {
			w.ioMu.Unlock()
			return written, err
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
	filePath := w.logFilePath()
	// Use the "temp" suffix (no period) so the encryption manager's
	// processPath trims it back to the canonical context.
	tmpPath := filePath + "temp"

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
		line, err := w.encodeRecordLine(r)
		if err != nil {
			return written, err
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

// scanKeyIds reads the on-disk log and returns the set of distinct key ids
// found across all lines. The empty string is included for any plaintext
// (legacy) line. The append handle is briefly flushed under ioMu so the
// scan sees a consistent file view without blocking concurrent appends.
func (w *logWriter) scanKeyIds() (map[string]struct{}, error) {
	filePath := w.logFilePath()

	w.ioMu.Lock()
	if w.appendWriter != nil {
		_ = w.appendWriter.Flush()
	}
	w.ioMu.Unlock()

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}
	defer f.Close()

	ids := make(map[string]struct{})
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		keyId, err := keyIDFromLine(scanner.Bytes())
		if err != nil {
			continue
		}
		ids[keyId] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return ids, fmt.Errorf("error reading log file: %v", err)
	}
	return ids, nil
}

// reencryptFile rewrites the on-disk log so any line encrypted under a
// keyId in keysToDropMap is re-encrypted under the active key. Other
// lines pass through. On success, dropped keyIds are removed from the
// reader cache (done after ioMu is released, never nested with readersMu).
func (w *logWriter) reencryptFile(keysToDropMap map[string]struct{}) error {
	if WriterHook == nil || ReaderHook == nil {
		return nil
	}

	if err := w.rewriteForDroppedKeys(keysToDropMap); err != nil {
		return err
	}
	w.invalidateReaderEntries(keysToDropMap)
	return nil
}

// rewriteForDroppedKeys performs the on-disk re-encryption pass under
// ioMu. It does not touch the reader cache.
func (w *logWriter) rewriteForDroppedKeys(keysToDropMap map[string]struct{}) error {
	filePath := w.logFilePath()
	tmpPath := filePath + "temp"

	w.ioMu.Lock()
	defer w.ioMu.Unlock()

	// Drop the cached writer so the rewrite resolves a freshly-active key
	// (cbauth has already rotated the dropped ones out).
	w.writer = cachedWriter{}

	// Stop appending while we rewrite — we'll reopen at the end.
	_ = w.closeAppendFileLocked()

	src, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Nothing to re-encrypt; reopen append handle for normal writes.
			_ = w.openAppendFileLocked()
			return nil
		}
		_ = w.openAppendFileLocked()
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		_ = w.openAppendFileLocked()
		return fmt.Errorf("failed to open temp file: %v", err)
	}

	shouldCleanup := true
	defer func() {
		if shouldCleanup {
			_ = dst.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriterSize(dst, 256*1024)
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)

		rec, keyId, err := w.decodeRecordLine(line)
		if err != nil {
			// Drop unparseable lines rather than abort the rewrite.
			log.Warnf("search_history: dropping unreadable line during re-encrypt: %v", err)
			continue
		}

		_, drop := keysToDropMap[keyId]
		if drop {
			// Re-encrypt under whatever key is currently active.
			line, err = w.encodeRecordLine(rec)
			if err != nil {
				return fmt.Errorf("failed to re-encrypt record: %v", err)
			}
		}

		line = append(line, '\n')
		if _, err := bw.Write(line); err != nil {
			return fmt.Errorf("failed to write record: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log file: %v", err)
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %v", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %v", err)
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("failed to rename temp file: %v", err)
	}
	shouldCleanup = false

	if err := w.openAppendFileLocked(); err != nil {
		log.Warnf("search_history: failed to reopen append file: %v", err)
	}

	return nil
}

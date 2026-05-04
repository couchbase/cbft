//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package search_history

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func withEncryptionHooks(
	t *testing.T,
	writer func(path []byte) (string, func([]byte) []byte, error),
	reader func(keyId string, path []byte) (func([]byte) ([]byte, error), error),
) {
	t.Helper()

	prevWriter := WriterHook
	prevReader := ReaderHook
	WriterHook = writer
	ReaderHook = reader

	t.Cleanup(func() {
		WriterHook = prevWriter
		ReaderHook = prevReader
	})
}

func TestScanKeyIdsWithoutDecrypt(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "searchhistory_keys_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := newLogWriter(tmpDir, 10)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer w.closeAppendFile()

	filePath := filepath.Join(tmpDir, "search_history", "request_log.jsonl")

	encLine, err := json.Marshal(envelope{K: "old-key-id", D: []byte("ciphertext")})
	if err != nil {
		t.Fatalf("failed to marshal encrypted line: %v", err)
	}

	plainLine, err := json.Marshal(&record{
		Timestamp: time.Now().UTC(),
		Index:     "idx",
		Request:   `{"query":"x"}`,
		TookMs:    3,
		TotalHits: 1,
		Status:    "success",
	})
	if err != nil {
		t.Fatalf("failed to marshal plaintext line: %v", err)
	}

	payload := append(encLine, '\n')
	payload = append(payload, plainLine...)
	payload = append(payload, '\n')

	if err := os.WriteFile(filePath, payload, 0600); err != nil {
		t.Fatalf("failed to write log file: %v", err)
	}

	ids, err := w.scanKeyIds()
	if err != nil {
		t.Fatalf("scanKeyIds failed: %v", err)
	}

	if _, ok := ids["old-key-id"]; !ok {
		t.Fatalf("expected old-key-id in use, got: %+v", ids)
	}
	if _, ok := ids[""]; !ok {
		t.Fatalf("expected empty key id for plaintext line, got: %+v", ids)
	}
}

func TestReadFileWithEncryptedLines(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "searchhistory_read_enc_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	activeKey := "k1"
	withEncryptionHooks(t,
		func(path []byte) (string, func([]byte) []byte, error) {
			cb := func(data []byte) []byte {
				return append([]byte("enc|"), data...)
			}
			return activeKey, cb, nil
		},
		func(keyId string, path []byte) (func([]byte) ([]byte, error), error) {
			cb := func(data []byte) ([]byte, error) {
				if keyId != "k1" {
					return nil, fmt.Errorf("unexpected key id: %s", keyId)
				}
				if len(data) < 4 || string(data[:4]) != "enc|" {
					return nil, fmt.Errorf("bad ciphertext")
				}
				return data[4:], nil
			}
			return cb, nil
		},
	)

	w, err := newLogWriter(tmpDir, 10)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer w.closeAppendFile()

	rec := &record{
		Timestamp: time.Now().UTC(),
		Index:     "idx",
		Request:   `{"query":"x"}`,
		TookMs:    4,
		TotalHits: 7,
		Status:    "success",
	}
	if err := w.append(rec); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.syncToDisk(); err != nil {
		t.Fatalf("syncToDisk failed: %v", err)
	}

	prevService := Service
	Service = nil
	defer func() { Service = prevService }()

	results, total, err := read(tmpDir, 10, 0, "", 0, "")
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if total != 1 || len(results) != 1 {
		t.Fatalf("expected one encrypted record, got total=%d len=%d", total, len(results))
	}
	if results[0].Index != rec.Index || results[0].Request != rec.Request {
		t.Fatalf("unexpected record read back: %+v", results[0])
	}
}

func TestReencryptFileByDroppedKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "searchhistory_drop_key_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	activeKey := "old-key-id"
	withEncryptionHooks(t,
		func(path []byte) (string, func([]byte) []byte, error) {
			key := activeKey
			cb := func(data []byte) []byte {
				return append([]byte("enc:"+key+"|"), data...)
			}
			return key, cb, nil
		},
		func(keyId string, path []byte) (func([]byte) ([]byte, error), error) {
			cb := func(data []byte) ([]byte, error) {
				prefix := "enc:" + keyId + "|"
				if len(data) < len(prefix) || string(data[:len(prefix)]) != prefix {
					return nil, fmt.Errorf("bad ciphertext for key %s", keyId)
				}
				return data[len(prefix):], nil
			}
			return cb, nil
		},
	)

	w, err := newLogWriter(tmpDir, 20)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer w.closeAppendFile()

	for i := 0; i < 2; i++ {
		if err := w.append(&record{
			Timestamp: time.Now().UTC().Add(time.Duration(i) * time.Second),
			Index:     "idx",
			Request:   fmt.Sprintf(`{"query":"q%d"}`, i),
			TookMs:    int64(10 + i),
			TotalHits: uint64(100 + i),
			Status:    "success",
		}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	if err := w.syncToDisk(); err != nil {
		t.Fatalf("syncToDisk failed: %v", err)
	}

	activeKey = "new-key-id"
	if err := w.reencryptFile(map[string]struct{}{"old-key-id": {}}); err != nil {
		t.Fatalf("reencryptFile failed: %v", err)
	}

	ids, err := w.scanKeyIds()
	if err != nil {
		t.Fatalf("scanKeyIds failed: %v", err)
	}
	if _, ok := ids["new-key-id"]; !ok {
		t.Fatalf("expected new key id in use, got: %+v", ids)
	}
	if _, ok := ids["old-key-id"]; ok {
		t.Fatalf("old key id should be re-encrypted away, got: %+v", ids)
	}

	// Successful re-encrypt must drop the cached reader for the purged key.
	w.readersMu.Lock()
	_, retained := w.readers["old-key-id"]
	w.readersMu.Unlock()
	if retained {
		t.Fatalf("reader cache retained callback for dropped key 'old-key-id'")
	}
}

// Writer slot is refreshed via WriterHook only after the TTL elapses.
func TestWriterSlotTTLRefresh(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "searchhistory_ttl_test_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	var hookCalls int
	activeKey := "k1"
	withEncryptionHooks(t,
		func(path []byte) (string, func([]byte) []byte, error) {
			hookCalls++
			key := activeKey
			cb := func(data []byte) []byte {
				return append([]byte("enc:"+key+"|"), data...)
			}
			return key, cb, nil
		},
		func(keyId string, path []byte) (func([]byte) ([]byte, error), error) {
			cb := func(data []byte) ([]byte, error) {
				prefix := "enc:" + keyId + "|"
				if len(data) < len(prefix) || string(data[:len(prefix)]) != prefix {
					return nil, fmt.Errorf("bad ciphertext for key %s", keyId)
				}
				return data[len(prefix):], nil
			}
			return cb, nil
		},
	)

	w, err := newLogWriter(tmpDir, 20)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer w.closeAppendFile()

	if err := w.append(&record{
		Timestamp: time.Now().UTC(),
		Index:     "idx",
		Request:   `{"query":"first"}`,
		Status:    "success",
	}); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.syncToDisk(); err != nil {
		t.Fatalf("syncToDisk failed: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("expected WriterHook called once after first write, got %d", hookCalls)
	}

	// Second write within TTL: hook must not be called again.
	if err := w.append(&record{
		Timestamp: time.Now().UTC(),
		Index:     "idx",
		Request:   `{"query":"second"}`,
		Status:    "success",
	}); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.syncToDisk(); err != nil {
		t.Fatalf("syncToDisk failed: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("expected WriterHook still at 1 within TTL, got %d", hookCalls)
	}

	// Force expiry and rotate the active key; next write should refresh.
	w.ioMu.Lock()
	w.writer.initTimestamp = time.Now().Add(-2 * writerCallbackTTL)
	w.ioMu.Unlock()
	activeKey = "k2"

	if err := w.append(&record{
		Timestamp: time.Now().UTC(),
		Index:     "idx",
		Request:   `{"query":"third"}`,
		Status:    "success",
	}); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.syncToDisk(); err != nil {
		t.Fatalf("syncToDisk failed: %v", err)
	}
	if hookCalls != 2 {
		t.Fatalf("expected WriterHook re-called after TTL expiry, got %d", hookCalls)
	}

	// syncToDisk forces a full-file compaction, so all on-disk records end up
	// under whichever key is active at sync time (k2 here). The point of the
	// test is the hookCalls counter, not the on-disk key distribution.
	ids, err := w.scanKeyIds()
	if err != nil {
		t.Fatalf("scanKeyIds failed: %v", err)
	}
	if _, ok := ids["k2"]; !ok {
		t.Fatalf("expected k2 in use after rotation, got: %+v", ids)
	}
}

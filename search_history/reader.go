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
)

func read(dataDir string, limit, offset int, indexName string, minTookMs int64, status string) ([]*record, int, error) {
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	if Service != nil && Service.writer != nil {
		memRecords := Service.writer.getRecords()
		var filtered []*record
		for _, record := range memRecords {
			if matchesFilters(record, indexName, minTookMs, status) {
				filtered = append(filtered, record)
			}
		}

		total := len(filtered)
		if offset >= len(filtered) {
			return []*record{}, total, nil
		}
		end := offset + limit
		if end > len(filtered) {
			end = len(filtered)
		}
		return filtered[offset:end], total, nil
	}

	filePath := filepath.Join(dataDir, "search_history", "request_log.jsonl")
	diskRecords, err := readFile(filePath, indexName, minTookMs, status)
	if err != nil && !os.IsNotExist(err) {
		return nil, 0, fmt.Errorf("failed to read from disk: %w", err)
	}
	total := len(diskRecords)
	if offset >= len(diskRecords) {
		return []*record{}, total, nil
	}
	end := offset + limit
	if end > len(diskRecords) {
		end = len(diskRecords)
	}
	return diskRecords[offset:end], total, nil
}

func readFile(filePath string, indexName string, minTookMs int64, status string) ([]*record, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var results []*record
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var record record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue
		}

		if matchesFilters(&record, indexName, minTookMs, status) {
			results = append(results, &record)
		}
	}

	if err := scanner.Err(); err != nil {
		return results, err
	}

	return results, nil
}

func matchesFilters(record *record, indexName string, minTookMs int64, status string) bool {
	if (indexName != "" && record.Index != indexName) ||
		(minTookMs > 0 && record.TookMs < minTookMs) ||
		(status != "" && record.Status != status) {
		return false
	}
	return true
}

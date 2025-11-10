//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package search_history

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/couchbase/cbgt/rest"
)

type SearchHistoryHandler struct{}

func NewSearchHistoryHandler() *SearchHistoryHandler {
	return &SearchHistoryHandler{}
}

func (h *SearchHistoryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if Service == nil {
		rest.PropagateError(w, nil, "search history service not initialized", http.StatusServiceUnavailable)
		return
	}
	params := req.URL.Query()
	limitStr := params.Get("limit")
	offsetStr := params.Get("offset")
	indexName := params.Get("index")
	minDurationStr := params.Get("minDuration")
	status := params.Get("status")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > defaultMaxRecords {
		limit = defaultMaxRecords
	}

	offset := 0
	if offsetStr != "" {
		if v, err := strconv.Atoi(offsetStr); err == nil && v >= 0 {
			offset = v
		}
	}
	var minTookMs int64
	if minDurationStr != "" {
		ms, err := parseDuration(minDurationStr)
		if err == nil && ms > 0 {
			minTookMs = ms
		}
	}

	writerBaseDir := Service.writer.baseDir
	dataDir := filepath.Dir(writerBaseDir)

	results, total, err := read(dataDir, limit, offset, indexName, minTookMs, status)
	if err != nil {
		rest.PropagateError(w, nil, fmt.Sprintf("failed to read search history: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"results": results,
		"count":   len(results),
		"total":   total,
	}
	rest.MustEncode(w, response)
}

// converts duration strings like "2s", "750ms", "1.5s" to milliseconds.
func parseDuration(durationStr string) (int64, error) {
	durationStr = strings.TrimSpace(durationStr)

	if strings.HasSuffix(durationStr, "ms") {
		msStr := strings.TrimSuffix(durationStr, "ms")
		return strconv.ParseInt(msStr, 10, 64)
	}

	if strings.HasSuffix(durationStr, "s") {
		secStr := strings.TrimSuffix(durationStr, "s")
		seconds, err := strconv.ParseFloat(secStr, 64)
		if err != nil {
			return 0, err
		}
		return int64(seconds * 1000), nil
	}

	return strconv.ParseInt(durationStr, 10, 64)
}

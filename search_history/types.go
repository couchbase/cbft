//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package search_history

import (
	"time"
)

const (
	defaultMaxRecords  = 8000  // per node
	maxAllowedRecords  = 20000 // per node
	syncBatchThreshold = 100   // sync after this many unsynced records
)

// logPayload holds the raw data for a search request to be logged.
type logPayload struct {
	indexName   string
	requestBody []byte
	took        time.Duration
	totalHits   uint64
	status      string
}

// record represents a single completed search request.
type record struct {
	Timestamp time.Time `json:"timestamp"`
	Index     string    `json:"index"`
	Request   string    `json:"request"`
	TookMs    int64     `json:"tookMs"`
	TotalHits uint64    `json:"totalHits"`
	Status    string    `json:"status"`
}

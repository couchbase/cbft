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
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
)

// Service is the singleton search history service.
var Service *service
var serviceOnce sync.Once

// Manages search history logging and retrieval.
type service struct {
	writer     *logWriter
	logChan    chan logPayload // Buffered channel for async logging
	workerDone chan struct{}   // Signal when worker stops
	enabled    uint32          // 0 = disabled, 1 = enabled (atomic)
}

func Init(dataDir string) {
	serviceOnce.Do(func() {
		writer, err := newLogWriter(dataDir, defaultMaxRecords)
		if err != nil {
			log.Warnf("search_history: failed to initialize: %v", err)
			return
		}

		Service = &service{
			writer:     writer,
			logChan:    make(chan logPayload, defaultMaxRecords),
			workerDone: make(chan struct{}),
			enabled:    0,
		}
		go Service.logWorker()
	})
}

// Logs search requests asynchronously, and is non-blocking.
func (s *service) LogRequest(
	indexName string,
	requestBody []byte,
	took time.Duration,
	totalHits uint64,
	respErr error,
) {
	if s == nil || atomic.LoadUint32(&s.enabled) == 0 {
		return
	}

	status := "success"
	if respErr != nil {
		status = "failed"
	}

	payload := logPayload{
		indexName:   indexName,
		requestBody: requestBody,
		took:        took,
		totalHits:   totalHits,
		status:      status,
	}

	select {
	case s.logChan <- payload:
	default:
		// Drop record & exit if channel is full to avoid impacting search performance
	}
}

func (s *service) logWorker() {
	defer close(s.workerDone)
	for payload := range s.logChan {
		record, err := s.buildRecord(
			payload.indexName,
			payload.requestBody,
			payload.took,
			payload.totalHits,
			payload.status,
		)
		if err != nil {
			log.Warnf("search_history: failed to build record: %v", err)
			continue
		}
		s.writer.append(record)
	}
}

func (s *service) buildRecord(
	indexName string,
	requestBody []byte,
	took time.Duration,
	totalHits uint64,
	status string,
) (*record, error) {
	request, err := sanitizeRequestBody(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to sanitize request: %v", err)
	}

	record := &record{
		Timestamp: time.Now().Add(-took).UTC(),
		Index:     indexName,
		Request:   request,
		TookMs:    took.Milliseconds(),
		TotalHits: totalHits,
		Status:    status,
	}

	return record, nil
}

// UpdateSettings updates the enabled state and/or maxRecords.
func (s *service) UpdateSettings(enabled *bool, maxRecords *int) {
	if s == nil {
		return
	}

	if enabled != nil {
		var val uint32
		if *enabled {
			val = 1
		}
		atomic.StoreUint32(&s.enabled, val)
	}

	if maxRecords != nil && s.writer != nil {
		max := *maxRecords
		if max > 0 && max <= maxAllowedRecords {
			s.writer.resizeCapacity(max)
		}
	}
}

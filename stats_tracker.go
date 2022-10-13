//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"math"
	"sync"
	"time"
)

func InitTimeSeriesStatTracker() {
	st = &timeSeriesStatsTracker{
		entries: make(map[string]*statDetails),
	}
}

func TrackStatistic(name string, numEntries int, isCumulative, estimateRate bool) {
	if st != nil {
		st.trackStatistic(name, numEntries, isCumulative, estimateRate)
	}
}

func DetermineNewAverage(name string, val uint64) uint64 {
	if st == nil {
		return val
	}

	return st.determineNewAverage(name, val)
}

// -----------------------------------------------------------------------------

// timeSeriesStatsTracker to be initialized at start of process if
// intended to be utilized.
var st *timeSeriesStatsTracker

type timeSeriesStatsTracker struct {
	m       sync.Mutex
	entries map[string]*statDetails
}

type statDetails struct {
	values []uint64
	size   int

	cumulative bool
	lastEntry  uint64

	estimateRate bool
	lastRecord   time.Time

	count int
	sum   uint64

	cursor int
}

func (s *timeSeriesStatsTracker) trackStatistic(name string, numEntries int,
	isCumulative, estimateRate bool) {
	s.m.Lock()
	s.entries[name] = &statDetails{
		values:       make([]uint64, numEntries),
		size:         numEntries,
		cumulative:   isCumulative,
		estimateRate: estimateRate,
		lastRecord:   time.Now(),
	}
	s.m.Unlock()
}

func (s *timeSeriesStatsTracker) determineNewAverage(name string, val uint64) uint64 {
	s.m.Lock()
	defer s.m.Unlock()

	sd, exists := s.entries[name]
	if !exists {
		return val
	}

	sd.sum -= sd.values[sd.cursor]
	newEntry := val
	if sd.cumulative {
		newEntry -= sd.lastEntry
		sd.lastEntry = val
		if sd.estimateRate {
			timeNow := time.Now()
			if !sd.lastRecord.IsZero() {
				newEntry = uint64(math.Ceil(float64(newEntry) /
					timeNow.Sub(sd.lastRecord).Seconds()))
			}
			sd.lastRecord = timeNow
		}
	}
	sd.values[sd.cursor] = newEntry
	sd.sum += newEntry

	sd.cursor = (sd.cursor + 1) % sd.size

	if sd.count < sd.size {
		sd.count++
	}

	return sd.sum / uint64(sd.count)
}

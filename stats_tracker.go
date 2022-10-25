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

func TrackStatistic(name string, numEntries int, trackGrowthRate bool) {
	if st != nil {
		st.trackStatistic(name, numEntries, trackGrowthRate)
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

	trackGrowthRate bool
	lastEntry       uint64
	lastRecord      time.Time

	count int
	sum   uint64

	cursor int
}

func (s *timeSeriesStatsTracker) trackStatistic(name string, numEntries int,
	trackGrowthRate bool) {
	s.m.Lock()
	s.entries[name] = &statDetails{
		values:          make([]uint64, numEntries),
		size:            numEntries,
		trackGrowthRate: trackGrowthRate,
		lastRecord:      time.Now(),
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
	if sd.trackGrowthRate {
		if newEntry > sd.lastEntry {
			newEntry -= sd.lastEntry
			sd.lastEntry = val
		} else {
			newEntry = 0
		}

		timeNow := time.Now()
		newEntry = uint64(math.Round(float64(newEntry) /
			timeNow.Sub(sd.lastRecord).Seconds()))
		sd.lastRecord = timeNow
	}
	sd.values[sd.cursor] = newEntry
	sd.sum += newEntry

	sd.cursor = (sd.cursor + 1) % sd.size

	if sd.count < sd.size {
		sd.count++
	}

	return sd.sum / uint64(sd.count)
}

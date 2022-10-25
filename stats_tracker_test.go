//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"testing"
	"time"
)

func TestStatsTrackerUninitialized(t *testing.T) {
	if DetermineNewAverage("non_existant_stat", uint64(100)) != 100 {
		t.Fatal("Expected average to be equal to the entry")
	}
}

func TestStatsTrackerNoEntry(t *testing.T) {
	InitTimeSeriesStatTracker()
	if DetermineNewAverage("non_existant_stat", uint64(100)) != 100 {
		t.Fatal("Expected average to be equal to the entry")
	}
}

func TestStatsTrackerBasic1(t *testing.T) {
	InitTimeSeriesStatTracker()
	TrackStatistic("basic1", 10, false)
	for i := 0; i < 100; i++ {
		got := DetermineNewAverage("basic1", 2)
		if got != 2 {
			t.Fatalf("[%d] Expected average to remain the same, but got: %v", i+1, got)
		}
	}
}

func TestStatsTrackerBasic2(t *testing.T) {
	InitTimeSeriesStatTracker()
	avgConsecutiveNumbers := func(start, end int) uint64 {
		return uint64((start + end) / 2)
	}

	TrackStatistic("basic2", 1440, false)
	for i := 1; i <= 10000; i++ {
		start := 1
		if i > 1440 {
			start = i - 1440
		}
		expect := avgConsecutiveNumbers(start, i)
		got := DetermineNewAverage("basic2", uint64(i))
		if got != expect {
			t.Fatalf("[%d] Expected average: %v, got: %v", i, expect, got)
		}
	}
}

func TestStatsTrackerGrowthRate(t *testing.T) {
	InitTimeSeriesStatTracker()
	TrackStatistic("cumulative", 2, true)
	for i := 1; i <= 3; i++ {
		time.Sleep(1 * time.Second)
		expect := uint64(1)
		got := DetermineNewAverage("cumulative", uint64(i))
		if got != expect {
			t.Fatalf("[%d] Expected average: %v, got: %v", i, expect, got)
		}
	}
}

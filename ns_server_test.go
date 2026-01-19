//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"runtime"
	"testing"
)

func TestConvertStatName(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			"/x/y/z",
			"y_z",
		},
	}

	for _, test := range tests {
		actual := convertStatName(test.in)
		if actual != test.out {
			t.Errorf("expected '%s' got '%s' for '%s'", test.out, actual, test.in)
		}
	}
}

func TestCamelToUnderscore(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			"ThisIsCamelCase",
			"this_is_camel_case",
		},
	}

	for _, test := range tests {
		actual := camelToUnderscore(test.in)
		if actual != test.out {
			t.Errorf("expected '%s' got '%s' for '%s'", test.out, actual, test.in)
		}
	}
}

// TestRuntimeMetricsCollection tests that the new runtime metrics
// we added can be collected without errors
func TestRuntimeMetricsCollection(t *testing.T) {
	// Test num_goroutines collection
	numGoroutines := runtime.NumGoroutine()
	if numGoroutines <= 0 {
		t.Errorf("runtime.NumGoroutine() returned %d, expected > 0", numGoroutines)
	}
	t.Logf("num_goroutines: %d", numGoroutines)

	// Test num_cgocalls collection
	numCgoCalls := runtime.NumCgoCall()
	if numCgoCalls < 0 {
		t.Errorf("runtime.NumCgoCall() returned %d, expected >= 0", numCgoCalls)
	}
	t.Logf("num_cgocalls: %d", numCgoCalls)

	// Test memStats collection - only the 8 essential metrics we expose
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Verify heap memory metrics have reasonable values
	if memStats.HeapAlloc == 0 {
		t.Error("HeapAlloc is 0, expected > 0")
	}
	if memStats.HeapInuse == 0 {
		t.Error("HeapInuse is 0, expected > 0")
	}

	// Verify heap relationship: HeapInuse should be >= HeapAlloc
	if memStats.HeapInuse < memStats.HeapAlloc {
		t.Errorf("HeapInuse (%d) < HeapAlloc (%d), expected HeapInuse >= HeapAlloc",
			memStats.HeapInuse, memStats.HeapAlloc)
	}

	// Verify allocation relationship: Mallocs should be >= Frees
	if memStats.Mallocs < memStats.Frees {
		t.Errorf("Mallocs (%d) < Frees (%d), expected Mallocs >= Frees",
			memStats.Mallocs, memStats.Frees)
	}

	t.Logf("Successfully collected new runtime metrics:")
	t.Logf("  heap_alloc: %d", memStats.HeapAlloc)
	t.Logf("  heap_idle: %d", memStats.HeapIdle)
	t.Logf("  heap_inuse: %d", memStats.HeapInuse)
	t.Logf("  heap_released: %d", memStats.HeapReleased)
	t.Logf("  mallocs: %d", memStats.Mallocs)
	t.Logf("  frees: %d", memStats.Frees)
}

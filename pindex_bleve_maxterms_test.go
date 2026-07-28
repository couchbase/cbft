//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"sync/atomic"
	"testing"
)

// TestEffectiveBleveMaxTerms covers the precedence rule used to resolve the
// per-request bleveMaxTerms limit:
//   - a nil override (the field absent from the request) falls back to the
//     cluster-wide default (BleveMaxTermsLimit);
//   - a non-positive override (0 or negative) explicitly disables the cap,
//     resolving to 0 (no limit) regardless of the default;
//   - a positive override wins over the default.
func TestEffectiveBleveMaxTerms(t *testing.T) {
	orig := atomic.LoadInt64(&BleveMaxTermsLimit)
	defer atomic.StoreInt64(&BleveMaxTermsLimit, orig)

	intPtr := func(i int) *int { return &i }

	tests := []struct {
		name        string
		globalLimit int64
		override    *int
		want        int
	}{
		{"positive override wins over default", 1024, intPtr(50), 50},
		{"override wins even when larger than default", 1024, intPtr(5000), 5000},
		{"zero override disables the cap (no limit)", 1024, intPtr(0), 0},
		{"negative override disables the cap (no limit)", 1024, intPtr(-1), 0},
		{"nil override falls back to global default", 1024, nil, 1024},
		{"nil override with disabled global default", 0, nil, 0},
		{"positive override still applies when global disabled", 0, intPtr(200), 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt64(&BleveMaxTermsLimit, tt.globalLimit)
			if got := effectiveBleveMaxTerms(tt.override); got != tt.want {
				t.Fatalf("effectiveBleveMaxTerms(%v) with global default %d = %d, want %d",
					tt.override, tt.globalLimit, got, tt.want)
			}
		})
	}
}
//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"os"
	"testing"

	"github.com/couchbase/cbgt"
)

func TestMainStart(t *testing.T) {
	mr, err := cbgt.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = mainStart(nil, cbgt.NewUUID(), nil, "", 1, "", ":1000",
		"bad data dir", "./static", "etag", "", "", mr, nil)
	if err == nil {
		t.Errorf("expected empty server string to fail mainStart()")
	}

	err = mainStart(nil, cbgt.NewUUID(), nil, "", 1, "", ":1000",
		"bad data dir", "./static", "etag", "bad server", "", mr, nil)
	if err == nil {
		t.Errorf("expected bad server string to fail mainStart()")
	}
}

func TestMainWelcome(t *testing.T) {
	mainWelcome(flagAliases) // Don't crash.
}

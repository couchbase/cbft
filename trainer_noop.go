//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !vectors
// +build !vectors

package cbft

type trainerNoop struct {
}

func initTrainer(bleveDest *BleveDest, kvconfig map[string]interface{}) trainer {
	return nil
}

func (t *trainerNoop) acquireSamples() {}

func (t *trainerNoop) wait() {
	// no-op
}

func (t *trainerNoop) close() error {
	// no-op
	return nil
}

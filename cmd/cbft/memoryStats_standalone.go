//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !server
// +build !server

package main

import (
	sigar "github.com/cloudfoundry/gosigar"
)

// getMemoryLimit returns the host's total memory, in bytes.
func getMemoryLimit() (uint64, error) {
	mem := sigar.Mem{}

	mem.Get()

	return uint64(mem.Total * 1000), nil
}

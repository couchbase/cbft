//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !server
// +build !server

package cbft

import (
	"os"
	"runtime"
	"strconv"

	sigar "github.com/cloudfoundry/gosigar"
)

func InitSystemStats() error {
	// no-op
	return nil
}

func GetNumCPUs() string {
	gomaxprocs := os.Getenv("GOMAXPROCS")
	if gomaxprocs == "" {
		return strconv.Itoa(runtime.NumCPU())
	}
	return gomaxprocs
}

// GetMemoryLimit returns the host's total memory, in bytes.
func GetMemoryLimit() (uint64, error) {
	mem := sigar.Mem{}
	mem.Get()

	return uint64(mem.Total * 1000), nil
}

// currentCPUPercent returns current CPU (percent) used by process.
func currentCPUPercent() (float64, error) {
	procCPU := sigar.ProcCpu{}
	procCPU.Get(os.Getpid())

	return procCPU.Percent * 100, nil
}

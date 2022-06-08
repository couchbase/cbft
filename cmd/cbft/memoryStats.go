//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build server
// +build server

package main

//#cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
//#cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//#cgo LDFLAGS: -L${SRCDIR}/../../../build/sigar/src -lsigar -Wl,-rpath,${SRCDIR}/../../../sigar/include
//#cgo CFLAGS: -I ${SRCDIR}/../../../sigar/include
//#include <sigar.h>
//#include <sigar_control_group.h>
import "C"
import (
	"fmt"
)

var (
	sigarCgroupSupported uint8 = 1
)

type systemStats struct {
	handle *C.sigar_t
	pid    C.sigar_pid_t
}

// NewSystemStats returns a new systemStats after populating handler and PID.
func NewSystemStats() (*systemStats, error) {

	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, fmt.Errorf("memorystats: failed to open sigar, err: %v", err)
	}

	s := &systemStats{}
	s.handle = handle
	s.pid = C.sigar_pid_get(handle)

	return s, nil
}

// Close closes the systemStats handler.
func (s *systemStats) Close() {
	C.sigar_close(s.handle)
}

// getMemoryLimit returns total memory based on cgroup limits, if possible.
func getMemoryLimit() (uint64, error) {
	stats, err := NewSystemStats()
	if err != nil {
		return 0, fmt.Errorf("memorystats: failed to get new stats,err: %v", err)
	}
	defer stats.Close()

	memTotal, err := stats.SystemTotalMem()
	if err != nil {
		return 0, fmt.Errorf("memorystats: failed to get total mem,err: %v", err)
	}

	cgroupInfo := stats.GetControlGroupInfo()
	if cgroupInfo.Supported == sigarCgroupSupported {
		cGroupTotal := cgroupInfo.MemoryMax
		// cGroupTotal is with-in valid system limits
		if cGroupTotal > 0 && cGroupTotal <= memTotal {
			return cGroupTotal, nil
		}
	}

	return memTotal, nil
}

// SystemTotalMem returns the hosts-level memory limit in bytes.
func (s *systemStats) SystemTotalMem() (uint64, error) {
	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(s.handle, &mem); err != C.SIGAR_OK {
		return uint64(0), fmt.Errorf("memorystats: failed to get total memory, err: %v",
			C.sigar_strerror(s.handle, err))
	}
	return uint64(mem.total), nil
}

// sigarControlGroupInfo represents the subset of the cgroup info statistics
// relevant to FTS.
type sigarControlGroupInfo struct {
	Supported uint8 // "1" if cgroup info is supprted, "0" otherwise
	Version   uint8 // "1" for cgroup v1, "2" for cgroup v2

	// Maximum memory available in the group. Derived from memory.max
	MemoryMax uint64
}

// GetControlGroupInfo returns the fields of C.sigar_control_group_info_t FTS uses.
// These reflect Linux control group settings, which are used by Kubernetes to set
// pod, memory and CPU limits.
func (h *systemStats) GetControlGroupInfo() *sigarControlGroupInfo {
	var info C.sigar_control_group_info_t
	C.sigar_get_control_group_info(&info)

	return &sigarControlGroupInfo{
		Supported: uint8(info.supported),
		Version:   uint8(info.version),
		MemoryMax: uint64(info.memory_max),
	}
}

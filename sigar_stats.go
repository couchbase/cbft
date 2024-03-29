//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build server
// +build server

package cbft

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
//#include <sigar_control_group.h>
import "C"

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"time"
)

var stats *systemStats

// InitSystemStats should be called at process inception to
// initialize stats^.
func InitSystemStats() error {
	var err error
	stats, err = newSystemStats()
	if err != nil {
		return fmt.Errorf("InitSystemStats, err: %v", err)
	}
	return nil
}

// -----------------------------------------------------------------------------

func GetNumCPUs() string {
	cgroupInfo := stats.getControlGroupInfo()
	if cgroupInfo.Supported == sigarCgroupSupported {
		return strconv.FormatFloat(float64(cgroupInfo.NumCpuPrc)/100.0, 'f', 3, 64)
	}

	gomaxprocs := os.Getenv("GOMAXPROCS")
	if gomaxprocs == "" {
		return strconv.Itoa(runtime.NumCPU())
	}
	return gomaxprocs
}

// GetMemoryLimit returns total memory based on cgroup limits, if possible.
func GetMemoryLimit() (uint64, error) {
	memTotal, err := stats.systemTotalMem()
	if err != nil {
		return 0, fmt.Errorf("GetMemoryLimit: failed to get total mem, err: %v", err)
	}

	cgroupInfo := stats.getControlGroupInfo()
	if cgroupInfo.Supported == sigarCgroupSupported {
		cGroupTotal := cgroupInfo.MemoryMax
		// cGroupTotal is with-in valid system limits
		if cGroupTotal > 0 && cGroupTotal <= memTotal {
			return cGroupTotal, nil
		}
	}

	return memTotal, nil
}

// currentCPUPercent returns current CPU percent used by process.
func currentCPUPercent() (float64, error) {
	cpu, err := stats.processCPUPercent()
	if err != nil {
		return 0,
			fmt.Errorf("CurrentCPUPercent: failed to get current cpu, err: %v", err)
	}

	return cpu, nil
}

// -----------------------------------------------------------------------------

var (
	sigarCgroupSupported uint8 = 1
)

type systemStats struct {
	handle *C.sigar_t
	pid    C.sigar_pid_t

	lastCPU     float64
	lastCPUTime time.Time
}

// newSystemStats returns a new systemStats after populating handler and PID.
func newSystemStats() (*systemStats, error) {
	var handle *C.sigar_t

	if err := C.sigar_open(&handle); err != C.SIGAR_OK {
		return nil, fmt.Errorf("failed to open sigar, err: %v", err)
	}

	s := &systemStats{}
	s.handle = handle
	s.pid = C.sigar_pid_get(handle)

	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(s.handle, s.pid, &cpu); err != C.SIGAR_OK {
		return nil, fmt.Errorf("failed to get CPU, err: %w",
			C.sigar_strerror(s.handle, err))
	}
	s.lastCPU = float64(cpu.user + cpu.sys)
	s.lastCPUTime = time.Now()

	return s, nil
}

// close systemStats handler.
func (s *systemStats) close() {
	C.sigar_close(s.handle)
}

// systemTotalMem returns the hosts-level memory limit in bytes.
func (s *systemStats) systemTotalMem() (uint64, error) {
	var mem C.sigar_mem_t
	if err := C.sigar_mem_get(s.handle, &mem); err != C.SIGAR_OK {
		return uint64(0), fmt.Errorf("failed to get total memory, err: %v",
			C.sigar_strerror(s.handle, err))
	}

	return uint64(mem.total), nil
}

// processCPUPercent gets the percent CPU and is in range
// of [0, GOMAXPROCS] * 100.
// So a value of 123.4 means it is consuming 1.234 CPU cores.
func (s *systemStats) processCPUPercent() (float64, error) {
	var cpu C.sigar_proc_cpu_t
	if err := C.sigar_proc_cpu_get(s.handle, s.pid, &cpu); err != C.SIGAR_OK {
		return float64(0), fmt.Errorf("failed to get CPU, err: %w",
			C.sigar_strerror(s.handle, err))
	}

	totalCPU := float64(cpu.user + cpu.sys)
	currentTime := time.Now()
	timeDiffInMilliseconds := float64(currentTime.Sub(s.lastCPUTime).Milliseconds())
	if timeDiffInMilliseconds <= 0 {
		// Avoid divide by zero.
		timeDiffInMilliseconds = 1
	}

	cpuPercent := (totalCPU - s.lastCPU) / timeDiffInMilliseconds
	s.lastCPU = totalCPU
	s.lastCPUTime = currentTime

	return cpuPercent * 100, nil
}

// sigarControlGroupInfo represents the subset of the cgroup info statistics
// relevant to FTS.
type sigarControlGroupInfo struct {
	Supported uint8 // "1" if cgroup info is supprted, "0" otherwise
	Version   uint8 // "1" for cgroup v1, "2" for cgroup v2

	// Maximum memory available in the group. Derived from memory.max
	MemoryMax uint64

	NumCpuPrc uint16
}

// getControlGroupInfo returns the fields of C.sigar_control_group_info_t FTS uses.
// These reflect Linux control group settings, which are used by Kubernetes to set
// pod, memory and CPU limits.
func (h *systemStats) getControlGroupInfo() *sigarControlGroupInfo {
	var info C.sigar_control_group_info_t
	C.sigar_get_control_group_info(&info)

	return &sigarControlGroupInfo{
		Supported: uint8(info.supported),
		Version:   uint8(info.version),
		MemoryMax: uint64(info.memory_max),
		NumCpuPrc: uint16(info.num_cpu_prc),
	}
}

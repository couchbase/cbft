//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

//#include "malloc.h"
import "C"
import (
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	log "github.com/couchbase/clog"
)

var (
	jemallocStatMutex         sync.Mutex
	isJemallocProfilingActive int64
)

// Reports the heap memory allocated (in bytes) by the C memory manager,
// jemalloc or malloc, depending on the build configuration.
func cHeapAlloc() uint64 {
	return uint64(C.mm_allocated())
}

// Retrieves memory statistics from the C memory manager,
// specifically enabled when the jemalloc build tag is active.
// Returns an empty string otherwise. Statistics can be
// returned in either text or JSON format.
func cHeapStats(jsonFormat bool) string {
	jemallocStatMutex.Lock()
	defer jemallocStatMutex.Unlock()
	var buf *C.char
	if jsonFormat {
		// malloc stats returned are in marshalled JSON format
		buf = C.mm_stats_json()
	} else {
		// malloc stats returned are in text format
		buf = C.mm_stats_text()
	}
	s := ""
	if buf != nil {
		s += C.GoString(buf)
		C.free(unsafe.Pointer(buf))
	}
	return s
}

// Actions supported by the jemalloc memory manager:
// 1. ActivateProfiler: Activates memory profiler.
// 2. DeactivateProfiler: Deactivates memory profiler.
// 3. DumpProfile: Dumps memory profile to a byte array.
const (
	ActivateProfiler = iota
	DeactivateProfiler
	DumpProfile
)

// Manages the memory profiler in the C memory manager.
// Profiler activation, deactivation, and profile retrieval
// are supported actions. The profiler is enabled only
// when the jemalloc build tag is active, otherwise it
// returns an error. Return values vary based on the action:
//   - ActivateProfiler and DeactivateProfiler return nil
//     for profiler output, and an error if the action fails.
//   - DumpProfile returns an error if the action fails, else
//     returns the profile output as a byte array.
func cHeapProfiler(action int) (profilerOut []byte, err error) {
	switch action {
	case ActivateProfiler:
		if !atomic.CompareAndSwapInt64(&isJemallocProfilingActive, 0, 1) {
			err = fmt.Errorf("cHeapProfiler: could not activate jemalloc memory profiling: profiling already active")
			break
		}
		if errCode := int(C.mm_prof_activate()); errCode != 0 {
			err = fmt.Errorf("cHeapProfiler: error during jemalloc profile activate. err = [%v]", C.GoString(C.strerror(C.int(errCode))))
			// reset the profiling state
			atomic.CompareAndSwapInt64(&isJemallocProfilingActive, 1, 0)
			break
		}
		log.Printf("cHeapProfiler: activated jemalloc memory profiling")

	case DeactivateProfiler:
		if atomic.LoadInt64(&isJemallocProfilingActive) != 1 {
			err = fmt.Errorf("cHeapProfiler: failed to deactivate jemalloc memory profiling: profiling already deactivated")
			break
		}
		if errCode := int(C.mm_prof_deactivate()); errCode != 0 {
			err = fmt.Errorf("cHeapProfiler: error during jemalloc profile deactivate. err = [%v]", C.GoString(C.strerror(C.int(errCode))))
			break
		}
		if !atomic.CompareAndSwapInt64(&isJemallocProfilingActive, 1, 0) {
			err = fmt.Errorf("cHeapProfiler: failed to deactivate jemalloc memory profiling: profiling state changed unexpectedly")
			break
		}
		log.Printf("cHeapProfiler: deactivated jemalloc memory profiling")

	case DumpProfile:
		if atomic.LoadInt64(&isJemallocProfilingActive) != 1 {
			err = fmt.Errorf("cHeapProfiler: could not dump jemalloc profile: profiling is not active")
			break
		}
		// Create a temp file to dump the profile
		file, e := os.CreateTemp("./", "jemalloc_memory_profile")
		if e != nil {
			err = fmt.Errorf("cHeapProfiler: failed to create temp file: %v", e)
			break
		}
		// The temp file is removed after the profile is dumped
		fname := file.Name()
		filePathAsCString := C.CString(fname)
		defer func() {
			C.free(unsafe.Pointer(filePathAsCString))
			if e := os.RemoveAll(fname); e != nil {
				log.Warnf("cHeapProfiler: failed to remove temp file: %v", e)
			}
		}()
		// Close the temp file now so that jemalloc's mallctl can open it
		// and write the profile to it
		if e := file.Close(); e != nil {
			err = fmt.Errorf("cHeapProfiler: failed to close temp file: %v", e)
			break
		}
		// Dump the profile to the temp file
		if errCode := int(C.mm_prof_dump(filePathAsCString)); errCode != 0 {
			err = fmt.Errorf("cHeapProfiler: error during jemalloc profile dump. err = [%v]", C.GoString(C.strerror(C.int(errCode))))
			break
		}
		// Read the dumped profile from the temp file
		profilerOut, err = os.ReadFile(fname)
		if err != nil {
			err = fmt.Errorf("cHeapProfiler: failed to read dumped profile: %v", err)
			break
		}
		log.Printf("cHeapProfiler: collected jemalloc memory profile")

	default:
		err = fmt.Errorf("invalid action")
	}
	return profilerOut, err
}

// -------------------------------------------------------
// HTTP handler methods to retrieve information pertaining
// to the memory manager on the C side.

// Handle the request for the jemalloc memory manager stats (GET)
// URL parameters supported:
//   - json=true: returns the stats in JSON format, else defaults to
//     text format
func JeMallocStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(cHeapStats(r.URL.Query().Get("json") == "true")))
}

// Handle the request for interacting with the jemalloc memory profiler (GET)
// URL parameters supported:
//   - action=activate: activates the jemalloc profiler
//   - action=deactivate: deactivates the jemalloc profiler
//   - action=dump: returns the jemalloc profile as a file which can be read
//     by the client using the `jeprof`	tool. Can only be called AFTER the profiler
//     has been activated before. As long as the profiler has not been deactivated,
//     multiple dump requests can be made.
//
// The jemalloc profiler is in inactive state by default and must be activated using
// the `action=activate` parameter before any dump requests can be made
func JeMallocProfilerHandler(w http.ResponseWriter, r *http.Request) {
	action := r.URL.Query().Get("action")
	var profilerAction int
	switch action {
	case "activate":
		profilerAction = ActivateProfiler
	case "deactivate":
		profilerAction = DeactivateProfiler
	case "dump":
		profilerAction = DumpProfile
	default:
		writeError(w, "jemallocProfilerHandler: invalid `action`/`action` not specified. "+
			"Supported `action` values: `activate`, `deactivate`, `dump`", http.StatusBadRequest)
		return
	}
	profileOut, err := cHeapProfiler(profilerAction)
	if err != nil {
		writeAndLogError(w, "jemallocProfilerHandler: "+err.Error())
		return
	}
	if profilerAction == DumpProfile && profileOut != nil {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="jemem.prof"`)
		if _, err = w.Write(profileOut); err != nil {
			writeAndLogError(w, fmt.Sprintf("jemallocProfilerHandler: failed to write response: %v", err))
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func writeError(w http.ResponseWriter, errStr string, errType int) {
	w.WriteHeader(errType)
	w.Write([]byte(errStr + "\n"))
}

func writeAndLogError(w http.ResponseWriter, errStr string) {
	writeError(w, errStr, http.StatusInternalServerError)
	log.Warnf("failed during jemalloc memory profiling: %v", errStr)
}

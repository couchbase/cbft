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
	"net/http"
	"sync"
	"unsafe"
)

var (
	jemallocStatMutex sync.Mutex
)

func cHeapAlloc() uint64 {
	return uint64(C.mm_allocated())
}

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

// -------------------------------------------------------
// HTTP handler methods to retrieve information pertaining
// to the memory manager on the C side.

// handle the request for the jemalloc memory manager stats (GET)
// URL parameters supported:
//   - json=true: returns the stats in JSON format, else defaults to
//     text format
func JeMallocStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	w.Write([]byte(cHeapStats(r.URL.Query().Get("json") == "true")))
}

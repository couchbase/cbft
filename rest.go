//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"

	bleveHttp "github.com/blevesearch/bleve/http"

	"github.com/gorilla/mux"

	log "github.com/couchbaselabs/clog"
)

var startTime = time.Now()

func NewManagerRESTRouter(mgr *Manager, staticDir, staticETag string, mr *MsgRing) (
	*mux.Router, error) {
	// create a router to serve static files
	r := staticFileRouter(staticDir, staticETag, []string{
		"/indexes",
		"/nodes",
		"/monitor",
		"/manage",
		"/logs",
		"/debug",
	})

	r.Handle("/api/log", NewGetLogHandler(mr)).Methods("GET")

	r.Handle("/api/index", NewListIndexHandler(mgr)).Methods("GET")
	r.Handle("/api/index/{indexName}", NewCreateIndexHandler(mgr)).Methods("PUT")
	r.Handle("/api/index/{indexName}", NewDeleteIndexHandler(mgr)).Methods("DELETE")
	r.Handle("/api/index/{indexName}", NewGetIndexHandler(mgr)).Methods("GET")

	if mgr.tagsMap == nil || mgr.tagsMap["queryer"] {
		r.Handle("/api/index/{indexName}/count", NewCountHandler(mgr)).Methods("GET")
		r.Handle("/api/index/{indexName}/query", NewQueryHandler(mgr)).Methods("POST")
	}

	r.Handle("/api/index/{indexName}/ingest/{op}",
		NewIngestPauseResumeHandler(mgr)).Methods("POST")

	// We use standard bleveHttp handlers for the /api/pindex-bleve endpoints.
	//
	// TODO: Need to cleanly separate the /api/pindex and
	// /api/pindex-bleve endpoints.
	if mgr.tagsMap == nil || mgr.tagsMap["pindex"] {
		r.Handle("/api/pindex",
			NewListPIndexHandler(mgr)).Methods("GET")
		r.Handle("/api/pindex/{pindexName}",
			NewGetPIndexHandler(mgr)).Methods("GET")
		r.Handle("/api/pindex/{pindexName}/count",
			NewCountPIndexHandler(mgr)).Methods("GET")
		r.Handle("/api/pindex/{pindexName}/query",
			NewQueryPIndexHandler(mgr)).Methods("POST")

		listIndexesHandler := bleveHttp.NewListIndexesHandler()
		r.Handle("/api/pindex-bleve",
			listIndexesHandler).Methods("GET")

		getIndexHandler := bleveHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}",
			getIndexHandler).Methods("GET")

		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/count",
			docCountHandler).Methods("GET")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/query",
			searchHandler).Methods("POST")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = pindexNameLookup
		docGetHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex-bleve/{pindexName}/doc/{docID}",
			docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = pindexNameLookup
		debugDocHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex-bleve/{pindexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/fields",
			listFieldsHandler).Methods("GET")
	}

	r.Handle("/api/cfg", NewCfgGetHandler(mgr)).Methods("GET")
	r.Handle("/api/cfgRefresh", NewCfgRefreshHandler(mgr)).Methods("POST")

	r.Handle("/api/managerKick", NewManagerKickHandler(mgr)).Methods("POST")
	r.Handle("/api/managerMeta", NewManagerMetaHandler(mgr)).Methods("GET")

	r.Handle("/api/currentStats", NewCurrentStatsHandler(mgr)).Methods("GET")

	r.HandleFunc("/runtime", restGetRuntime).Methods("GET")
	r.HandleFunc("/runtime/flags", restGetRuntimeFlags).Methods("GET")
	r.HandleFunc("/runtime/gc", restPostRuntimeGC).Methods("POST")
	r.HandleFunc("/runtime/profile/cpu", restProfileCPU).Methods("POST")
	r.HandleFunc("/runtime/profile/memory", restProfileMemory).Methods("POST")
	r.HandleFunc("/runtime/memStats", restGetRuntimeMemStats).Methods("GET")

	return r, nil
}

func muxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

func restGetRuntime(w http.ResponseWriter, r *http.Request) {
	mustEncode(w, map[string]interface{}{
		"version":   VERSION,
		"startTime": startTime,
		"arch":      runtime.GOARCH,
		"os":        runtime.GOOS,
		"numCPU":    runtime.NumCPU(),
		"go": map[string]interface{}{
			"GOMAXPROCS":     runtime.GOMAXPROCS(0),
			"GOROOT":         runtime.GOROOT(),
			"version":        runtime.Version(),
			"numGoroutine":   runtime.NumGoroutine(),
			"numCgoCall":     runtime.NumCgoCall(),
			"compiler":       runtime.Compiler,
			"memProfileRate": runtime.MemProfileRate,
		},
	})
}

func restGetRuntimeFlags(w http.ResponseWriter, r *http.Request) {
	m := map[string]interface{}{}
	flag.VisitAll(func(f *flag.Flag) {
		m[f.Name] = f.Value
	})
	mustEncode(w, m)
}

func restPostRuntimeGC(w http.ResponseWriter, r *http.Request) {
	runtime.GC()
}

// To start a cpu profiling...
//    curl -X POST http://127.0.0.1:9090/runtime/profile/cpu -d secs=5
// To analyze a profiling...
//    go tool pprof ./cbft run-cpu.pprof
func restProfileCPU(w http.ResponseWriter, r *http.Request) {
	secs, err := strconv.Atoi(r.FormValue("secs"))
	if err != nil || secs <= 0 {
		http.Error(w, "incorrect or missing secs parameter", 400)
		return
	}
	fname := "./run-cpu.pprof"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't create file: %s, err: %v",
			fname, err), 500)
		return
	}
	log.Printf("cpu profiling start, file: %s", fname)
	err = pprof.StartCPUProfile(f)
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't start CPU profile, file: %s, err: %v",
			fname, err), 500)
		return
	}
	go func() {
		time.Sleep(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
		log.Printf("cpu profiling end, file: %s", fname)
	}()
	w.WriteHeader(204)
}

// To grab a memory profiling...
//    curl -X POST http://127.0.0.1:9090/runtime/profile/memory
// To analyze a profiling...
//    go tool pprof ./cbft run-memory.pprof
func restProfileMemory(w http.ResponseWriter, r *http.Request) {
	fname := "./run-memory.pprof"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		http.Error(w, fmt.Sprintf("couldn't create file: %v, err: %v",
			fname, err), 500)
		return
	}
	defer f.Close()
	pprof.WriteHeapProfile(f)
}

func restGetRuntimeMemStats(w http.ResponseWriter, r *http.Request) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	mustEncode(w, memStats)
}

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

	log "github.com/couchbase/clog"
)

var startTime = time.Now()

func NewManagerRESTRouter(versionMain string, mgr *Manager,
	staticDir, staticETag string, mr *MsgRing) (*mux.Router, error) {
	// create a router to serve static files
	r := staticFileRouter(staticDir, staticETag, []string{
		"/indexes",
		"/nodes",
		"/monitor",
		"/manage",
		"/logs",
		"/debug",
	})

	r.Handle("/api/index", NewListIndexHandler(mgr)).Methods("GET")
	r.Handle("/api/index/{indexName}", NewCreateIndexHandler(mgr)).Methods("PUT")
	r.Handle("/api/index/{indexName}", NewDeleteIndexHandler(mgr)).Methods("DELETE")
	r.Handle("/api/index/{indexName}", NewGetIndexHandler(mgr)).Methods("GET")

	if mgr.tagsMap == nil || mgr.tagsMap["queryer"] {
		r.Handle("/api/index/{indexName}/count",
			NewCountHandler(mgr)).Methods("GET")
		r.Handle("/api/index/{indexName}/query",
			NewQueryHandler(mgr)).Methods("POST")
	}

	r.Handle("/api/index/{indexName}/planFreezeControl/{op}",
		NewIndexControlHandler(mgr, "planFreeze", map[string]bool{
			"freeze":   true,
			"unfreeze": true,
		})).Methods("POST")
	r.Handle("/api/index/{indexName}/ingestControl/{op}",
		NewIndexControlHandler(mgr, "write", map[string]bool{
			"pause":  true,
			"resume": true,
		})).Methods("POST")
	r.Handle("/api/index/{indexName}/queryControl/{op}",
		NewIndexControlHandler(mgr, "read", map[string]bool{
			"allow":    true,
			"disallow": true,
		})).Methods("POST")

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

	r.Handle("/api/diag", NewDiagGetHandler(versionMain, mgr, mr)).Methods("GET")

	r.Handle("/api/log", NewGetLogHandler(mgr, mr)).Methods("GET")

	r.Handle("/api/managerKick", NewManagerKickHandler(mgr)).Methods("POST")
	r.Handle("/api/managerMeta", NewManagerMetaHandler(mgr)).Methods("GET")

	r.Handle("/api/runtime", NewRuntimeGetHandler(versionMain, mgr)).Methods("GET")

	r.HandleFunc("/api/runtime/flags", restGetRuntimeFlags).Methods("GET")
	r.HandleFunc("/api/runtime/gc", restPostRuntimeGC).Methods("POST")
	r.HandleFunc("/api/runtime/profile/cpu", restProfileCPU).Methods("POST")
	r.HandleFunc("/api/runtime/profile/memory", restProfileMemory).Methods("POST")
	r.HandleFunc("/api/runtime/stats", restGetRuntimeStats).Methods("GET")
	r.HandleFunc("/api/runtime/statsMem", restGetRuntimeStatsMem).Methods("GET")

	r.Handle("/api/stats", NewStatsHandler(mgr)).Methods("GET")

	return r, nil
}

func muxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

type RuntimeGetHandler struct {
	versionMain string
	mgr         *Manager
}

func NewRuntimeGetHandler(versionMain string, mgr *Manager) *RuntimeGetHandler {
	return &RuntimeGetHandler{versionMain: versionMain, mgr: mgr}
}

func (h *RuntimeGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mustEncode(w, map[string]interface{}{
		"versionMain": h.versionMain,
		"versionData": h.mgr.version,
		"arch":        runtime.GOARCH,
		"os":          runtime.GOOS,
		"numCPU":      runtime.NumCPU(),
		"go": map[string]interface{}{
			"GOMAXPROCS": runtime.GOMAXPROCS(0),
			"GOROOT":     runtime.GOROOT(),
			"version":    runtime.Version(),
			"compiler":   runtime.Compiler,
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
//    curl -X POST http://127.0.0.1:9090/api/runtime/profile/cpu -d secs=5
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
		http.Error(w, fmt.Sprintf("profileCPU:"+
			" couldn't create file: %s, err: %v",
			fname, err), 500)
		return
	}
	log.Printf("profileCPU: start, file: %s", fname)
	err = pprof.StartCPUProfile(f)
	if err != nil {
		http.Error(w, fmt.Sprintf("profileCPU:"+
			" couldn't start CPU profile, file: %s, err: %v",
			fname, err), 500)
		return
	}
	go func() {
		time.Sleep(time.Duration(secs) * time.Second)
		pprof.StopCPUProfile()
		f.Close()
		log.Printf("profileCPU: end, file: %s", fname)
	}()
	w.WriteHeader(204)
}

// To grab a memory profiling...
//    curl -X POST http://127.0.0.1:9090/api/runtime/profile/memory
// To analyze a profiling...
//    go tool pprof ./cbft run-memory.pprof
func restProfileMemory(w http.ResponseWriter, r *http.Request) {
	fname := "./run-memory.pprof"
	os.Remove(fname)
	f, err := os.Create(fname)
	if err != nil {
		http.Error(w, fmt.Sprintf("profileMemory:"+
			" couldn't create file: %v, err: %v",
			fname, err), 500)
		return
	}
	defer f.Close()
	pprof.WriteHeapProfile(f)
}

func restGetRuntimeStatsMem(w http.ResponseWriter, r *http.Request) {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	mustEncode(w, memStats)
}

func restGetRuntimeStats(w http.ResponseWriter, r *http.Request) {
	mustEncode(w, map[string]interface{}{
		"startTime": startTime,
		"go": map[string]interface{}{
			"numGoroutine":   runtime.NumGoroutine(),
			"numCgoCall":     runtime.NumCgoCall(),
			"memProfileRate": runtime.MemProfileRate,
		},
	})
}

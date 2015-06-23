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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/cbgt"
	"github.com/couchbaselabs/cbgt/rest"
)

// NewRESTRouter creates a mux.Router initialized with the REST
// API and web UI routes.  See also InitStaticRouter if you need finer
// control of the router initialization.
func NewRESTRouter(versionMain string, mgr *cbgt.Manager,
	staticDir, staticETag string, mr *cbgt.MsgRing) (
	*mux.Router, map[string]rest.RESTMeta, error) {
	return rest.InitRESTRouter(InitStaticRouter(),
		versionMain, mgr, staticDir, staticETag, mr,
		myAssetDir, myAsset)
}

// ------------------------------------------------------

// DiagGetHandler is a REST handler that retrieves diagnostic
// information for a node.
type DiagGetHandler struct {
	versionMain string
	mgr         *cbgt.Manager
	mr          *cbgt.MsgRing
}

func NewDiagGetHandler(versionMain string,
	mgr *cbgt.Manager, mr *cbgt.MsgRing) *DiagGetHandler {
	return &DiagGetHandler{versionMain: versionMain, mgr: mgr, mr: mr}
}

func (h *DiagGetHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	handlers := []cbgt.DiagHandler{
		{"/api/cfg", rest.NewCfgGetHandler(h.mgr), nil},
		{"/api/index", rest.NewListIndexHandler(h.mgr), nil},
		{"/api/log", rest.NewLogGetHandler(h.mgr, h.mr), nil},
		{"/api/managerMeta", rest.NewManagerMetaHandler(h.mgr, nil), nil},
		{"/api/pindex", rest.NewListPIndexHandler(h.mgr), nil},
		{"/api/runtime", rest.NewRuntimeGetHandler(h.versionMain, h.mgr), nil},
		{"/api/runtime/args", nil, rest.RESTGetRuntimeArgs},
		{"/api/runtime/stats", nil, rest.RESTGetRuntimeStats},
		{"/api/runtime/statsMem", nil, rest.RESTGetRuntimeStatsMem},
		{"/api/stats", rest.NewStatsHandler(h.mgr), nil},
		{"/debug/pprof/block?debug=1", nil,
			func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "block", 2)
			}},
		{"/debug/pprof/goroutine?debug=2", nil,
			func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "goroutine", 2)
			}},
		{"/debug/pprof/heap?debug=1", nil,
			func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "heap", 1)
			}},
		{"/debug/pprof/threadcreate?debug=1", nil,
			func(w http.ResponseWriter, r *http.Request) {
				DiagGetPProf(w, "threadcreate", 1)
			}},
	}

	for _, t := range cbgt.PIndexImplTypes {
		for _, h := range t.DiagHandlers {
			handlers = append(handlers, h)
		}
	}

	w.Write(cbgt.JsonOpenBrace)
	for i, handler := range handlers {
		if i > 0 {
			w.Write(cbgt.JsonComma)
		}
		w.Write([]byte(fmt.Sprintf(`"%s":`, handler.Name)))
		if handler.Handler != nil {
			handler.Handler.ServeHTTP(w, req)
		}
		if handler.HandlerFunc != nil {
			handler.HandlerFunc.ServeHTTP(w, req)
		}
	}

	var first = true
	var visit func(path string, f os.FileInfo, err error) error
	visit = func(path string, f os.FileInfo, err error) error {
		m := map[string]interface{}{
			"Path":    path,
			"Name":    f.Name(),
			"Size":    f.Size(),
			"Mode":    f.Mode(),
			"ModTime": f.ModTime().Format(time.RFC3339Nano),
			"IsDir":   f.IsDir(),
		}
		if strings.HasPrefix(f.Name(), "PINDEX_") || // Matches PINDEX_xxx_META.
			strings.HasSuffix(f.Name(), "_META") || // Matches PINDEX_META.
			strings.HasSuffix(f.Name(), ".json") { // Matches index_meta.json.
			b, err := ioutil.ReadFile(path)
			if err == nil {
				m["Contents"] = string(b)
			}
		}
		buf, err := json.Marshal(m)
		if err == nil {
			if !first {
				w.Write(cbgt.JsonComma)
			}
			w.Write(buf)
			first = false
		}
		return nil
	}

	w.Write([]byte(`,"dataDir":[`))
	filepath.Walk(h.mgr.DataDir(), visit)
	w.Write([]byte(`]`))

	entries, err := rest.AssetDir("static/dist")
	if err == nil {
		for _, name := range entries {
			// Ex: "static/dist/manifest.txt".
			a, err := rest.Asset("static/dist/" + name)
			if err == nil {
				j, err := json.Marshal(strings.TrimSpace(string(a)))
				if err == nil {
					w.Write([]byte(`,"`))
					w.Write([]byte("/static/dist/" + name))
					w.Write([]byte(`":`))
					w.Write(j)
				}
			}
		}
	}

	w.Write(cbgt.JsonCloseBrace)
}

func DiagGetPProf(w http.ResponseWriter, profile string, debug int) {
	var b bytes.Buffer
	pprof.Lookup(profile).WriteTo(&b, debug)
	rest.MustEncode(w, b.String())
}

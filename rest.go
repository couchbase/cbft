//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"
	"github.com/gorilla/mux"

	log "github.com/couchbaselabs/clog"
)

func NewManagerRESTRouter(mgr *Manager, staticDir string, mr *MsgRing) (*mux.Router, error) {
	// create a router to serve static files
	r := staticFileRouter(staticDir, []string{
		"/overview",
		"/search",
		"/indexes",
		"/analysis",
		"/monitor",
		"/logs",
	})

	r.Handle("/api/log", NewGetLogHandler(mr))
	r.Handle("/api/index", NewListIndexHandler(mgr)).Methods("GET")
	r.Handle("/api/index/{indexName}", NewCreateIndexHandler(mgr)).Methods("PUT")
	r.Handle("/api/index/{indexName}", NewDeleteIndexHandler(mgr)).Methods("DELETE")
	r.Handle("/api/index/{indexName}", NewGetIndexHandler(mgr)).Methods("GET")
	r.Handle("/api/index/{indexName}/count", NewCountHandler(mgr)).Methods("GET")
	r.Handle("/api/index/{indexName}/search", NewSearchHandler(mgr)).Methods("POST")

	// the rest are standard bleveHttp handlers for the lower "pindex" level...
	tags := mgr.Tags()
	if tags == nil || tags["queryer"] {
		r.Handle("/api/pindex", bleveHttp.NewListIndexesHandler()).Methods("GET")

		getIndexHandler := bleveHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}", getIndexHandler).Methods("GET")

		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}/count", docCountHandler).Methods("GET")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = indexNameLookup
		docGetHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex/{indexName}/doc/{docID}", docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = indexNameLookup
		debugDocHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex/{indexName}/docDebug/{docID}", debugDocHandler).Methods("GET")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}/search", searchHandler).Methods("POST")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}/fields", listFieldsHandler).Methods("GET")
	}

	return r, nil
}

func muxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

func docIDLookup(req *http.Request) string {
	return muxVariableLookup(req, "docID")
}

func indexNameLookup(req *http.Request) string {
	return muxVariableLookup(req, "indexName")
}

func indexAlias(mgr *Manager, indexName, indexUUID string) (bleve.IndexAlias, error) {
	// TODO: also add remote pindexes to alias, not just local pindexes.
	alias := bleve.NewIndexAlias()

	_, pindexes := mgr.CurrentMaps()
	for _, pindex := range pindexes {
		if pindex.IndexType == "bleve" &&
			pindex.IndexName == indexName &&
			(indexUUID == "" || pindex.IndexUUID == indexUUID) {
			bindex, ok := pindex.Impl.(bleve.Index)
			if ok && bindex != nil {
				alias.Add(bindex)
			}
		}
	}

	return alias, nil
}

// ---------------------------------------------------

type ListIndexHandler struct {
	mgr *Manager
}

func NewListIndexHandler(mgr *Manager) *ListIndexHandler {
	return &ListIndexHandler{mgr: mgr}
}

func (h *ListIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexDefs, _, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		showError(w, req, "could not retrieve index defs", 500)
		return
	}

	rv := struct {
		Status    string     `json:"status"`
		IndexDefs *IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type GetIndexHandler struct {
	mgr *Manager
}

func NewGetIndexHandler(mgr *Manager) *GetIndexHandler {
	return &GetIndexHandler{mgr: mgr}
}

func (h *GetIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	_, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		showError(w, req, "could not retrieve index defs", 500)
		return
	}

	indexDef, exists := indexDefsByName[indexName]
	if !exists || indexDef == nil {
		showError(w, req, "not an index", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")
	if indexUUID != "" && indexUUID != indexDef.UUID {
		showError(w, req, "wrong index UUID", 400)
		return
	}

	m := map[string]interface{}{}
	if indexDef.Schema != "" {
		if err := json.Unmarshal([]byte(indexDef.Schema), &m); err != nil {
			showError(w, req, "could not unmarshal mapping", 500)
			return
		}
	}

	rv := struct {
		Status       string                 `json:"status"`
		IndexDef     *IndexDef              `json:"indexDef"`
		IndexMapping map[string]interface{} `json:"indexMapping"`
	}{
		Status:       "ok",
		IndexDef:     indexDef,
		IndexMapping: m,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type CountHandler struct {
	mgr *Manager
}

func NewCountHandler(mgr *Manager) *CountHandler {
	return &CountHandler{mgr: mgr}
}

func (h *CountHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	alias, err := indexAlias(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("index alias: %v", err), 500)
		return
	}

	docCount, err := alias.DocCount()
	if err != nil {
		showError(w, req, fmt.Sprintf("error counting docs: %v", err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  docCount,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type SearchHandler struct {
	mgr *Manager
}

func NewSearchHandler(mgr *Manager) *SearchHandler {
	return &SearchHandler{mgr: mgr}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	log.Printf("search request: %s", indexName)

	alias, err := indexAlias(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("index alias: %v", err), 500)
		return
	}

	// read the request body
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	log.Printf("search request body: %s", requestBody)

	// parse the request
	var searchRequest bleve.SearchRequest
	err = json.Unmarshal(requestBody, &searchRequest)
	if err != nil {
		showError(w, req, fmt.Sprintf("error parsing query: %v", err), 400)
		return
	}

	log.Printf("search parsed request %#v", searchRequest)

	// varlidate the query
	err = searchRequest.Query.Validate()
	if err != nil {
		showError(w, req, fmt.Sprintf("error validating query: %v", err), 400)
		return
	}

	// execute the query
	searchResponse, err := alias.Search(&searchRequest)
	if err != nil {
		showError(w, req, fmt.Sprintf("error executing query: %v", err), 500)
		return
	}

	// encode the response
	mustEncode(w, searchResponse)
}

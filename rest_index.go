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
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/couchbaselabs/clog"
)

func docIDLookup(req *http.Request) string {
	return muxVariableLookup(req, "docID")
}

func indexNameLookup(req *http.Request) string {
	return muxVariableLookup(req, "indexName")
}

func pindexNameLookup(req *http.Request) string {
	return muxVariableLookup(req, "pindexName")
}

// ------------------------------------------------------------------

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

	planPIndexes, planPIndexesByName, err := h.mgr.GetPlanPIndexes(false)
	if err != nil {
		showError(w, req, fmt.Sprintf("GetPlanPIndexes, err: %v", err), 400)
	}

	mustEncode(w, struct {
		Status       string        `json:"status"`
		IndexDef     *IndexDef     `json:"indexDef"`
		PlanPIndexes []*PlanPIndex `json:"planPIndexes"`
		Warnings     []string      `json:"warnings"`
	}{
		Status:       "ok",
		IndexDef:     indexDef,
		PlanPIndexes: planPIndexesByName[indexName],
		Warnings:     planPIndexes.Warnings[indexName],
	})
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

	pindexImplType, err := PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Count == nil {
		showError(w, req, fmt.Sprintf("rest.Count,"+
			" no pindexImplType, indexName: %s, err: %v", indexName, err), 400)
		return
	}

	count, err := pindexImplType.Count(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest.Count,"+
			" indexName: %s, err: %v", indexName, err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  count,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type QueryHandler struct {
	mgr *Manager
}

func NewQueryHandler(mgr *Manager) *QueryHandler {
	return &QueryHandler{mgr: mgr}
}

func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest.Query,"+
			" could not read request body, indexName: %s", indexName), 400)
		return
	}

	pindexImplType, err := PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Query == nil {
		showError(w, req, fmt.Sprintf("rest.Query,"+
			" no pindexImplType, indexName: %s, err: %v", indexName, err), 400)
		return
	}

	log.Printf("rest.Query indexName: %s, requestBody: %s", indexName, requestBody)

	err = pindexImplType.Query(h.mgr, indexName, indexUUID, requestBody, w)
	if err != nil {
		if errCW, ok := err.(*ErrorConsistencyWait); ok {
			rv := struct {
				Status       string              `json:"status"`
				Message      string              `json:"message"`
				StartEndSeqs map[string][]uint64 `json:"startEndSeqs"`
			}{
				Status: errCW.Status,
				Message: fmt.Sprintf("rest.Query,"+
					" indexName: %s, requestBody: %s, req: %#v, err: %v",
					indexName, requestBody, req, err),
				StartEndSeqs: errCW.StartEndSeqs,
			}
			mustEncode(w, rv)
			return
		}

		showError(w, req, fmt.Sprintf("rest.Query,"+
			" indexName: %s, requestBody: %s, req: %#v, err: %v",
			indexName, requestBody, req, err), 400)
		return
	}

	log.Printf("rest.Query indexName: %s, DONE, requestBody: %s", indexName, requestBody)
}

// ---------------------------------------------------

type CountPIndexHandler struct {
	mgr *Manager
}

func NewCountPIndexHandler(mgr *Manager) *CountPIndexHandler {
	return &CountPIndexHandler{mgr: mgr}
}

func (h *CountPIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pindexName := pindexNameLookup(req)
	if pindexName == "" {
		showError(w, req, "pindex name is required", 400)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		showError(w, req, fmt.Sprintf("rest.CountPIndex,"+
			" no pindex, pindexName: %s", pindexName), 400)
		return
	}
	if pindex.Dest == nil {
		showError(w, req, fmt.Sprintf("rest.CountPIndex,"+
			" no pindex.Dest, pindexName: %s", pindexName), 400)
		return
	}

	pindexUUID := req.FormValue("pindexUUID")
	if pindexUUID != "" && pindex.UUID != pindexUUID {
		showError(w, req, fmt.Sprintf("rest.CountPIndex,"+
			" wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			pindexUUID, pindex.UUID, pindexName), 400)
		return
	}

	var cancelCh chan string // TODO: Support request timeout and cancellation.

	log.Printf("rest.CountPIndex pindexName: %s", pindexName)

	count, err := pindex.Dest.Count(pindex, cancelCh)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest.CountPIndex,"+
			" pindexName: %s, req: %#v, err: %v", pindexName, req, err), 400)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  count,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type QueryPIndexHandler struct {
	mgr *Manager
}

func NewQueryPIndexHandler(mgr *Manager) *QueryPIndexHandler {
	return &QueryPIndexHandler{mgr: mgr}
}

func (h *QueryPIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pindexName := pindexNameLookup(req)
	if pindexName == "" {
		showError(w, req, "pindex name is required", 400)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		showError(w, req, fmt.Sprintf("rest.QueryPIndex,"+
			" no pindex, pindexName: %s", pindexName), 400)
		return
	}
	if pindex.Dest == nil {
		showError(w, req, fmt.Sprintf("rest.QueryPIndex,"+
			" no pindex.Dest, pindexName: %s", pindexName), 400)
		return
	}

	pindexUUID := req.FormValue("pindexUUID")
	if pindexUUID != "" && pindex.UUID != pindexUUID {
		showError(w, req, fmt.Sprintf("rest.QueryPIndex,"+
			" wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			pindexUUID, pindex.UUID, pindexName), 400)
		return
	}

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest.QueryPIndex,"+
			" could not read request body, pindexName: %s", pindexName), 400)
		return
	}

	var cancelCh chan string // TODO: Support request timeout and cancellation.

	log.Printf("rest.QueryPIndex pindexName: %s, requestBody: %s",
		pindexName, requestBody)

	err = pindex.Dest.Query(pindex, requestBody, w, cancelCh)
	if err != nil {
		if errCW, ok := err.(*ErrorConsistencyWait); ok {
			rv := struct {
				Status       string              `json:"status"`
				Message      string              `json:"message"`
				StartEndSeqs map[string][]uint64 `json:"startEndSeqs"`
			}{
				Status: errCW.Status,
				Message: fmt.Sprintf("rest.QueryPIndex,"+
					" pindexName: %s, requestBody: %s, req: %#v, err: %v",
					pindexName, requestBody, req, err),
				StartEndSeqs: errCW.StartEndSeqs,
			}
			mustEncode(w, rv)
			return
		}

		showError(w, req, fmt.Sprintf("rest.QueryPIndex,"+
			" pindexName: %s, requestBody: %s, req: %#v, err: %v",
			pindexName, requestBody, req, err), 400)
		return
	}

	log.Printf("rest.QueryPIndex pindexName: %s, DONE, requestBody: %s",
		pindexName, requestBody)
}

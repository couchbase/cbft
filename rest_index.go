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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
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

func (h *GetIndexHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] = "required, string, URL path parameter\n\n" +
		"The name of the index definition to be retrieved."
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
		showError(w, req,
			fmt.Sprintf("rest_index: GetPlanPIndexes, err: %v", err), 400)
		return
	}

	planPIndexesForIndex := []*PlanPIndex(nil)
	if planPIndexesByName != nil {
		planPIndexesForIndex = planPIndexesByName[indexName]
	}

	planPIndexesWarnings := []string(nil)
	if planPIndexes != nil && planPIndexes.Warnings != nil {
		planPIndexesWarnings = planPIndexes.Warnings[indexName]
	}

	mustEncode(w, struct {
		Status       string        `json:"status"`
		IndexDef     *IndexDef     `json:"indexDef"`
		PlanPIndexes []*PlanPIndex `json:"planPIndexes"`
		Warnings     []string      `json:"warnings"`
	}{
		Status:       "ok",
		IndexDef:     indexDef,
		PlanPIndexes: planPIndexesForIndex,
		Warnings:     planPIndexesWarnings,
	})
}

// ---------------------------------------------------

type CountHandler struct {
	mgr *Manager
}

func NewCountHandler(mgr *Manager) *CountHandler {
	return &CountHandler{mgr: mgr}
}

func (h *CountHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] = "required, string, URL path parameter\n\n" +
		"The name of the index whose count is to be retrieved."
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
		showError(w, req, fmt.Sprintf("rest_index: Count,"+
			" no pindexImplType, indexName: %s, err: %v", indexName, err), 400)
		return
	}

	count, err := pindexImplType.Count(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_index: Count,"+
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

func (h *QueryHandler) RESTOpts(opts map[string]string) {
	indexTypes := []string(nil)
	for indexType, t := range PIndexImplTypes {
		if t.QuerySample != nil {
			indexTypes = append(indexTypes,
				"For index type ```"+indexType+"```"+
					", an example POST body:\n\n    "+IndentJSON(t.QuerySample, "    ", "  "))
		}
	}
	sort.Strings(indexTypes)

	opts["param: indexName"] = "required, string, URL path parameter\n\n" +
		"The name of the index to be queried."
	opts[""] = "The request's POST body depends on the index type:\n\n" +
		strings.Join(indexTypes, "\n")
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
		showError(w, req, fmt.Sprintf("rest_index: Query,"+
			" could not read request body, indexName: %s", indexName), 400)
		return
	}

	pindexImplType, err := PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Query == nil {
		showError(w, req, fmt.Sprintf("rest_index: Query,"+
			" no pindexImplType, indexName: %s, err: %v", indexName, err), 400)
		return
	}

	err = pindexImplType.Query(h.mgr, indexName, indexUUID, requestBody, w)
	if err != nil {
		if errCW, ok := err.(*ErrorConsistencyWait); ok {
			rv := struct {
				Status       string              `json:"status"`
				Message      string              `json:"message"`
				StartEndSeqs map[string][]uint64 `json:"startEndSeqs"`
			}{
				Status: errCW.Status,
				Message: fmt.Sprintf("rest_index: Query,"+
					" indexName: %s, requestBody: %s, req: %#v, err: %v",
					indexName, requestBody, req, err),
				StartEndSeqs: errCW.StartEndSeqs,
			}
			buf, err := json.Marshal(rv)
			if err == nil && buf != nil {
				showError(w, req, string(buf), 408)
				return
			}
		}

		showError(w, req, fmt.Sprintf("rest_index: Query,"+
			" indexName: %s, requestBody: %s, req: %#v, err: %v",
			indexName, requestBody, req, err), 400)
		return
	}
}

// ---------------------------------------------------

type IndexControlHandler struct {
	mgr        *Manager
	control    string
	allowedOps map[string]bool
}

func NewIndexControlHandler(mgr *Manager, control string,
	allowedOps map[string]bool) *IndexControlHandler {
	return &IndexControlHandler{
		mgr:        mgr,
		control:    control,
		allowedOps: allowedOps,
	}
}

func (h *IndexControlHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] = "required, string, URL path parameter\n\n" +
		"The name of the index whose control values will be modified."
}

func (h *IndexControlHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	op := muxVariableLookup(req, "op")
	if !h.allowedOps[op] {
		showError(w, req, fmt.Sprintf("rest_index: IndexControl,"+
			" error: unsupported op: %s", op), 400)
		return
	}

	err := fmt.Errorf("rest_index: unknown op")
	if h.control == "read" {
		err = h.mgr.IndexControl(indexName, indexUUID, op, "", "")
	} else if h.control == "write" {
		err = h.mgr.IndexControl(indexName, indexUUID, "", op, "")
	} else if h.control == "planFreeze" {
		err = h.mgr.IndexControl(indexName, indexUUID, "", "", op)
	}
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_index: IndexControl,"+
			" control: %s, could not op: %s, err: %v", h.control, op, err), 400)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}

// ------------------------------------------------------------------

type ListPIndexHandler struct {
	mgr *Manager
}

func NewListPIndexHandler(mgr *Manager) *ListPIndexHandler {
	return &ListPIndexHandler{mgr: mgr}
}

func (h *ListPIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	_, pindexes := h.mgr.CurrentMaps()

	rv := struct {
		Status   string             `json:"status"`
		PIndexes map[string]*PIndex `json:"pindexes"`
	}{
		Status:   "ok",
		PIndexes: pindexes,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type GetPIndexHandler struct {
	mgr *Manager
}

func NewGetPIndexHandler(mgr *Manager) *GetPIndexHandler {
	return &GetPIndexHandler{mgr: mgr}
}

func (h *GetPIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pindexName := pindexNameLookup(req)
	if pindexName == "" {
		showError(w, req, "rest_index: pindex name is required", 400)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		showError(w, req, fmt.Sprintf("rest_index: GetPIndex,"+
			" no pindex, pindexName: %s", pindexName), 400)
		return
	}

	mustEncode(w, struct {
		Status string  `json:"status"`
		PIndex *PIndex `json:"pindex"`
	}{
		Status: "ok",
		PIndex: pindex,
	})
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
		showError(w, req, "rest_index: pindex name is required", 400)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		showError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" no pindex, pindexName: %s", pindexName), 400)
		return
	}
	if pindex.Dest == nil {
		showError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" no pindex.Dest, pindexName: %s", pindexName), 400)
		return
	}

	pindexUUID := req.FormValue("pindexUUID")
	if pindexUUID != "" && pindex.UUID != pindexUUID {
		showError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
			" wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			pindexUUID, pindex.UUID, pindexName), 400)
		return
	}

	var cancelCh <-chan bool

	cn, ok := w.(http.CloseNotifier)
	if ok && cn != nil {
		cnc := cn.CloseNotify()
		if cnc != nil {
			cancelCh = cnc
		}
	}

	count, err := pindex.Dest.Count(pindex, cancelCh)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_index: CountPIndex,"+
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
		showError(w, req, "rest_index: pindex name is required", 400)
		return
	}

	pindex := h.mgr.GetPIndex(pindexName)
	if pindex == nil {
		showError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" no pindex, pindexName: %s", pindexName), 400)
		return
	}
	if pindex.Dest == nil {
		showError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" no pindex.Dest, pindexName: %s", pindexName), 400)
		return
	}

	pindexUUID := req.FormValue("pindexUUID")
	if pindexUUID != "" && pindex.UUID != pindexUUID {
		showError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			pindexUUID, pindex.UUID, pindexName), 400)
		return
	}

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" could not read request body, pindexName: %s", pindexName), 400)
		return
	}

	var cancelCh <-chan bool

	cn, ok := w.(http.CloseNotifier)
	if ok && cn != nil {
		cnc := cn.CloseNotify()
		if cnc != nil {
			cancelCh = cnc
		}
	}

	err = pindex.Dest.Query(pindex, requestBody, w, cancelCh)
	if err != nil {
		if errCW, ok := err.(*ErrorConsistencyWait); ok {
			rv := struct {
				Status       string              `json:"status"`
				Message      string              `json:"message"`
				StartEndSeqs map[string][]uint64 `json:"startEndSeqs"`
			}{
				Status: errCW.Status,
				Message: fmt.Sprintf("rest_index: QueryPIndex,"+
					" pindexName: %s, requestBody: %s, req: %#v, err: %v",
					pindexName, requestBody, req, err),
				StartEndSeqs: errCW.StartEndSeqs,
			}
			buf, err := json.Marshal(rv)
			if err == nil && buf != nil {
				showError(w, req, string(buf), 408)
				return
			}
		}

		showError(w, req, fmt.Sprintf("rest_index: QueryPIndex,"+
			" pindexName: %s, requestBody: %s, req: %#v, err: %v",
			pindexName, requestBody, req, err), 400)
		return
	}
}

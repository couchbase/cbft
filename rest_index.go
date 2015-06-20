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

	"github.com/couchbaselabs/cbgt"
)

// ListIndexHandler is a REST handler for list indexes.
type ListIndexHandler struct {
	mgr *cbgt.Manager
}

func NewListIndexHandler(mgr *cbgt.Manager) *ListIndexHandler {
	return &ListIndexHandler{mgr: mgr}
}

func (h *ListIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexDefs, _, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		showError(w, req, "could not retrieve index defs", 500)
		return
	}

	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	cbgt.MustEncode(w, rv)
}

// ---------------------------------------------------

// GetIndexHandler is a REST handler for retrieving an index
// definition.
type GetIndexHandler struct {
	mgr *cbgt.Manager
}

func NewGetIndexHandler(mgr *cbgt.Manager) *GetIndexHandler {
	return &GetIndexHandler{mgr: mgr}
}

func (h *GetIndexHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index definition to be retrieved."
}

func (h *GetIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := cbgt.IndexNameLookup(req)
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

	planPIndexes, planPIndexesByName, err :=
		h.mgr.GetPlanPIndexes(false)
	if err != nil {
		showError(w, req,
			fmt.Sprintf("rest_index: GetPlanPIndexes, err: %v",
				err), 400)
		return
	}

	planPIndexesForIndex := []*cbgt.PlanPIndex(nil)
	if planPIndexesByName != nil {
		planPIndexesForIndex = planPIndexesByName[indexName]
	}

	planPIndexesWarnings := []string(nil)
	if planPIndexes != nil && planPIndexes.Warnings != nil {
		planPIndexesWarnings = planPIndexes.Warnings[indexName]
	}

	cbgt.MustEncode(w, struct {
		Status       string             `json:"status"`
		IndexDef     *cbgt.IndexDef     `json:"indexDef"`
		PlanPIndexes []*cbgt.PlanPIndex `json:"planPIndexes"`
		Warnings     []string           `json:"warnings"`
	}{
		Status:       "ok",
		IndexDef:     indexDef,
		PlanPIndexes: planPIndexesForIndex,
		Warnings:     planPIndexesWarnings,
	})
}

// ---------------------------------------------------

// CountHandler is a REST handler for counting documents/entries in an
// index.
type CountHandler struct {
	mgr *cbgt.Manager
}

func NewCountHandler(mgr *cbgt.Manager) *CountHandler {
	return &CountHandler{mgr: mgr}
}

func (h *CountHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index whose count is to be retrieved."
}

func (h *CountHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := cbgt.IndexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	pindexImplType, err :=
		cbgt.PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Count == nil {
		showError(w, req, fmt.Sprintf("rest_index: Count,"+
			" no pindexImplType, indexName: %s, err: %v",
			indexName, err), 400)
		return
	}

	count, err :=
		pindexImplType.Count(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_index: Count,"+
			" indexName: %s, err: %v",
			indexName, err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  count,
	}
	cbgt.MustEncode(w, rv)
}

// ---------------------------------------------------

// QueryHandler is a REST handler for querying an index.
type QueryHandler struct {
	mgr *cbgt.Manager
}

func NewQueryHandler(mgr *cbgt.Manager) *QueryHandler {
	return &QueryHandler{mgr: mgr}
}

func (h *QueryHandler) RESTOpts(opts map[string]string) {
	indexTypes := []string(nil)
	for indexType, t := range cbgt.PIndexImplTypes {
		if t.QuerySamples != nil {
			s := "For index type ```" + indexType + "```:\n\n"

			for _, sample := range t.QuerySamples() {
				if sample.Text != "" {
					s = s + sample.Text + "\n\n"
				}

				if sample.JSON != nil {
					s = s + "    " +
						cbgt.IndentJSON(sample.JSON, "    ", "  ") +
						"\n"
				}
			}

			indexTypes = append(indexTypes, s)
		}
	}
	sort.Strings(indexTypes)

	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index to be queried."
	opts[""] =
		"The request's POST body depends on the index type:\n\n" +
			strings.Join(indexTypes, "\n")
}

func (h *QueryHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := cbgt.IndexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_index: Query,"+
			" could not read request body, indexName: %s",
			indexName), 400)
		return
	}

	pindexImplType, err :=
		cbgt.PIndexImplTypeForIndex(h.mgr.Cfg(), indexName)
	if err != nil || pindexImplType.Query == nil {
		showError(w, req, fmt.Sprintf("rest_index: Query,"+
			" no pindexImplType, indexName: %s, err: %v",
			indexName, err), 400)
		return
	}

	err = pindexImplType.Query(h.mgr, indexName, indexUUID, requestBody, w)
	if err != nil {
		if errCW, ok := err.(*cbgt.ErrorConsistencyWait); ok {
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

// IndexControlHandler is a REST handler for processing admin control
// requests on an index.
type IndexControlHandler struct {
	mgr        *cbgt.Manager
	control    string
	allowedOps map[string]bool
}

func NewIndexControlHandler(mgr *cbgt.Manager, control string,
	allowedOps map[string]bool) *IndexControlHandler {
	return &IndexControlHandler{
		mgr:        mgr,
		control:    control,
		allowedOps: allowedOps,
	}
}

func (h *IndexControlHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index whose control values will be modified."
}

func (h *IndexControlHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := cbgt.IndexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	op := cbgt.MuxVariableLookup(req, "op")
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
			" control: %s, could not op: %s, err: %v",
			h.control, op, err), 400)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	cbgt.MustEncode(w, rv)
}

// ------------------------------------------------------------------

// ListPIndexHandler is a REST handler for listing pindexes.
type ListPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewListPIndexHandler(mgr *cbgt.Manager) *ListPIndexHandler {
	return &ListPIndexHandler{mgr: mgr}
}

func (h *ListPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	_, pindexes := h.mgr.CurrentMaps()

	rv := struct {
		Status   string                  `json:"status"`
		PIndexes map[string]*cbgt.PIndex `json:"pindexes"`
	}{
		Status:   "ok",
		PIndexes: pindexes,
	}
	cbgt.MustEncode(w, rv)
}

// ---------------------------------------------------

// GetPIndexHandler is a REST handler for retrieving information on a
// pindex.
type GetPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewGetPIndexHandler(mgr *cbgt.Manager) *GetPIndexHandler {
	return &GetPIndexHandler{mgr: mgr}
}

func (h *GetPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	pindexName := cbgt.PIndexNameLookup(req)
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

	cbgt.MustEncode(w, struct {
		Status string       `json:"status"`
		PIndex *cbgt.PIndex `json:"pindex"`
	}{
		Status: "ok",
		PIndex: pindex,
	})
}

// ---------------------------------------------------

// CountPIndexHandler is a REST handler for counting the
// documents/entries in a pindex.
type CountPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewCountPIndexHandler(mgr *cbgt.Manager) *CountPIndexHandler {
	return &CountPIndexHandler{mgr: mgr}
}

func (h *CountPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	pindexName := cbgt.PIndexNameLookup(req)
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
			" pindexName: %s, req: %#v, err: %v",
			pindexName, req, err), 400)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  count,
	}
	cbgt.MustEncode(w, rv)
}

// ---------------------------------------------------

// QueryPIndexHandler is a REST handler for querying a pindex.
type QueryPIndexHandler struct {
	mgr *cbgt.Manager
}

func NewQueryPIndexHandler(mgr *cbgt.Manager) *QueryPIndexHandler {
	return &QueryPIndexHandler{mgr: mgr}
}

func (h *QueryPIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	pindexName := cbgt.PIndexNameLookup(req)
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
			" could not read request body, pindexName: %s",
			pindexName), 400)
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
		if errCW, ok := err.(*cbgt.ErrorConsistencyWait); ok {
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

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

type CreateIndexHandler struct {
	mgr *Manager
}

func NewCreateIndexHandler(mgr *Manager) *CreateIndexHandler {
	return &CreateIndexHandler{mgr: mgr}
}

func (h *CreateIndexHandler) RESTOpts(opts map[string]string) {
	opts["form value: indexType"] = "required, string"
	opts["form value: indexParams"] = "optional, string (JSON)"
	opts["form value: sourceType"] = "required, string"
	opts["form value: sourceName"] = "optional, string"
	opts["form value: sourceUUID"] = "optional, string"
	opts["form value: sourceParams"] = "optional, string (JSON)"
	opts["form value: planParams"] = "optional, string (JSON)"
	opts["form value: prevIndexUUID"] = "optional, string"
	opts["result on error"] = `non-200 HTTP error code`
	opts["result on success"] = `HTTP 200 with body JSON of {"status": "ok"}` // TODO: 200.
}

func (h *CreateIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// TODO: Need more input validation (check source UUID's, name lengths, etc).
	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		showError(w, req, "rest_create_index: index name is required", 400)
		return
	}

	indexType := req.FormValue("indexType")
	if indexType == "" {
		showError(w, req, "rest_create_index: index type is required", 400)
		return
	}

	indexParams := req.FormValue("indexParams")

	sourceType := req.FormValue("sourceType")
	if sourceType == "" {
		showError(w, req, "rest_create_index: source type is required", 400)
		return
	}

	sourceName := req.FormValue("sourceName")
	if sourceName == "" {
		// NOTE: Some sourceTypes (like "nil") don't care if sourceName is "".
		if sourceType == "couchbase" {
			sourceName = indexName // TODO: Revisit default of sourceName as indexName.
		}
	}

	sourceUUID := req.FormValue("sourceUUID") // Defaults to "".

	sourceParams := req.FormValue("sourceParams") // Defaults to "".

	planParams := &PlanParams{}
	planParamsStr := req.FormValue("planParams")
	if planParamsStr != "" {
		err := json.Unmarshal([]byte(planParamsStr), planParams)
		if err != nil {
			showError(w, req, fmt.Sprintf("rest_create_index:"+
				" error parsing planParams: %s, err: %v",
				planParamsStr, err), 400)
			return
		}
	}

	prevIndexUUID := req.FormValue("prevIndexUUID") // Defaults to "".

	err := h.mgr.CreateIndex(sourceType, sourceName, sourceUUID, sourceParams,
		indexType, indexName, string(indexParams), *planParams, prevIndexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("rest_create_index:"+
			" error creating index: %s, err: %v",
			indexName, err), 400)
		return
	}

	mustEncode(w, struct { // TODO: Should return created instead of 200 HTTP code?
		Status string `json:"status"`
	}{Status: "ok"})
}

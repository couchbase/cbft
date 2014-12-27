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

func (h *CreateIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// TODO: Need more input validation (check the UUID's, name lengths, etc).
	indexType := req.FormValue("indexType")
	if indexType == "" {
		indexType = "bleve" // TODO: Revisit default indexType?  Should be table'ized?
	}

	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexParams := req.FormValue("indexParams")

	sourceType := req.FormValue("sourceType")
	if sourceType == "" {
		sourceType = "couchbase" // TODO: Revisit default of sourceType as couchbase.
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
			showError(w, req, fmt.Sprintf("error parsing planParams: %s, err: %v",
				planParamsStr, err), 400)
			return
		}
	}

	err := h.mgr.CreateIndex(sourceType, sourceName, sourceUUID, sourceParams,
		indexType, indexName, string(indexParams), *planParams)
	if err != nil {
		showError(w, req, fmt.Sprintf("error creating index: %s, err: %v",
			indexName, err), 400)
		return
	}

	mustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

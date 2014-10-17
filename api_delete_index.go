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

package main

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

type DeleteIndexHandler struct {
	mgr *Manager
}

func NewDeleteIndexHandler(mgr *Manager) *DeleteIndexHandler {
	return &DeleteIndexHandler{mgr: mgr}
}

func (h *DeleteIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// find the name of the index to delete
	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	err := h.mgr.DeleteIndex(indexName)
	if err != nil {
		showError(w, req, fmt.Sprintf("error deleting index, err: %v", err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}

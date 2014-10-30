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
	"io/ioutil"
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
	indexType := "bleve"

	// find the name of the index to create
	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	// read the request body, which is treated as index mapping JSON bytes
	indexSchema, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	sourceType := "couchbase"
	sourceName := indexName
	sourceUUID := ""
	sourceParams := ""
	planParams := PlanParams{}

	// TODO: bad assumption of bucketName == indexName right now.
	// TODO: need a bucketUUID, or perhaps "" just means use latest.
	err = h.mgr.CreateIndex(sourceType, sourceName, sourceUUID, sourceParams,
		indexType, indexName, string(indexSchema), planParams)
	if err != nil {
		showError(w, req, fmt.Sprintf("error creating index: %s, err: %v",
			indexName, err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}

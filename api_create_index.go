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

	bleveHttp "github.com/blevesearch/bleve/http"

	"github.com/gorilla/mux"
)

type CreateIndexHandler struct {
	mgr *Manager
}

func NewCreateIndexHander(mgr *Manager) *CreateIndexHandler {
	return &CreateIndexHandler{
		mgr: mgr,
	}
}

func (h *CreateIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// find the name of the index to create
	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	// read the request body
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	mgr := h.mgr

	// TODO: a logical index might map to multiple PIndexes, not the current 1-to-1.
	indexPath := mgr.IndexPath(indexName)

	// TODO: need to check if this pindex already exists?
	// TODO: need to alloc a version/uuid for the pindex?
	pindex, err := NewPIndex(indexName, indexPath, requestBody)
	if err != nil {
		showError(w, req, fmt.Sprintf("error running pindex: %v", err), 500)
		return
	}

	mgr.RegisterPIndex(indexName, pindex)

	bleveHttp.RegisterIndexName(indexName, pindex.Index())

	// make sure there is a bucket with this name
	// TODO: incorporate bucket UUID somehow
	// TODO: Also, for now indexName == bucketName
	uuid := ""
	feed, err := NewTAPFeed(mgr.server, "default", indexName, uuid)
	if err != nil {
		// TODO: cleanup?
		showError(w, req, fmt.Sprintf("error preparing feed: %v", err), 400)
		return
	}

	if err = feed.Start(); err != nil {
		// TODO: cleanup?
		showError(w, req, fmt.Sprintf("error starting feed: %v", err), 500)
		return
	}

	mgr.RegisterFeed(indexName, feed)

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}

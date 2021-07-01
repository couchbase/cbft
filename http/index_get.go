//  Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package http

import (
	"fmt"
	"net/http"

	"github.com/blevesearch/bleve/v2/mapping"
)

type GetIndexHandler struct {
	IndexNameLookup varLookupFunc
}

func NewGetIndexHandler() *GetIndexHandler {
	return &GetIndexHandler{}
}

func (h *GetIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// find the name of the index to create
	var indexName string
	if h.IndexNameLookup != nil {
		indexName = h.IndexNameLookup(req)
	}
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	index := IndexByName(indexName)
	if index == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	rv := struct {
		Status  string               `json:"status"`
		Name    string               `json:"name"`
		Mapping mapping.IndexMapping `json:"mapping"`
	}{
		Status:  "ok",
		Name:    indexName,
		Mapping: index.Mapping(),
	}
	mustEncode(w, rv)
}

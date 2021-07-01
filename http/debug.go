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

	"github.com/blevesearch/bleve/v2/index/upsidedown"
)

// DebugDocumentHandler allows you to debug the index content
// for a given document id.
type DebugDocumentHandler struct {
	defaultIndexName string
	IndexNameLookup  varLookupFunc
	DocIDLookup      varLookupFunc
}

func NewDebugDocumentHandler(defaultIndexName string) *DebugDocumentHandler {
	return &DebugDocumentHandler{
		defaultIndexName: defaultIndexName,
	}
}

func (h *DebugDocumentHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	// find the index to operate on
	var indexName string
	if h.IndexNameLookup != nil {
		indexName = h.IndexNameLookup(req)
	}
	if indexName == "" {
		indexName = h.defaultIndexName
	}
	index := IndexByName(indexName)
	if index == nil {
		showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
		return
	}

	// find the docID
	var docID string
	if h.DocIDLookup != nil {
		docID = h.DocIDLookup(req)
	}

	internalIndex, err := index.Advanced()
	if err != nil {
		showError(w, req, fmt.Sprintf("error getting index: %v", err), 500)
		return
	}
	internalIndexReader, err := internalIndex.Reader()
	if err != nil {
		showError(w, req, fmt.Sprintf("error operning index reader: %v", err), 500)
		return
	}
	upsidedownIndexReader, ok := internalIndexReader.(*upsidedown.IndexReader)
	if !ok {
		showError(w, req, "error with index reader", 500)
		return
	}

	var rv []interface{}
	rowChan := upsidedownIndexReader.DumpDoc(docID)
	for row := range rowChan {
		switch row := row.(type) {
		case error:
			showError(w, req, fmt.Sprintf("error debugging document: %v", row), 500)
			return
		case upsidedown.UpsideDownCouchRow:
			tmp := struct {
				Key []byte `json:"key"`
				Val []byte `json:"val"`
			}{
				Key: row.Key(),
				Val: row.Value(),
			}
			rv = append(rv, tmp)
		}
	}
	err = internalIndexReader.Close()
	if err != nil {
		showError(w, req, fmt.Sprintf("error closing index reader: %v", err), 500)
		return
	}
	mustEncode(w, rv)
}

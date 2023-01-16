//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"io"
	"net/http"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// AnalyzeDocHandler is a REST handler for analyzing documents against
// a given index.
type AnalyzeDocHandler struct {
	mgr *cbgt.Manager
}

func NewAnalyzeDocHandler(mgr *cbgt.Manager) *AnalyzeDocHandler {
	return &AnalyzeDocHandler{mgr: mgr}
}

func (h *AnalyzeDocHandler) RESTOpts(opts map[string]string) {
	opts["param: indexName"] =
		"required, string, URL path parameter\n\n" +
			"The name of the index against which the doc needs to be analyzed."
}

func (h *AnalyzeDocHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := rest.IndexNameLookup(req)
	if indexName == "" {
		rest.ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		rest.ShowErrorBody(w, nil, fmt.Sprintf("bleve: AnalyzeDoc,"+
			" could not read request body, indexName: %s",
			indexName), http.StatusBadRequest)
		return
	}

	_, _, err = cbgt.GetIndexDef(h.mgr.Cfg(), indexName)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("bleve: AnalyzeDoc,"+
			" no indexName: %s found, err: %v",
			indexName, err), http.StatusBadRequest)
	}

	err = AnalyzeDoc(h.mgr, indexName, indexUUID, requestBody, w)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("bleve: AnalyzeDoc,"+
			" indexName: %s, err: %v",
			indexName, err), http.StatusInternalServerError)
		return
	}
}

func AnalyzeDoc(mgr *cbgt.Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	pindexes, _, _, err := mgr.CoveringPIndexesEx(
		cbgt.CoveringPIndexesSpec{
			IndexName:            indexName,
			IndexUUID:            indexUUID,
			PlanPIndexFilterName: "canRead",
		}, nil, false)

	if err != nil {
		return err
	}

	if len(pindexes) == 0 {
		return fmt.Errorf("bleve: AnalyzeDoc, no local pindexes found")
	}

	bindex, bdest, _, err := bleveIndex(pindexes[0])
	if err != nil {
		return err
	}

	defaultType := "_default"
	if imi, ok := bindex.Mapping().(*mapping.IndexMappingImpl); ok {
		defaultType = imi.DefaultType
	}

	var cbftDoc *BleveDocument
	cbftDoc, err = bdest.bleveDocConfig.BuildDocument([]byte("key"), req, defaultType)
	if err != nil {
		return err
	}

	idx, err := bindex.Advanced()
	if err != nil {
		return err
	}

	sh, ok := idx.(*scorch.Scorch)
	if !ok {
		return fmt.Errorf("Method only supported for scorch index type")
	}

	doc := document.NewDocument("key")
	err = bindex.Mapping().MapDocument(doc, cbftDoc)
	if err != nil {
		return err
	}

	sh.Analyze(doc)

	analyzed := []index.TokenFrequencies{}
	doc.VisitFields(func(field index.Field) {
		if field.Options().IsIndexed() {
			analyzed = append(analyzed, field.AnalyzedTokenFrequencies())
		}
	})

	rv := struct {
		Status   string      `json:"status"`
		Analyzed interface{} `json:"analyzed"`
	}{
		Status:   "ok",
		Analyzed: analyzed,
	}

	mustEncode(res, rv)

	return nil
}

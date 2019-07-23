//  Copyright (c) 2019 Couchbase, Inc.
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
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/mapping"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// AnalyzeDocHandler is a REST handler for analyzing documents against
// a given index.
type AnalyzeDocHandler struct {
	mgr *cbgt.Manager
}

func NewAnalyzeDocHandler(mgr *cbgt.Manager) *AnalyzeDocHandler {
	_ = http.NewServeMux()
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

	requestBody, err := ioutil.ReadAll(req.Body)
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
	cbftDoc, err = bdest.bleveDocConfig.BuildDocument(nil, req, defaultType)
	if err != nil {
		return err
	}

	idx, _, err := bindex.Advanced()
	if err != nil {
		return err
	}

	doc := document.NewDocument("dummy")
	err = bindex.Mapping().MapDocument(doc, cbftDoc)
	if err != nil {
		return err
	}

	ar := idx.Analyze(doc)

	rv := struct {
		Status   string      `json:"status"`
		Analyzed interface{} `json:"analyzed"`
	}{
		Status:   "ok",
		Analyzed: ar.Analyzed,
	}

	mustEncode(res, rv)

	return nil
}

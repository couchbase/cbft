//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"
)

func docIDLookup(req *http.Request) string {
	return muxVariableLookup(req, "docID")
}

func indexNameLookup(req *http.Request) string {
	return muxVariableLookup(req, "indexName")
}

// Return an indexAlias that represents all the PIndexes for the index.
//
// TODO: But, what if the index is an explicit, user-defined alias?
// Then, this should instead be an alias to indexes, and we should
// instead use table-lookup driven logic here to switch behavior based
// on index type.
func indexAlias(mgr *Manager, indexName, indexUUID string) (bleve.IndexAlias, error) {
	nodeDefs, _, err := CfgGetNodeDefs(mgr.Cfg(), NODE_DEFS_WANTED)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve wanted nodeDefs, err: %v", err)
	}

	// Returns true if the node has the "pindex" tag.
	nodeDoesPIndexes := func(nodeUUID string) (*NodeDef, bool) {
		for _, nodeDef := range nodeDefs.NodeDefs {
			if nodeDef.UUID == nodeUUID {
				if len(nodeDef.Tags) <= 0 {
					return nodeDef, true
				}
				for _, tag := range nodeDef.Tags {
					if tag == "pindex" {
						return nodeDef, true
					}
				}
			}
		}
		return nil, false
	}

	_, allPlanPIndexes, err := mgr.GetPlanPIndexes(false)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve allPlanPIndexes, err: %v", err)
	}

	planPIndexes, exists := allPlanPIndexes[indexName]
	if !exists || len(planPIndexes) <= 0 {
		return nil, fmt.Errorf("no planPIndexes for indexName: %s", indexName)
	}

	_, pindexes := mgr.CurrentMaps()

	selfUUID := mgr.UUID()
	_, selfDoesPIndexes := nodeDoesPIndexes(selfUUID)

	// TODO: need some abstractions to allow for non-bleve pindexes, perhaps.
	alias := bleve.NewIndexAlias()

build_alias_loop:
	for _, planPIndex := range planPIndexes {
		// First check whether this local node serves that planPIndex.
		if selfDoesPIndexes &&
			strings.Contains(planPIndex.NodeUUIDs[selfUUID], PLAN_PINDEX_NODE_READ) {
			localPIndex, exists := pindexes[planPIndex.Name]
			if exists &&
				localPIndex != nil &&
				localPIndex.Name == planPIndex.Name &&
				localPIndex.IndexType == "bleve" &&
				localPIndex.IndexName == indexName &&
				(indexUUID == "" || localPIndex.IndexUUID == indexUUID) {
				bindex, ok := localPIndex.Impl.(bleve.Index)
				if ok && bindex != nil {
					alias.Add(bindex)
					continue build_alias_loop
				}
			}
		}

		// Otherwise, look for a remote node that serves that planPIndex.
		//
		// TODO: We should favor the most up-to-date node rather than
		// the first one that we run into here?  But, perhaps the most
		// up-to-date node is also the most overloaded?
		for nodeUUID, nodeState := range planPIndex.NodeUUIDs {
			if nodeUUID != selfUUID {
				nodeDef, ok := nodeDoesPIndexes(nodeUUID)
				if ok &&
					strings.Contains(nodeState, PLAN_PINDEX_NODE_READ) {
					baseURL := "http://" + nodeDef.HostPort + "/api/pindex/" + planPIndex.Name
					// TODO: Propagate auth to bleve client.
					// TODO: Propagate consistency requirements to bleve client.
					alias.Add(&BleveClient{
						SearchURL:   baseURL + "/search",
						DocCountURL: baseURL + "/count",
					})
					continue build_alias_loop
				}
			}
		}

		return nil, fmt.Errorf("no node provides planPIndex: %#v", planPIndex)
	}

	return alias, nil
}

// ------------------------------------------------------------------

type ListIndexHandler struct {
	mgr *Manager
}

func NewListIndexHandler(mgr *Manager) *ListIndexHandler {
	return &ListIndexHandler{mgr: mgr}
}

func (h *ListIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexDefs, _, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		showError(w, req, "could not retrieve index defs", 500)
		return
	}

	rv := struct {
		Status    string     `json:"status"`
		IndexDefs *IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type GetIndexHandler struct {
	mgr *Manager
}

func NewGetIndexHandler(mgr *Manager) *GetIndexHandler {
	return &GetIndexHandler{mgr: mgr}
}

func (h *GetIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
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

	m := map[string]interface{}{}
	if indexDef.Schema != "" {
		if err := json.Unmarshal([]byte(indexDef.Schema), &m); err != nil {
			showError(w, req, "could not unmarshal mapping", 500)
			return
		}
	}

	rv := struct {
		Status       string                 `json:"status"`
		IndexDef     *IndexDef              `json:"indexDef"`
		IndexMapping map[string]interface{} `json:"indexMapping"`
	}{
		Status:       "ok",
		IndexDef:     indexDef,
		IndexMapping: m,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type CountHandler struct {
	mgr *Manager
}

func NewCountHandler(mgr *Manager) *CountHandler {
	return &CountHandler{mgr: mgr}
}

func (h *CountHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	alias, err := indexAlias(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("index alias: %v", err), 500)
		return
	}

	docCount, err := alias.DocCount()
	if err != nil {
		showError(w, req, fmt.Sprintf("error counting docs: %v", err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{
		Status: "ok",
		Count:  docCount,
	}
	mustEncode(w, rv)
}

// ---------------------------------------------------

type SearchHandler struct {
	mgr *Manager
}

func NewSearchHandler(mgr *Manager) *SearchHandler {
	return &SearchHandler{mgr: mgr}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := indexNameLookup(req)
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	indexUUID := req.FormValue("indexUUID")

	log.Printf("rest search request: %s", indexName)

	alias, err := indexAlias(h.mgr, indexName, indexUUID)
	if err != nil {
		showError(w, req, fmt.Sprintf("index alias: %v", err), 500)
		return
	}

	// read the request body
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		showError(w, req, fmt.Sprintf("error reading request body: %v", err), 400)
		return
	}

	log.Printf("rest search request body: %s", requestBody)

	// parse the request
	var searchRequest bleve.SearchRequest
	err = json.Unmarshal(requestBody, &searchRequest)
	if err != nil {
		showError(w, req, fmt.Sprintf("error parsing query: %v", err), 400)
		return
	}

	log.Printf("rest search parsed request %#v", searchRequest)

	// varlidate the query
	err = searchRequest.Query.Validate()
	if err != nil {
		showError(w, req, fmt.Sprintf("error validating query: %v", err), 400)
		return
	}

	// execute the query
	searchResponse, err := alias.Search(&searchRequest)
	if err != nil {
		showError(w, req, fmt.Sprintf("error executing query: %v", err), 500)
		return
	}

	// encode the response
	mustEncode(w, searchResponse)
}

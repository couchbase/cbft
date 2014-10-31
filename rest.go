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
	"net/http"

	bleveHttp "github.com/blevesearch/bleve/http"
	"github.com/gorilla/mux"
)

func NewManagerRESTRouter(mgr *Manager, staticDir string, mr *MsgRing) (*mux.Router, error) {
	// create a router to serve static files
	r := staticFileRouter(staticDir, []string{
		"/overview",
		"/search",
		"/indexes",
		"/analysis",
		"/monitor",
		"/logs",
	})

	// these are custom handlers for cbft
	r.Handle("/api/logs", NewGetLogsHandler(mr))
	r.Handle("/api/{indexName}", NewCreateIndexHandler(mgr)).Methods("PUT")
	r.Handle("/api/{indexName}", NewDeleteIndexHandler(mgr)).Methods("DELETE")

	// the rest are standard bleveHttp handlers
	r.Handle("/api", bleveHttp.NewListIndexesHandler()).Methods("GET")

	getIndexHandler := bleveHttp.NewGetIndexHandler()
	getIndexHandler.IndexNameLookup = indexNameLookup
	r.Handle("/api/{indexName}", getIndexHandler).Methods("GET")

	tags := mgr.Tags()
	if tags == nil || tags["queryer"] {
		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/{indexName}/_count", docCountHandler).Methods("GET")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = indexNameLookup
		docGetHandler.DocIDLookup = docIDLookup
		r.Handle("/api/{indexName}/{docID}", docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = indexNameLookup
		debugDocHandler.DocIDLookup = docIDLookup
		r.Handle("/api/{indexName}/{docID}/_debug", debugDocHandler).Methods("GET")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/{indexName}/_search", searchHandler).Methods("POST")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/{indexName}/_fields", listFieldsHandler).Methods("GET")
	}

	return r, nil
}

func muxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

func docIDLookup(req *http.Request) string {
	return muxVariableLookup(req, "docID")
}

func indexNameLookup(req *http.Request) string {
	return muxVariableLookup(req, "indexName")
}

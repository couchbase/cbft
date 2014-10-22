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
	bleveHttp "github.com/blevesearch/bleve/http"
	"github.com/gorilla/mux"
)

func NewManagerRESTRouter(mgr *Manager, staticDir string) (*mux.Router, error) {
	// create a router to serve static files
	r := staticFileRouter(staticDir, []string{
		"/overview",
		"/search",
		"/indexes",
		"/analysis",
		"/monitor",
	})

	// these are custom handlers for cbft
	r.Handle("/api/{indexName}", NewCreateIndexHandler(mgr)).Methods("PUT")
	r.Handle("/api/{indexName}", NewDeleteIndexHandler(mgr)).Methods("DELETE")

	// the rest are standard bleveHttp handlers
	r.Handle("/api/{indexName}", bleveHttp.NewGetIndexHandler()).Methods("GET")
	r.Handle("/api", bleveHttp.NewListIndexesHander()).Methods("GET")

	r.Handle("/api/{indexName}/_count", bleveHttp.NewDocCountHandler("")).Methods("GET")
	r.Handle("/api/{indexName}/{docID}", bleveHttp.NewDocGetHandler("")).Methods("GET")
	// r.Handle("/api/{indexName}/{docID}", bleveHttp.NewDocIndexHandler("")).Methods("PUT")
	// r.Handle("/api/{indexName}/{docID}", bleveHttp.NewDocDeleteHandler("")).Methods("DELETE")
	r.Handle("/api/{indexName}/{docID}/_debug", bleveHttp.NewDebugDocumentHandler("")).Methods("GET")

	r.Handle("/api/{indexName}/_search", bleveHttp.NewSearchHandler("")).Methods("POST")
	r.Handle("/api/{indexName}/_fields", bleveHttp.NewListFieldsHandler("")).Methods("GET")

	return r, nil
}

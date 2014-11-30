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

func NewManagerRESTRouter(mgr *Manager, staticDir string, mr *MsgRing) (
	*mux.Router, error) {
	// create a router to serve static files
	r := staticFileRouter(staticDir, []string{
		"/indexes",
		"/monitor",
		"/manage",
		"/logs",
		"/debug",
	})

	r.Handle("/api/log", NewGetLogHandler(mr)).Methods("GET")

	r.Handle("/api/index", NewListIndexHandler(mgr)).Methods("GET")
	r.Handle("/api/index/{indexName}", NewCreateIndexHandler(mgr)).Methods("PUT")
	r.Handle("/api/index/{indexName}", NewDeleteIndexHandler(mgr)).Methods("DELETE")
	r.Handle("/api/index/{indexName}", NewGetIndexHandler(mgr)).Methods("GET")

	if mgr.tagsMap == nil || mgr.tagsMap["queryer"] {
		r.Handle("/api/index/{indexName}/count", NewCountHandler(mgr)).Methods("GET")
		r.Handle("/api/index/{indexName}/query", NewQueryHandler(mgr)).Methods("POST")
	}

	// We use standard bleveHttp handlers for the /api/pindex-bleve endpoints.
	if mgr.tagsMap == nil || mgr.tagsMap["pindex"] {
		listIndexesHandler := bleveHttp.NewListIndexesHandler()
		r.Handle("/api/pindex", listIndexesHandler).Methods("GET")
		r.Handle("/api/pindex-bleve", listIndexesHandler).Methods("GET")

		getIndexHandler := bleveHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}",
			getIndexHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{indexName}",
			getIndexHandler).Methods("GET")

		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}/count",
			docCountHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{indexName}/count",
			docCountHandler).Methods("GET")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = indexNameLookup
		docGetHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex/{indexName}/doc/{docID}",
			docGetHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{indexName}/doc/{docID}",
			docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = indexNameLookup
		debugDocHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex/{indexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{indexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")

		// We have cbft purpose-built pindex query handler, instead of
		// just using bleveHttp, to handle auth and query consistency
		// across >1 pindex.
		r.Handle("/api/pindex/{indexName}/query",
			NewQueryPIndexHandler(mgr)).Methods("POST")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex-bleve/{indexName}/query",
			searchHandler).Methods("POST")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = indexNameLookup
		r.Handle("/api/pindex/{indexName}/fields",
			listFieldsHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{indexName}/fields",
			listFieldsHandler).Methods("GET")

		r.Handle("/api/feedStats",
			NewFeedStatsHandler(mgr)).Methods("GET")
	}

	r.Handle("/api/cfg", NewCfgGetHandler(mgr)).Methods("GET")
	r.Handle("/api/cfgRefresh", NewCfgRefreshHandler(mgr)).Methods("POST")

	r.Handle("/api/managerKick", NewManagerKickHandler(mgr)).Methods("POST")
	r.Handle("/api/managerMeta", NewManagerMetaHandler(mgr)).Methods("GET")

	return r, nil
}

func muxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

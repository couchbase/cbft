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
	//
	// TODO: Need to cleanly separate the /api/pindex and
	// /api/pindex-bleve endpoints.
	if mgr.tagsMap == nil || mgr.tagsMap["pindex"] {
		listIndexesHandler := bleveHttp.NewListIndexesHandler()
		r.Handle("/api/pindex", listIndexesHandler).Methods("GET")
		r.Handle("/api/pindex-bleve", listIndexesHandler).Methods("GET")

		getIndexHandler := bleveHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex/{pindexName}",
			getIndexHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{pindexName}",
			getIndexHandler).Methods("GET")

		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex/{pindexName}/count",
			docCountHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{pindexName}/count",
			docCountHandler).Methods("GET")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = pindexNameLookup
		docGetHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex/{pindexName}/doc/{docID}",
			docGetHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{pindexName}/doc/{docID}",
			docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = pindexNameLookup
		debugDocHandler.DocIDLookup = docIDLookup
		r.Handle("/api/pindex/{pindexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{pindexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")

		// We have a purpose-built pindex query handler, instead of
		// just using bleveHttp, to handle auth and query consistency.
		r.Handle("/api/pindex/{pindexName}/query",
			NewQueryPIndexHandler(mgr)).Methods("POST")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/query",
			searchHandler).Methods("POST")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = pindexNameLookup
		r.Handle("/api/pindex/{pindexName}/fields",
			listFieldsHandler).Methods("GET")
		r.Handle("/api/pindex-bleve/{pindexName}/fields",
			listFieldsHandler).Methods("GET")
	}

	r.Handle("/api/cfg", NewCfgGetHandler(mgr)).Methods("GET")
	r.Handle("/api/cfgRefresh", NewCfgRefreshHandler(mgr)).Methods("POST")

	r.Handle("/api/managerKick", NewManagerKickHandler(mgr)).Methods("POST")
	r.Handle("/api/managerMeta", NewManagerMetaHandler(mgr)).Methods("GET")

	r.Handle("/api/feedStats", NewFeedStatsHandler(mgr)).Methods("GET")

	return r, nil
}

func muxVariableLookup(req *http.Request, name string) string {
	return mux.Vars(req)[name]
}

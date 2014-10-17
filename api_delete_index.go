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
	"net/http"
	"os"

	"github.com/gorilla/mux"

	log "github.com/couchbaselabs/clog"
)

type DeleteIndexHandler struct {
	mgr *Manager
}

func NewDeleteIndexHandler(mgr *Manager) *DeleteIndexHandler {
	return &DeleteIndexHandler{mgr: mgr}
}

func (h *DeleteIndexHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// find the name of the index to delete
	indexName := mux.Vars(req)["indexName"]
	if indexName == "" {
		showError(w, req, "index name is required", 400)
		return
	}

	// try to stop the feed
	// TODO: should be future, multiple feeds
	feed := h.mgr.UnregisterFeed(indexName)
	if feed != nil {
		err := feed.Close()
		if err != nil {
			log.Printf("error closing stream: %v", err)
		}
		// not returning error here
		// because we still want to try and delete it
	}

	pindex := h.mgr.UnregisterPIndex(indexName)
	if pindex != nil {
		// TODO: if we closed the stream right now, then feed might
		// try writing to closed channel.
		// If there is multiple feeds going into one stream (fan-in)
		// then need to know how to count down to the final Close().
		// err := stream.Close()
		// if err != nil {
		// 	log.Printf("error closing pindex: %v", err)
		// }
		// not returning error here
		// because we still want to try and delete it
	}

	// TODO: what about any inflight queries or ops?

	// close the index
	// TODO: looks like the pindex be responsible for the final bleve.Close()
	// and actual subdirectory deletes?
	// indexToDelete := bleveHttp.UnregisterIndexByName(indexName)
	// if indexToDelete == nil {
	// 	showError(w, req, fmt.Sprintf("no such index '%s'", indexName), 404)
	// 	return
	//  }
	// indexToDelete.Close()
	pindex.BIndex().Close()

	// now delete it
	// TODO: should really send a msg to PIndex who's responsible for
	// actual file / subdir deletion.
	err := os.RemoveAll(h.mgr.IndexPath(indexName))
	if err != nil {
		showError(w, req, fmt.Sprintf("error deletoing index: %v", err), 500)
		return
	}

	rv := struct {
		Status string `json:"status"`
	}{
		Status: "ok",
	}
	mustEncode(w, rv)
}

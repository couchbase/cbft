//  Copyright (c) 2017 Couchbase, Inc.
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
	"net/http"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// FilteredListIndexHandler is a REST handler that lists indexes,
// similar to cbgt.rest.ListIndexHandler, but filters results based on
// cbauth permissions.
type FilteredListIndexHandler struct {
	mgr      definitionLookuper
	isCBAuth bool
}

func NewFilteredListIndexHandler(mgr *cbgt.Manager) *FilteredListIndexHandler {
	return &FilteredListIndexHandler{
		mgr:      mgr,
		isCBAuth: mgr != nil && mgr.Options()["authType"] == "cbauth",
	}
}

func (h *FilteredListIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexDefs, indexDefsByName, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		rest.PropagateError(w, nil, fmt.Sprintf("rest_list: filteredListIndex,"+
			" could not retrieve index defs, err: %v", err),
			http.StatusInternalServerError)
		return
	}

	if h.isCBAuth {
		creds, err := CBAuthWebCreds(req)
		if err != nil {
			rest.PropagateError(w, nil, fmt.Sprintf("rest_list: filteredListIndex,"+
				" cbauth.AuthWebCreds, err: %v", err), http.StatusForbidden)
			return
		}

		if indexDefs != nil && indexDefsByName != nil {
			allowSourceName := func(sourceName string) bool {
				perm := decoratePermStrings(
					"cluster.collection["+sourceName+"].fts!read",
					sourceName)

				allowed, err := CBAuthIsAllowed(creds, perm)

				return allowed && err == nil
			}

			// Copy fields, but start a separate, filtered IndexDefs map.
			out := *indexDefs
			out.IndexDefs = map[string]*cbgt.IndexDef{}
			var sourceNames []string
		OUTER:
			for indexName, indexDef := range indexDefsByName {
				if indexDef.Type == "fulltext-alias" {
					sourceNames, err =
						sourceNamesForAlias(indexName, indexDefsByName, 0)
					if err != nil {
						rest.PropagateError(w, nil,
							fmt.Sprintf("rest_list: filteredListIndex, sourceNamesForAlias,"+
								" err: %v", err), http.StatusInternalServerError)
						return
					}
				} else {
					sourceNames, err = getSourceNamesFromIndexDef(indexDef)
					if err != nil {
						rest.PropagateError(w, nil,
							fmt.Sprintf("rest_list: filteredListIndex, getSourceNamesFromIndexDef,"+
								" err: %v", err), http.StatusInternalServerError)
					}
				}

				for _, sourceName := range sourceNames {
					if !allowSourceName(sourceName) {
						continue OUTER
					}
				}

				out.IndexDefs[indexName] = indexDef
			}

			indexDefs = &out
		}
	}

	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	rest.MustEncode(w, rv)
}

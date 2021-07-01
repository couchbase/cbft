//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

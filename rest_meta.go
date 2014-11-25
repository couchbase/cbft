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

	"github.com/couchbaselabs/blance"
)

type ManagerMetaHandler struct {
	mgr *Manager
}

func NewManagerMetaHandler(mgr *Manager) *ManagerMetaHandler {
	return &ManagerMetaHandler{mgr: mgr}
}

type MetaDesc struct {
	Description string      `json:"description"`
	StartSample interface{} `json:"startSample"`
}

func (h *ManagerMetaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	structs := map[string]interface{}{
		"blanceHierarchyRules": &blance.HierarchyRules{},
		"blanceHierarchyRule":  &blance.HierarchyRule{},
		"indexDef":             &IndexDef{},
		"nodeDef":              &NodeDef{},
		"planParams":           &PlanParams{},
		"dcpFeedParams":        &DCPFeedParams{},
		"tapFeedParams":        &TAPFeedParams{},
	}

	// Key is sourceType, value is description.
	sourceTypes := map[string]string{}
	for sourceType, f := range feedTypes {
		if f.Public {
			sourceTypes[sourceType] = f.Description
		}
	}

	// Key is indexType, value is description.
	indexTypes := map[string]*MetaDesc{}
	for indexType, t := range pindexImplTypes {
		indexTypes[indexType] = &MetaDesc{
			Description: t.Description,
			StartSample: t.StartSample,
		}
	}

	mustEncode(w, struct {
		Status      string                 `json:"status"`
		Structs     map[string]interface{} `json:"structs"`
		SourceTypes map[string]string      `json:"sourceTypes"`
		IndexTypes  map[string]*MetaDesc   `json:"indexTypes"`
	}{
		Status:      "ok",
		Structs:     structs,
		SourceTypes: sourceTypes,
		IndexTypes:  indexTypes,
	})
}

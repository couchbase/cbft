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
	startSamples := map[string]interface{}{
		"planParams": &PlanParams{},
	}

	// Key is sourceType, value is description.
	sourceTypes := map[string]*MetaDesc{}
	for sourceType, f := range feedTypes {
		if f.Public {
			sourceTypes[sourceType] = &MetaDesc{
				Description: f.Description,
				StartSample: f.StartSample,
			}
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
		Status       string                 `json:"status"`
		StartSamples map[string]interface{} `json:"startSamples"`
		SourceTypes  map[string]*MetaDesc   `json:"sourceTypes"`
		IndexTypes   map[string]*MetaDesc   `json:"indexTypes"`
	}{
		Status:       "ok",
		StartSamples: startSamples,
		SourceTypes:  sourceTypes,
		IndexTypes:   indexTypes,
	})
}

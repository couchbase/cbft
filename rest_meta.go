//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"net/http"
)

type ManagerMetaHandler struct {
	mgr  *Manager
	meta map[string]RESTMeta
}

func NewManagerMetaHandler(mgr *Manager,
	meta map[string]RESTMeta) *ManagerMetaHandler {
	return &ManagerMetaHandler{mgr: mgr, meta: meta}
}

type MetaDesc struct {
	Description     string            `json:"description"`
	StartSample     interface{}       `json:"startSample"`
	StartSampleDocs map[string]string `json:"startSampleDocs"`
}

type MetaDescSource MetaDesc

type MetaDescIndex struct {
	MetaDesc

	CanCount bool `json:"canCount"`
	CanQuery bool `json:"canQuery"`

	QuerySample interface{} `json:"querySample"`
	QueryHelp   string      `json:"queryHelp"`
}

func (h *ManagerMetaHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	startSamples := map[string]interface{}{
		"planParams": &PlanParams{
			MaxPartitionsPerPIndex: 20,
		},
	}

	// Key is sourceType, value is description.
	sourceTypes := map[string]*MetaDescSource{}
	for sourceType, f := range FeedTypes {
		if f.Public {
			sourceTypes[sourceType] = &MetaDescSource{
				Description:     f.Description,
				StartSample:     f.StartSample,
				StartSampleDocs: f.StartSampleDocs,
			}
		}
	}

	// Key is indexType, value is description.
	indexTypes := map[string]*MetaDescIndex{}
	for indexType, t := range PIndexImplTypes {
		indexTypes[indexType] = &MetaDescIndex{
			MetaDesc: MetaDesc{
				Description: t.Description,
				StartSample: t.StartSample,
			},
			CanCount:    t.Count != nil,
			CanQuery:    t.Query != nil,
			QuerySample: t.QuerySample,
			QueryHelp:   t.QueryHelp,
		}
	}

	r := map[string]interface{}{
		"status":       "ok",
		"startSamples": startSamples,
		"sourceTypes":  sourceTypes,
		"indexNameRE":  INDEX_NAME_REGEXP,
		"indexTypes":   indexTypes,
		"refREST":      h.meta,
	}

	for _, t := range PIndexImplTypes {
		if t.MetaExtra != nil {
			t.MetaExtra(r)
		}
	}

	mustEncode(w, r)
}

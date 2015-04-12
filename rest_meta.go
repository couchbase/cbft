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

	bleveRegistry "github.com/blevesearch/bleve/registry"
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

	br := make(map[string]map[string][]string)

	t, i := bleveRegistry.AnalyzerTypesAndInstances()
	br["Analyzer"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.ByteArrayConverterTypesAndInstances()
	br["ByteArrayConverter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.CharFilterTypesAndInstances()
	br["CharFilter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.DateTimeParserTypesAndInstances()
	br["DateTimeParser"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.FragmentFormatterTypesAndInstances()
	br["FragmentFormatte"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.FragmenterTypesAndInstances()
	br["Fragmenter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.HighlighterTypesAndInstances()
	br["Highlighter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.KVStoreTypesAndInstances()
	br["KVStore"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.TokenFilterTypesAndInstances()
	br["TokenFilter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.TokenMapTypesAndInstances()
	br["TokenMap"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.TokenizerTypesAndInstances()
	br["Tokenizer"] = map[string][]string{"types": t, "instances": i}

	mustEncode(w, struct {
		Status       string                         `json:"status"`
		StartSamples map[string]interface{}         `json:"startSamples"`
		SourceTypes  map[string]*MetaDesc           `json:"sourceTypes"`
		IndexNameRE  string                         `json:"indexNameRE"`
		IndexTypes   map[string]*MetaDesc           `json:"indexTypes"`
		RefREST      map[string]RESTMeta            `json:"refREST"`
		RegBleve     map[string]map[string][]string `json:"regBleve"`
	}{
		Status:       "ok",
		StartSamples: startSamples,
		SourceTypes:  sourceTypes,
		IndexNameRE:  INDEX_NAME_REGEXP,
		IndexTypes:   indexTypes,
		RefREST:      h.meta,
		RegBleve:     br,
	})
}

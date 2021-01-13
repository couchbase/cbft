//  Copyright (c) 2020 Couchbase, Inc.
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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
)

func buildMapping(mappings []string, defaultMapping bool) *mapping.IndexMappingImpl {
	im := mapping.NewIndexMapping()
	for _, m := range mappings {
		childMapping := mapping.NewDocumentMapping()
		im.AddDocumentMapping(m, childMapping)
	}
	im.DefaultMapping.Enabled = defaultMapping
	return im
}

func TestScopeCollectionTypeMappings(t *testing.T) {
	tests := []struct {
		title          string
		skipMapping    bool
		defaultMapping bool
		im             *mapping.IndexMappingImpl
		scope          string
		colls          []string
		tmappings      []string
	}{
		{
			title:          "simple collection index mapping",
			skipMapping:    false,
			defaultMapping: false,
			im:             buildMapping([]string{"scope1.collection1.beer"}, false),
			scope:          "scope1",
			colls:          []string{"collection1"},
			tmappings:      []string{"beer"},
		},
		{
			title:          "single collection, multiple type mapping",
			skipMapping:    false,
			defaultMapping: false,
			im:             buildMapping([]string{"scope1.collection1.beer", "scope1.collection1.brewery"}, false),
			scope:          "scope1",
			colls:          []string{"collection1", "collection1"},
			tmappings:      []string{"beer", "brewery"},
		},
		{
			title:          "multi collection, index type mappings",
			skipMapping:    false,
			defaultMapping: false,
			im:             buildMapping([]string{"scope1.collection1.beer", "scope1.collection2.airport"}, false),
			scope:          "scope1",
			colls:          []string{"collection1", "collection2"},
			tmappings:      []string{"airport", "beer"},
		},
		{
			title:          "single collection, single type mapping, skipmapping",
			skipMapping:    true,
			defaultMapping: false,
			im:             buildMapping([]string{"scope1.collection1.beer", "scope1.collection1.airport"}, false),
			scope:          "scope1",
			colls:          []string{"collection1"},
			tmappings:      nil,
		},
		{
			title:          "single collection, multiple type mapping, skipmapping",
			skipMapping:    true,
			defaultMapping: false,
			im:             buildMapping([]string{"scope1.collection1.beer", "scope1.collection2.airport"}, false),
			scope:          "scope1",
			colls:          []string{"collection1", "collection2"},
			tmappings:      nil,
		},
		{
			title:          "multi collection, multi type mappings",
			skipMapping:    true,
			defaultMapping: false,
			im:             buildMapping([]string{"scope1.collection1.beer", "scope1.collection2.airport"}, false),
			scope:          "scope1",
			colls:          []string{"collection1", "collection2"},
			tmappings:      nil,
		},
		{
			title:          "multi scope mappings",
			skipMapping:    false,
			defaultMapping: true,
			im:             buildMapping([]string{"scope1.collection1.beer"}, true),
			scope:          "",
			colls:          nil,
			tmappings:      nil,
		},
		{
			title:          "normal type mappings",
			skipMapping:    false,
			defaultMapping: false,
			im:             buildMapping([]string{"beer", "brewery"}, false),
			scope:          "_default",
			colls:          []string{"_default", "_default"},
			tmappings:      []string{"beer", "brewery"},
		},
		{
			title:          "normal type mappings with skip",
			skipMapping:    true,
			defaultMapping: false,
			im:             buildMapping([]string{"beer", "brewery"}, false),
			scope:          "_default",
			colls:          []string{"_default"},
			tmappings:      nil,
		},
	}

	for _, test := range tests {
		scope, colls, tmappings, err := getScopeCollTypeMappings(test.im, test.skipMapping)
		if err != nil && !test.defaultMapping {
			t.Errorf("test %s failed, err: %v", test.title, err)
		}
		if scope != test.scope {
			t.Errorf("expected scope '%s' got '%s' for '%s'", test.scope, scope, test.title)
		}
		sort.Strings(colls)
		sort.Strings(test.colls)
		if !reflect.DeepEqual(colls, test.colls) {
			t.Errorf("expected collections '%v' got '%v' for '%s'", test.colls, colls, test.title)
		}
		sort.Strings(tmappings)
		sort.Strings(test.tmappings)
		if !reflect.DeepEqual(tmappings, test.tmappings) {
			t.Errorf("expected type mappings '%v' got '%v' for '%s'", test.tmappings, tmappings, test.title)
		}
	}
}

func TestScopeCollectionTypeMappingsErrorCases(t *testing.T) {
	tests := []struct {
		title       string
		skipMapping bool
		im          *mapping.IndexMappingImpl
		errText     string
	}{
		{
			title:       "simple collection index mapping with default mapping enabled",
			skipMapping: false,
			im:          buildMapping([]string{"scope1.collection1.beer"}, true),
			errText:     "collection_utils: multiple scopes found",
		},
		{
			title:       "multiple scope in type mappings",
			skipMapping: true,
			im:          buildMapping([]string{"scope1.collection1.beer", "scope2.collection1.brewery"}, true),
			errText:     "collection_utils: multiple scopes found",
		},
	}

	for _, test := range tests {
		_, _, _, err := getScopeCollTypeMappings(test.im, test.skipMapping)
		if err == nil || !strings.HasPrefix(err.Error(), test.errText) {
			t.Errorf("err %v expected, but test %s passed", test.errText, test.title)
		}
	}
}

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
	"encoding/json"
	"reflect"
	"sort"
	"strings"

	"testing"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/mapping"
	"github.com/couchbase/cbgt"
)

func TestCheckSourceNameMatchesFilters(t *testing.T) {
	tests := []struct {
		indexDef         *cbgt.IndexDef
		bucketLevel      bool
		filterRules      string
		expBucketFilters []string
		expScopeFilters  []string
		expColFilters    []string
		expSourceNames   []string
		match            bool
	}{
		//  matching empty filter for index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "", false, nil, []string{"beer", "brewery"}),
			filterRules:      "",
			expSourceNames:   []string{"beer-sample._default._default"},
			expBucketFilters: nil, expScopeFilters: nil,
			expColFilters: nil, match: true},
		// non matching $bucket filter for index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "", false, nil, []string{"beer", "brewery"}),
			filterRules:      "travel-sample",
			expSourceNames:   []string{"beer-sample._default._default"},
			expBucketFilters: []string{"travel-sample"}, expScopeFilters: nil,
			expColFilters: nil, match: false},
		// matching $bucket filter for index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "", false, nil, []string{"beer", "brewery"}),
			filterRules:      "beer-sample",
			expSourceNames:   []string{"beer-sample._default._default"},
			expBucketFilters: []string{"beer-sample"}, expScopeFilters: nil,
			expColFilters: nil, match: true},
		// matching $bucket.$scope filter for index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "", false, nil, []string{"beer", "brewery"}),
			filterRules:      "beer-sample._default",
			expSourceNames:   []string{"beer-sample._default._default"},
			expBucketFilters: nil, expScopeFilters: []string{"beer-sample._default"},
			expColFilters: nil, match: true},
		// matching $bucket filter for multi collection index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection2"}, []string{"beer", "brewery"}),
			filterRules:      "beer-sample",
			expSourceNames:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"},
			expBucketFilters: []string{"beer-sample"}, expScopeFilters: nil,
			expColFilters: nil, match: true},
		// matching $bucket,$bucket.$scope filters for multi collection index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection2"}, []string{"beer", "brewery"}),
			filterRules:      "travel-sample,beer-sample.scope1",
			expSourceNames:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"},
			expBucketFilters: []string{"travel-sample"}, expScopeFilters: []string{"beer-sample.scope1"},
			expColFilters: nil, match: true},
		// non matching $bucket,$bucket.$scope filters for multi collection index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection2"}, []string{"beer", "brewery"}),
			filterRules:      "travel-sample,beer-sample.scope2",
			expSourceNames:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"},
			expBucketFilters: []string{"travel-sample"}, expScopeFilters: []string{"beer-sample.scope2"},
			expColFilters: nil, match: false},
		// non matching $bucket,$bucket.$scope.$collection filters for multi collection index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection200"}, []string{"beer", "brewery"}),
			filterRules:      "travel-sample,beer-sample.scope1.collection1",
			expSourceNames:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection200"},
			expBucketFilters: []string{"travel-sample"}, expScopeFilters: nil,
			expColFilters: []string{"beer-sample.scope1.collection1"}, match: false},
		// matching multiple $bucket.$scope.$collection filters for multi collection index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection200"}, []string{"beer", "brewery"}),
			filterRules:      "beer-sample.scope1.collection1,beer-sample.scope1.collection200",
			expSourceNames:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection200"},
			expBucketFilters: nil, expScopeFilters: nil,
			expColFilters: []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection200"}, match: true},
		// test bucket level filters
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection200"}, []string{"beer", "brewery"}),
			filterRules:      "scope1",
			bucketLevel:      true,
			expSourceNames:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection200"},
			expBucketFilters: nil, expScopeFilters: []string{"beer-sample.scope1"},
			expColFilters: nil, match: true},
	}

	for i, test := range tests {
		sns := parseSourceNamesFromIndexDefs(test.indexDef)
		if !reflect.DeepEqual(sns, test.expSourceNames) {
			t.Errorf("test: %d, expected sourceNames: %v got %v", i, test.expSourceNames, sns)
		}
		var bFilters, sFilters, cFilters []string
		if !test.bucketLevel {
			bFilters, sFilters, cFilters = parseBackupFilters(test.filterRules, "")
		} else {
			_, sFilters, cFilters = parseBackupFilters(test.filterRules, test.indexDef.SourceName)
		}

		if !reflect.DeepEqual(bFilters, test.expBucketFilters) {
			t.Errorf("test: %d, expected bucketFilters: %v got %v", i, test.expBucketFilters, bFilters)
		}

		if !reflect.DeepEqual(sFilters, test.expScopeFilters) {
			t.Errorf("test: %d, expected scopeFilters: %v got %v", i, test.expScopeFilters, sFilters)
		}
		if !reflect.DeepEqual(cFilters, test.expColFilters) {
			t.Errorf("test: %d, expected collectionFilters: %v got %v", i, test.expColFilters, cFilters)
		}
		actual := checkSourceNameMatchesFilters(sns, bFilters, sFilters, cFilters)
		if actual != test.match {
			t.Errorf("test: %d, expected %t got %t", i, test.match, actual)
		}
	}
}

func TestCheckSourceNameMatchesFiltersBucketLevel(t *testing.T) {
	tests := []struct {
		indexDef        *cbgt.IndexDef
		filterRules     string
		bucketName      string
		expScopeFilters []string
		expColFilters   []string
		expSourceNames  []string
		match           bool
	}{
		// empty filters for index definition for bucketlevel filtering.
		{indexDef: testIndexDefn(t, "beer-sample", "", false, nil, []string{"beer", "brewery"}),
			filterRules:     "",
			bucketName:      "beer-sample",
			expSourceNames:  []string{"beer-sample._default._default"},
			expScopeFilters: nil,
			expColFilters:   nil, match: true},
		// matching $scope filter for index definition for bucketlevel filtering.
		{indexDef: testIndexDefn(t, "beer-sample", "", false, nil, []string{"beer", "brewery"}),
			filterRules:     "_default",
			bucketName:      "beer-sample",
			expSourceNames:  []string{"beer-sample._default._default"},
			expScopeFilters: []string{"beer-sample._default"},
			expColFilters:   nil, match: true},
		// non matching $scope.$collection filter for index definition for bucketlevel filtering.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection2"}, []string{"beer", "brewery"}),
			filterRules:     "_scope1.collection5",
			bucketName:      "beer-sample",
			expSourceNames:  []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"},
			expScopeFilters: nil,
			expColFilters:   []string{"beer-sample._scope1.collection5"}, match: false},
		// matching $scope filter for multi collection index definition for bucketlevel filtering.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection2"}, []string{"beer", "brewery"}),
			filterRules:     "scope1",
			bucketName:      "beer-sample",
			expSourceNames:  []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"},
			expScopeFilters: []string{"beer-sample.scope1"},
			expColFilters:   nil, match: true},
		// matching mulit $scope.$collection filters for multi collection index definition for bucketlevel filtering.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection2"}, []string{"beer", "brewery"}),
			filterRules:     "scope1.collection1,scope1.collection2",
			bucketName:      "beer-sample",
			expSourceNames:  []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"},
			expScopeFilters: nil,
			expColFilters:   []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection2"}, match: true},
		// non matching $scope.$collection filter for multi collection index definition for bucketlevel filtering.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1", "collection200"}, []string{"beer", "brewery"}),
			filterRules:     "scope1.collection1",
			bucketName:      "beer-sample",
			expSourceNames:  []string{"beer-sample.scope1.collection1", "beer-sample.scope1.collection200"},
			expScopeFilters: nil,
			expColFilters:   []string{"beer-sample.scope1.collection1"}, match: false},
	}

	for i, test := range tests {
		sns := parseSourceNamesFromIndexDefs(test.indexDef)
		if !reflect.DeepEqual(sns, test.expSourceNames) {
			t.Errorf("test: %d, expected sourceNames: %v got %v", i, test.expSourceNames, sns)
		}
		_, sFilters, cFilters := parseBackupFilters(test.filterRules, test.bucketName)
		if !reflect.DeepEqual(sFilters, test.expScopeFilters) {
			t.Errorf("test: %d, expected scopeFilters: %v got %v", i, test.expScopeFilters, sFilters)
		}
		if !reflect.DeepEqual(cFilters, test.expColFilters) {
			t.Errorf("test: %d, expected collectionFilters: %v got %v", i, test.expColFilters, cFilters)
		}
		actual := checkSourceNameMatchesFilters(sns, nil, sFilters, cFilters)
		if actual != test.match {
			t.Errorf("test: %d, expected %t got %t", i, test.match, actual)
		}
	}
}

func testIndexDefn(t *testing.T, bucketName, scopeName string, colMode bool,
	collName, typs []string) *cbgt.IndexDef {
	indexDefn := &cbgt.IndexDef{
		SourceName: bucketName,
	}
	bp := NewBleveParams()
	var im *mapping.IndexMappingImpl
	var ok bool
	if im, ok = bp.Mapping.(*mapping.IndexMappingImpl); ok {
		im.TypeMapping = make(map[string]*mapping.DocumentMapping, 1)
		im.DefaultMapping.Enabled = false
	}
	if colMode {
		bp.DocConfig.Mode = "scope.collection.type_field"
	} else {
		bp.DocConfig.Mode = "type_field"
	}

	for i, cn := range collName {
		typ := scopeName + "." + cn + "." + typs[i]
		im.TypeMapping[typ] = bleve.NewDocumentMapping()
	}

	pBytes, err := json.Marshal(bp)
	if err != nil {
		t.Errorf("testIndexDefn, json err: %v", err)
	}
	indexDefn.Params = string(pBytes)
	return indexDefn
}

func TestRemapTypeMappings(t *testing.T) {
	tests := []struct {
		typMappings   map[string]*mapping.DocumentMapping
		remapRules    string
		bucketName    string
		bucketLevel   bool
		expTypNames   []string
		expBucketName string
	}{
		// remap scopes in a single collection type mapping
		{
			typMappings:   createTypeMappings([]string{"scope1.collection1.brewery"}),
			remapRules:    "beer-sample.scope1:beer-sample.scope2",
			bucketName:    "beer-sample",
			bucketLevel:   false,
			expTypNames:   []string{"scope2.collection1.brewery"},
			expBucketName: "beer-sample",
		},
		// remap scopes in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"scope1.collection1.brewery", "scope1.collection2.beer"}),
			remapRules:    "beer-sample.scope1:beer-sample.scope2",
			bucketName:    "beer-sample",
			bucketLevel:   false,
			expTypNames:   []string{"scope2.collection1.brewery", "scope2.collection2.beer"},
			expBucketName: "beer-sample",
		},
		// remap scopes in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"scope1.collection1.brewery", "scope1.collection2.beer"}),
			remapRules:    "scope1:scope2",
			bucketName:    "beer-sample",
			bucketLevel:   true,
			expTypNames:   []string{"scope2.collection1.brewery", "scope2.collection2.beer"},
			expBucketName: "beer-sample",
		},
		// remap collections in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"scope1.collection1.brewery", "scope1.collection2.beer"}),
			remapRules:    "scope1.collection1:scope1.collection3",
			bucketName:    "beer-sample",
			bucketLevel:   true,
			expTypNames:   []string{"scope1.collection2.beer", "scope1.collection3.brewery"},
			expBucketName: "beer-sample",
		},
		// remap scope and collections in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"scope1.collection1.brewery", "scope1.collection2.beer"}),
			remapRules:    "scope1.collection1:scope2.collection2,scope1.collection2:scope2.collection1",
			bucketName:    "beer-sample",
			bucketLevel:   true,
			expTypNames:   []string{"scope2.collection1.beer", "scope2.collection2.brewery"},
			expBucketName: "beer-sample",
		},
		// remap bucket, scope and collections in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"scope1.collection1.brewery", "scope1.collection2.beer"}),
			remapRules:    "beer-sample.scope1.collection1:travel-sample.scope2.collection2,beer-sample.scope1.collection2:travel-sample.scope2.collection1",
			bucketName:    "beer-sample",
			bucketLevel:   false,
			expTypNames:   []string{"scope2.collection1.beer", "scope2.collection2.brewery"},
			expBucketName: "travel-sample",
		},
		// remap bucket and the default scope/collections in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"brewery", "beer"}),
			remapRules:    "beer-sample._default._default:travel-sample.scope1.collection1",
			bucketName:    "beer-sample",
			bucketLevel:   false,
			expTypNames:   []string{"scope1.collection1.beer", "scope1.collection1.brewery"},
			expBucketName: "travel-sample",
		},
		// remap bucket and the explicit default scope/collections in a multi collection type mappings
		{
			typMappings:   createTypeMappings([]string{"_default._default.brewery", "_default._default.beer"}),
			remapRules:    "beer-sample._default._default:travel-sample.scope1.collection1",
			bucketName:    "beer-sample",
			bucketLevel:   false,
			expTypNames:   []string{"scope1.collection1.beer", "scope1.collection1.brewery"},
			expBucketName: "travel-sample",
		},
	}
	for i, test := range tests {
		rmr, err := parseMappingParams(test.remapRules)
		if err != nil {
			t.Errorf("test %d parseMappingParams failed, err: %v", i, err)
		}
		typMappings, bucketName, err := remapTypeMappings(test.typMappings, rmr, "testIndex", test.bucketName, test.bucketLevel)
		if err != nil {
			t.Errorf("test %d remapTypeMappings failed, err: %v", i, err)
		}
		actualTypNames := getTypeNames(typMappings)
		if !reflect.DeepEqual(actualTypNames, test.expTypNames) {
			t.Errorf("test %d remapTypeMappings failed, expected remapped types: %v, got: %v", i, test.expTypNames, actualTypNames)
		}
		if test.expBucketName != bucketName {
			t.Errorf("test %d remapTypeMappings failed, expected bucketName: %s, got: %s", i, test.expBucketName, bucketName)
		}
	}

}

func TestRemapTypeMappingsErrCases(t *testing.T) {
	tests := []struct {
		typMappings    map[string]*mapping.DocumentMapping
		remapRules     string
		bucketName     string
		bucketLevel    bool
		errDescription string
	}{
		// remap fails upon remap conflicts with an existing mapping.
		{
			typMappings:    createTypeMappings([]string{"scope1.collection1.beer", "scope1.collection2.beer"}),
			remapRules:     "beer-sample.scope1.collection1:beer-sample.scope1.collection2",
			bucketName:     "beer-sample",
			bucketLevel:    false,
			errDescription: "conflicts the existing type mappings for:",
		},
		// remap fails upon remap bucket.scope.collection rules within bucket scope. (it is supported for cluster level)
		{
			typMappings:    createTypeMappings([]string{"scope1.collection1.beer", "scope1.collection2.beer"}),
			remapRules:     "beer-sample.scope1.collection1:beer-sample.scope1.collection2",
			bucketName:     "beer-sample",
			bucketLevel:    true,
			errDescription: "Invalid remap rules:",
		},
	}

	for i, test := range tests {
		rmr, err := parseMappingParams(test.remapRules)
		if err != nil {
			t.Errorf("test %d parseMappingParams failed, err: %v", i, err)
		}
		typMappings, bucketName, err := remapTypeMappings(test.typMappings, rmr, "testIndex", test.bucketName, test.bucketLevel)
		if err == nil || !strings.Contains(err.Error(), test.errDescription) {
			t.Errorf("test %d remapTypeMappings expected to fail, but got :%v %s", i, typMappings, bucketName)
		}
	}
}

func createTypeMappings(typNames []string) map[string]*mapping.DocumentMapping {
	tm := make(map[string]*mapping.DocumentMapping, len(typNames))
	for _, tname := range typNames {
		tm[tname] = mapping.NewDocumentMapping()
	}
	return tm
}

func getTypeNames(tm map[string]*mapping.DocumentMapping) []string {
	var rv []string
	for tname, _ := range tm {
		rv = append(rv, tname)
	}
	sort.Strings(rv)
	return rv
}

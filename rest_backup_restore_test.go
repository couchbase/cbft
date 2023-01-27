//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"

	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
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
		// non-matching $bucket.$scope.$collection filter for index definition.
		{indexDef: testIndexDefn(t, "beer-sample", "scope1", true, []string{"collection1"}, nil),
			bucketLevel:      true,
			filterRules:      "scope1.collection2",
			expSourceNames:   []string{"beer-sample.scope1.collection1"},
			expBucketFilters: nil, expScopeFilters: nil,
			expColFilters: []string{"beer-sample.scope1.collection2"}, match: false},
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
		typ := scopeName + "." + cn
		if i < len(typs) {
			typ = typ + "." + typs[i]
		}

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

func TestRemapIndexDefinions(t *testing.T) {
	tests := []struct {
		indexDefBytes   []byte
		mappingRules    map[string]string
		resIndexName    string
		resMappings     []string
		testDescription string
	}{
		// verify an index name and mappings remap for the keyspaced index.
		{
			indexDefBytes: []byte(`{"uuid":"34483a9bd6044df7","indexDefs":{"travel-sample.inventory.DemoIndex":
			{"type":"fulltext-index","name":"travel-sample.inventory.DemoIndex","uuid":"","sourceType":
			"gocbcore","sourceName":"travel-sample","planParams":{"maxPartitionsPerPIndex":1024,"indexPartitions":1},
			"params":{"doc_config":{"docid_prefix_delim":"","docid_regexp":"","mode":"scope.collection.type_field",
			"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard","default_datetime_parser":
			"dateTimeOptional","default_field":"_all","default_mapping":{"dynamic":true,"enabled":false},
			"default_type":"_default","docvalues_dynamic":false,"index_dynamic":true,"store_dynamic":false,
			"type_field":"_type","types":{"inventory.hotel":{"dynamic":true,"enabled":true},"inventory.landmark":
			{"dynamic":true,"enabled":true}}},"store":{"indexType":"scorch","segmentVersion":15}},"sourceParams":{}}},
			"implVersion":"5.6.0"}`),
			mappingRules: map[string]string{
				"travel-sample.inventory": "beer-sample.brewery",
			},
			resIndexName:    "beer-sample.brewery.DemoIndex",
			resMappings:     []string{"brewery.hotel", "brewery.landmark"},
			testDescription: "remap an index defn with both bucket and scope name remapped",
		},
		// verify an index name and mappings remap for the restored index.
		{
			indexDefBytes: []byte(`{"uuid":"34483a9bd6044df7","indexDefs":{"DemoIndex":
			{"type":"fulltext-index","name":"DemoIndex","uuid":"","sourceType":
			"gocbcore","sourceName":"travel-sample","planParams":{"maxPartitionsPerPIndex":1024,"indexPartitions":1},
			"params":{"doc_config":{"docid_prefix_delim":"","docid_regexp":"","mode":"scope.collection.type_field",
			"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard","default_datetime_parser":
			"dateTimeOptional","default_field":"_all","default_mapping":{"dynamic":true,"enabled":false},
			"default_type":"_default","docvalues_dynamic":false,"index_dynamic":true,"store_dynamic":false,
			"type_field":"_type","types":{"inventory.hotel":{"dynamic":true,"enabled":true},"inventory.landmark":
			{"dynamic":true,"enabled":true}}},"store":{"indexType":"scorch","segmentVersion":15}},"sourceParams":{}}},
			"implVersion":"5.6.0"}`),
			mappingRules: map[string]string{
				"travel-sample.inventory": "beer-sample.brewery",
			},
			resIndexName:    "DemoIndex",
			resMappings:     []string{"brewery.hotel", "brewery.landmark"},
			testDescription: "remap an index defn with both bucket and scope name remapped",
		},
		// verify an index name and mappings remap to the default index.
		{
			indexDefBytes: []byte(`{"uuid":"34483a9bd6044df7","indexDefs":{"travel-sample.inventory.DemoIndex":
			{"type":"fulltext-index","name":"travel-sample.inventory.DemoIndex","uuid":"","sourceType":
			"gocbcore","sourceName":"travel-sample","planParams":{"maxPartitionsPerPIndex":1024,"indexPartitions":1},
			"params":{"doc_config":{"docid_prefix_delim":"","docid_regexp":"","mode":"scope.collection.type_field",
			"type_field":"type"},"mapping":{"analysis":{},"default_analyzer":"standard","default_datetime_parser":
			"dateTimeOptional","default_field":"_all","default_mapping":{"dynamic":true,"enabled":false},
			"default_type":"_default","docvalues_dynamic":false,"index_dynamic":true,"store_dynamic":false,
			"type_field":"_type","types":{"inventory.hotel":{"dynamic":true,"enabled":true},"inventory.landmark":
			{"dynamic":true,"enabled":true}}},"store":{"indexType":"scorch","segmentVersion":15}},"sourceParams":{}}},
			"implVersion":"5.6.0"}`),
			mappingRules: map[string]string{
				"travel-sample.inventory": "beer-sample._default",
			},
			resIndexName:    "beer-sample._default.DemoIndex",
			resMappings:     []string{"_default.hotel", "_default.landmark"},
			testDescription: "remap an index defn with both bucket and scope name remapped",
		},
	}

	versionTracker = &clusterVersionTracker{}
	versionTracker.version, _ = cbgt.CompatibilityVersion(FeatureScopedIndexNamesVersion)
	versionTracker.clusterVersion = versionTracker.version
	versionTracker.compatibleFeatures = make(map[string]struct{})

	for i, test := range tests {
		var indexDefs cbgt.IndexDefs
		err := json.Unmarshal(test.indexDefBytes, &indexDefs)
		if err != nil {
			t.Errorf("test %d, json err: %v", i, err)
		}

		resIndexDefs, err := remapIndexDefinitions(&indexDefs, test.mappingRules, "", true)
		if err != nil {
			t.Errorf("test %d, remapIndexDefinitions failed, err: %v", i, err)
		}

		if len(resIndexDefs.IndexDefs) != 1 {
			t.Errorf("test %d remapIndexDefinitions, multiple index defns found: %+v",
				i, resIndexDefs.IndexDefs)
		}

		var res *cbgt.IndexDef
		var found bool
		if res, found = resIndexDefs.IndexDefs[test.resIndexName]; !found {
			t.Errorf("test %d remapIndexDefinitions, no index defn found for: %s, %+v",
				i, test.resIndexName, resIndexDefs.IndexDefs)
		}

		for _, mappingName := range test.resMappings {
			if !strings.Contains(res.Params, mappingName) {
				t.Errorf("test %d remapIndexDefinitions, no mapping found for: %s",
					i, mappingName)
			}
		}
	}

	versionTracker = nil
}

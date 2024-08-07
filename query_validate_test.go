//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
)

func TestQueryValidationAgainstTailoredIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": true,
				"dynamic": false,
				"properties": {
					"content": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"analyzer": "en",
							"index": true,
							"name": "content",
							"type": "text"
						}
						]
					},
					"num": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"index": true,
							"name": "num",
							"type": "number"
						}
						]
					},
					"geo": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"index": true,
							"name": "geo",
							"type": "geopoint"
						}
						]
					}
				}
			}
		}
	}`
	m, emptyDir, err := createManagerAndIntroduceIndex(t, "foo", indexParams)
	if err != nil {
		cbgt.DataSourceUUID = prevDataSourceUUID
		t.Fatal(err)
	}
	defer func() {
		m.Stop()
		os.RemoveAll(emptyDir)
		cbgt.DataSourceUUID = prevDataSourceUUID
	}()

	tests := []struct {
		sr               []byte
		expectedResponse error
	}{
		{
			// match query on wrong field
			sr: []byte(`{
				"query": {"match": "x", "field": "wrongContent"}
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// match
			sr: []byte(`{
				"query": {"match": "x", "field": "content"}
			}`),
			expectedResponse: nil,
		},
		{
			// numeric-range OR date-range
			sr: []byte(`{
				"query": {"disjuncts": [
					{"min": 1, "max": 2, "field": "num"},
					{"start": "<datetime>", "end": "<datetime>", "field": "content"}
				]}
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// wildcard
			sr: []byte(`{
				"query": {"wildcard": "x", "field": "content"}
			}`),
			expectedResponse: nil,
		},
		{
			// numeric range
			sr: []byte(`{
				"query": {"min": 1, "max": 2, "field": "num"}
			}`),
			expectedResponse: nil,
		},
		{
			//term range AND numeric
			sr: []byte(`{
				"query": {"conjuncts": [
					{"min": "term1", "max": "term2", "field": "content"},
					{"min": 1, "max": 2, "field": "num"}
				]}
			}`),
			expectedResponse: nil,
		},
		{
			// geo
			sr: []byte(`{
				"query": {
					"location": {
						"lon": -2.235143,
						"lat": 53.482358
					},
					"distance": "100mi",
					"field": "geo"
				}
			}`),
			expectedResponse: nil,
		},
	}

	for testI, test := range tests {
		var sr *bleve.SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		err = validateSearchRequestAgainstIndex(m, "foo", "", nil, sr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

func TestQueryValidationAgainstDynamicIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": true,
				"dynamic": true
			}
		}
	}`
	m, emptyDir, err := createManagerAndIntroduceIndex(t, "foo", indexParams)
	if err != nil {
		cbgt.DataSourceUUID = prevDataSourceUUID
		t.Fatal(err)
	}
	defer func() {
		m.Stop()
		os.RemoveAll(emptyDir)
		cbgt.DataSourceUUID = prevDataSourceUUID
	}()

	tests := []struct {
		sr               []byte
		expectedResponse error
	}{
		{
			// match
			sr: []byte(`{
				"query": {"match": "x", "field": "wrongContent"}
			}`),
			expectedResponse: nil,
		},
		{
			// term OR date-range
			sr: []byte(`{
				"query": {"disjuncts": [
					{"term": "x", "field": "content"},
					{"start": "<datetime>", "end": "<datetime>", "field": "content"}
				]}
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// boolean over basic types
			sr: []byte(`{
				"query": {
					"must": {"conjuncts": [
						{"min": "term1", "max": "term2", "field": "A"},
						{"min": 1, "max": 2, "field": "B"}
					]},
					"should": {"disjuncts": [
						{"match": "x", "field": "C"}
					]},
					"must_not": {"disjuncts": [
						{"bool": true, "field": "D"}
					]}
				}
			}`),
			expectedResponse: nil,
		},
		{
			// geo
			sr: []byte(`{
				"query": {
					"location": {
						"lon": -2.235143,
						"lat": 53.482358
					},
					"distance": "100mi",
					"field": "geo"
				}
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
	}

	for testI, test := range tests {
		var sr *SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		bsr, err := sr.ConvertToBleveSearchRequest()
		if err != nil {
			t.Fatal(err)
		}
		err = validateSearchRequestAgainstIndex(m, "foo", "", sr.Collections, bsr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

func TestQueryValidationAgainstNestedDynamicIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"dynamic": false,
				"enabled": true,
				"properties": {
					"reviews": {
						"dynamic": false,
						"enabled": true,
						"properties": {
							"content": {
								"dynamic": false,
								"enabled": true,
								"fields": [
								{
									"index": true,
									"name": "content",
									"type": "text"
								}
								]
							},
							"ratings": {
								"dynamic": true,
								"enabled": true
							}
						}
					}
				}
			}
		}
	}`

	m, emptyDir, err := createManagerAndIntroduceIndex(t, "foo", indexParams)
	if err != nil {
		cbgt.DataSourceUUID = prevDataSourceUUID
		t.Fatal(err)
	}
	defer func() {
		m.Stop()
		os.RemoveAll(emptyDir)
		cbgt.DataSourceUUID = prevDataSourceUUID
	}()

	tests := []struct {
		sr               []byte
		expectedResponse error
	}{
		{
			sr: []byte(`{
				"query": {"match": "x", "field": "content"}
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// term OR numeric-range
			sr: []byte(`{
				"query": {"disjuncts": [
					{"term": "x", "field": "reviews.content"},
					{"min": 1, "max": 2, "field": "reviews.ratings.num"}
				]}
			}`),
			expectedResponse: nil,
		},
	}

	for testI, test := range tests {
		var sr *bleve.SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		err = validateSearchRequestAgainstIndex(m, "foo", "", nil, sr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

func TestQueryValidationAgainstCompositeFieldIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": true,
				"dynamic": false,
				"properties": {
					"content": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"index": true,
							"name": "content",
							"type": "text",
							"include_in_all": true
						}
						]
					},
					"date": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"index": true,
							"name": "date",
							"type": "datetime",
							"include_in_all": true
						}
						]
					},
					"geo": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"index": true,
							"name": "geo",
							"type": "geopoint",
							"include_in_all": true
						}
						]
					}
				}
			}
		}
	}`
	m, emptyDir, err := createManagerAndIntroduceIndex(t, "foo", indexParams)
	if err != nil {
		cbgt.DataSourceUUID = prevDataSourceUUID
		t.Fatal(err)
	}
	defer func() {
		m.Stop()
		os.RemoveAll(emptyDir)
		cbgt.DataSourceUUID = prevDataSourceUUID
	}()

	tests := []struct {
		sr               []byte
		expectedResponse error
	}{
		{
			// match
			sr: []byte(`{
				"query": {"match": "x"}
			}`),
			expectedResponse: nil,
		},
		{
			// wildcard
			sr: []byte(`{
				"query": {"wildcard": "x"}
			}`),
			expectedResponse: nil,
		},
		{
			// numeric range (not available in _all)
			sr: []byte(`{
				"query": {"min": 1, "max": 2}
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// date range
			sr: []byte(`{
				"query": {"start": "2024-07-01", "end": "2024-07-31"}
			}`),
			expectedResponse: nil,
		},
		{
			// geo
			sr: []byte(`{
				"query": {
					"location": {
						"lon": -2.235143,
						"lat": 53.482358
					},
					"distance": "100mi"
				}
			}`),
			expectedResponse: nil,
		},
	}

	for testI, test := range tests {
		var sr *bleve.SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		err = validateSearchRequestAgainstIndex(m, "foo", "", nil, sr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

func TestQueryValidationOverCollectionIndexes(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"doc_config": {
			"mode": "scope.collection.type_field",
			"type_field": "type"
		},
		"mapping": {
			"analysis": {},
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": false
			},
			"default_type": "_default",
			"docvalues_dynamic": false,
			"index_dynamic": false,
			"store_dynamic": false,
			"type_field": "_type",
			"types": {
				"inventory.hotel": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"name": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "en",
								"index": true,
								"name": "name",
								"type": "text"
							}
							]
						}
					}
				},
				"inventory.landmark": {
					"dynamic": false,
					"enabled": true,
					"properties": {
						"address": {
							"dynamic": false,
							"enabled": true,
							"fields": [
							{
								"analyzer": "en",
								"index": true,
								"name": "address",
								"type": "text"
							}
							]
						}
					}
				}
			}
		}
	}`
	m, emptyDir, err := createManagerAndIntroduceIndex(t, "foo", indexParams)
	if err != nil {
		cbgt.DataSourceUUID = prevDataSourceUUID
		t.Fatal(err)
	}
	defer func() {
		m.Stop()
		os.RemoveAll(emptyDir)
		cbgt.DataSourceUUID = prevDataSourceUUID
	}()

	tests := []struct {
		sr               []byte
		expectedResponse error
	}{
		{
			// match on  _all
			sr: []byte(`{
					"query": {"match": "x"}
				}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// match on fields, no collections filtering
			sr: []byte(`{
					"query": {"conjuncts": [
						{"match": "x", "field": "name"},
						{"match": "y", "field": "address"}
					]}
				}`),
			expectedResponse: nil,
		},
		{
			// match on field, with collection filtering
			sr: []byte(`{
				"query": {"match": "x", "field": "name"},
				"collections": ["hotel"]
			}`),
			expectedResponse: nil,
		},
		{
			// match on field, with collection filtering
			sr: []byte(`{
				"query": {"match": "x", "field": "name"},
				"collections": ["landmark"]
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			// match on fields, with collections filtering
			sr: []byte(`{
				"query": {"conjuncts": [
					{"match": "x", "field": "name"},
					{"match": "y", "field": "address"}
				]},
				"collections": ["hotel", "landmark"]
			}`),
			expectedResponse: nil,
		},
	}

	for testI, test := range tests {
		var sr *SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		bsr, err := sr.ConvertToBleveSearchRequest()
		if err != nil {
			t.Fatal(err)
		}
		err = validateSearchRequestAgainstIndex(m, "foo", "", sr.Collections, bsr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

// -----------------------------------------------------------------------------

func createManagerAndIntroduceIndex(t *testing.T, indexName, indexParams string) (
	m *cbgt.Manager, dataDir string, err error) {
	cbgt.DataSourceUUID = func(sourceType, sourceName, sourceParams, server string,
		options map[string]string) (string, error) {
		return "123", nil
	}

	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	cfg := cbgt.NewCfgMem()
	m = cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		_ = os.RemoveAll(emptyDir)
		return nil, "", fmt.Errorf("expected Manager.Start() to work, err: %v", err)
	}

	if err := m.CreateIndex("primary", "default", "123", "",
		"fulltext-index", indexName, indexParams, cbgt.PlanParams{}, ""); err != nil {
		_ = os.RemoveAll(emptyDir)
		return nil, "", fmt.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	return m, emptyDir, nil
}

//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build vectors
// +build vectors

package cbft

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
)

func TestQueryValidationWithKNNAgainstTailoredIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": true,
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
					"embedding": {
						"enabled": true,
						"dynamic": false,
						"fields": [
						{
							"dims": 3,
							"index": true,
							"name": "embedding",
							"similarity": "dot_product",
							"type": "vector"
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
			sr: []byte(`{
				"query": {"match": "x", "field": "content"},
				"knn": [{"field": "wrongEmbedding", "k": 5, "vector": [1, 2, 3]}]
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			sr: []byte(`{
				"query": {"match": "x", "field": "content"},
				"knn": [{"field": "embedding", "k": 5, "vector": [1, 2, 3]}]
			}`),
			expectedResponse: nil,
		},
		{
			sr: []byte(`{
				"query": {"match": "x", "field": "content"},
				"knn": [{"field": "embedding", "k": 5, "vector": [1, 2, 3, 4]}]
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			sr: []byte(`{
				"query": {"match_none": {}},
				"knn": [{"field": "embedding", "k": 5, "vector": [5, 6, 7]}]
			}`),
			expectedResponse: nil,
		},
		{
			sr: []byte(`{
				"knn": [{"field": "embedding", "k": 5, "vector": [5, 6, 7]}]
			}`),
			expectedResponse: nil,
		},
		{
			sr: []byte(`{
				"knn": [{"field": "embedding", "k": 5, "vector_base64": "gibberish"}]
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			sr: []byte(fmt.Sprintf(`{
				"knn": [{"field": "embedding", "k": 5, "vector_base64": "%v"}]
			}`, base64EncodeVector([]float32{1, 2, 3}))),
			expectedResponse: nil,
		},
		{
			sr: []byte(fmt.Sprintf(`{
				"knn": [{"field": "embedding", "k": 5, "vector_base64": "%v"}]
			}`, base64EncodeVector([]float32{1, 2, 3, 4}))),
			expectedResponse: fmt.Errorf("non-nil"),
		},
	}

	for testI, test := range tests {
		var sr *bleve.SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		err := validateSearchRequestAgainstIndex(m, "foo", "", nil, sr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

func TestQueryValidationWithKNNAgainstDynamicIndex(t *testing.T) {
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

	srBytes := []byte(`{
		"query": {"match": "x", "field": "content"},
		"knn": [{"field": "embedding", "k": 5, "vector": [1, 2, 3]}]
	}`)
	var sr *bleve.SearchRequest
	if err := json.Unmarshal(srBytes, &sr); err != nil {
		t.Fatal(err)
	}

	if err := validateSearchRequestAgainstIndex(m, "foo", "", nil, sr); err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestQueryValidationWithKNNAgainstNestedTailoredIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": true,
				"dynamic": false,
				"properties": {
					"x": {
						"enabled": true,
						"dynamic": true,
						"properties": {
							"embedding": {
								"enabled": true,
								"dynamic": false,
								"fields": [
								{
									"name": "embedding",
									"type": "vector",
									"index": true,
									"similarity": "dot_product",
									"vector_index_optimized_for": "recall",
									"dims": 3
								}
								]
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
				"query": {"match_none": {}},
				"knn": [{"field": "x.embedding", "k": 5, "vector": [1, 2, 3]}]
			}`),
			expectedResponse: nil,
		},
		{
			sr: []byte(`{
				"knn": [{"field": "x.embedding", "k": 5, "vector": [4, 5, 6]}]
			}`),
			expectedResponse: nil,
		},
		{
			sr: []byte(`{
				"knn": [{"field": "x.embedding", "k": 5, "vector_base64": "gibberish"}]
			}`),
			expectedResponse: fmt.Errorf("non-nil"),
		},
		{
			sr: []byte(fmt.Sprintf(`{
				"knn": [{"field": "x.embedding", "k": 5, "vector_base64": "%v"}]
			}`, base64EncodeVector([]float32{1, 2, 3}))),
			expectedResponse: nil,
		},
		{
			sr: []byte(fmt.Sprintf(`{
				"knn": [{"field": "x.embedding", "k": 5, "vector_base64": "%v"}]
			}`, base64EncodeVector([]float32{1, 2, 3, 4}))),
			expectedResponse: fmt.Errorf("non-nil"),
		},
	}

	for testI, test := range tests {
		var sr *bleve.SearchRequest
		if err := json.Unmarshal(test.sr, &sr); err != nil {
			t.Fatal(testI+1, err)
		}
		err := validateSearchRequestAgainstIndex(m, "foo", "", nil, sr)
		if (test.expectedResponse == nil && err != nil) ||
			(test.expectedResponse != nil && err == nil) {
			t.Errorf("[%d] expected %v, got: %v", testI+1, test.expectedResponse, err)
		}
	}
}

func TestQueryValidationWithKNNAgainstNestedDynamicIndex(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	indexParams := `{
		"mapping": {
			"default_analyzer": "standard",
			"default_mapping": {
				"enabled": true,
				"dynamic": false,
				"properties": {
					"x": {
						"enabled": true,
						"dynamic": true
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

	srBytes := []byte(`{
		"knn": [{"field": "x.embedding", "k": 5, "vector": [1, 2, 3]}]
	}`)
	var sr *bleve.SearchRequest
	if err := json.Unmarshal(srBytes, &sr); err != nil {
		t.Fatal(err)
	}

	if err := validateSearchRequestAgainstIndex(m, "foo", "", nil, sr); err == nil {
		t.Errorf("expected error, got nil")
	}
}

// -----------------------------------------------------------------------------

func base64EncodeVector(vec []float32) string {
	buf := new(bytes.Buffer)
	for _, v := range vec {
		err := binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			return ""
		}
	}

	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

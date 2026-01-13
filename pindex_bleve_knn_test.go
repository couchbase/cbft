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
	"encoding/json"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

func TestDecorateKNNRequestWithCollections(t *testing.T) {
	testCache := getTestCache()

	tests := []struct {
		name          string
		indexName     string
		collections   []string
		knnJSON       string
		qNumDisjuncts int
		qField        string
		qTerms        []string
	}{
		{
			name:          "single collection with single KNN",
			indexName:     "ftsIndexA",
			collections:   []string{"colA"},
			knnJSON:       `[{"field": "embedding", "k": 5, "vector": [1, 2, 3]}]`,
			qField:        "_$scope_$collection",
			qTerms:        []string{"_$suid_$cuidA"},
			qNumDisjuncts: 1,
		},
		{
			name:          "multiple collections with single KNN",
			indexName:     "ftsIndexB",
			collections:   []string{"colA", "colB", "colC"},
			knnJSON:       `[{"field": "embedding", "k": 5, "vector": [1, 2, 3]}]`,
			qField:        "_$scope_$collection",
			qTerms:        []string{"_$suid_$cuidA", "_$suid_$cuidB", "_$suid_$cuidC"},
			qNumDisjuncts: 3,
		},
		{
			name:          "multiple collections with multiple KNNs",
			indexName:     "ftsIndexB",
			collections:   []string{"colA", "colB"},
			knnJSON:       `[{"field": "embedding1", "k": 5, "vector": [1, 2, 3]}, {"field": "embedding2", "k": 10, "vector": [4, 5, 6]}]`,
			qField:        "_$scope_$collection",
			qTerms:        []string{"_$suid_$cuidA", "_$suid_$cuidB"},
			qNumDisjuncts: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queryStr := `{"query": {"query": "california"}, "knn": ` + test.knnJSON + `}`
			var sr *SearchRequest
			err := json.Unmarshal([]byte(queryStr), &sr)
			if err != nil {
				t.Fatal(err)
			}
			sr.Collections = test.collections

			bsr, err := sr.ConvertToBleveSearchRequest()
			if err != nil {
				t.Fatal(err)
			}

			originalKNN, decoratedKNN := sr.decorateKNNRequest(test.indexName, bsr, testCache)

			// Check that original KNN is returned unchanged
			originalKNNRequests, ok := originalKNN.([]*bleve.KNNRequest)
			if !ok {
				t.Fatal("expected originalKNN to be []*bleve.KNNRequest")
			}
			if len(originalKNNRequests) == 0 {
				t.Fatal("expected at least one KNN request in original")
			}
			// Original should not have any filter queries added (they were nil initially)
			for _, knnReq := range originalKNNRequests {
				if knnReq.FilterQuery != nil {
					t.Errorf("expected original KNN request to have nil FilterQuery")
				}
			}

			// Check decorated KNN
			decoratedKNNRequests, ok := decoratedKNN.([]*bleve.KNNRequest)
			if !ok {
				t.Fatal("expected decoratedKNN to be []*bleve.KNNRequest")
			}
			if len(decoratedKNNRequests) != len(originalKNNRequests) {
				t.Fatalf("expected %d decorated KNN requests, got %d",
					len(originalKNNRequests), len(decoratedKNNRequests))
			}

			// Verify each decorated KNN request has the proper filter
			for _, knnReq := range decoratedKNNRequests {
				if knnReq.FilterQuery == nil {
					t.Fatal("expected decorated KNN request to have non-nil FilterQuery")
				}

				djnq, ok := knnReq.FilterQuery.(*query.DisjunctionQuery)
				if !ok {
					t.Fatalf("expected FilterQuery to be DisjunctionQuery, got %T", knnReq.FilterQuery)
				}
				if len(djnq.Disjuncts) != test.qNumDisjuncts {
					t.Fatalf("expected %d disjuncts, got %d", test.qNumDisjuncts, len(djnq.Disjuncts))
				}

				for i, dq := range djnq.Disjuncts {
					mq, ok := dq.(*query.MatchQuery)
					if !ok {
						t.Fatalf("expected disjunct to be MatchQuery, got %T", dq)
					}
					if mq.Match != test.qTerms[i] || mq.FieldVal != test.qField {
						t.Errorf("expected match query with field=%s, term=%s; got field=%s, term=%s",
							test.qField, test.qTerms[i], mq.FieldVal, mq.Match)
					}
				}
			}
		})
	}
}

func TestDecorateKNNRequestWithExistingFilter(t *testing.T) {
	testCache := getTestCache()

	// Test that when KNN has existing filter, it's conjuncted with collection filter
	queryStr := `{
		"query": {"query": "california"},
		"knn": [{"field": "embedding", "k": 5, "vector": [1, 2, 3], "filter": {"term": "active", "field": "status"}}]
	}`
	var sr *SearchRequest
	err := json.Unmarshal([]byte(queryStr), &sr)
	if err != nil {
		t.Fatal(err)
	}
	sr.Collections = []string{"colA", "colB"}

	bsr, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		t.Fatal(err)
	}

	// Verify the initial filter is present
	if len(bsr.KNN) == 0 {
		t.Fatal("expected KNN to be present")
	}
	if bsr.KNN[0].FilterQuery == nil {
		t.Fatal("expected initial FilterQuery to be present")
	}

	_, decoratedKNN := sr.decorateKNNRequest("ftsIndexB", bsr, testCache)

	decoratedKNNRequests, ok := decoratedKNN.([]*bleve.KNNRequest)
	if !ok {
		t.Fatal("expected decoratedKNN to be []*bleve.KNNRequest")
	}

	// The filter should now be a ConjunctionQuery containing:
	// 1. The original filter
	// 2. The collection disjunction
	cjnq, ok := decoratedKNNRequests[0].FilterQuery.(*query.ConjunctionQuery)
	if !ok {
		t.Fatalf("expected FilterQuery to be ConjunctionQuery when original filter exists, got %T",
			decoratedKNNRequests[0].FilterQuery)
	}
	if len(cjnq.Conjuncts) != 2 {
		t.Fatalf("expected 2 conjuncts (original filter + collection disjunction), got %d",
			len(cjnq.Conjuncts))
	}

	// Second conjunct should be the disjunction with collection matches
	djnq, ok := cjnq.Conjuncts[1].(*query.DisjunctionQuery)
	if !ok {
		t.Fatalf("expected second conjunct to be DisjunctionQuery, got %T", cjnq.Conjuncts[1])
	}
	if len(djnq.Disjuncts) != 2 {
		t.Fatalf("expected 2 disjuncts for 2 collections, got %d", len(djnq.Disjuncts))
	}
}

func TestDecorateKNNRequestNoCollections(t *testing.T) {
	// When no collections are specified, the KNN should be returned unchanged
	queryStr := `{
		"query": {"query": "california"},
		"knn": [{"field": "embedding", "k": 5, "vector": [1, 2, 3]}]
	}`
	var sr *SearchRequest
	err := json.Unmarshal([]byte(queryStr), &sr)
	if err != nil {
		t.Fatal(err)
	}
	// No collections set

	bsr, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		t.Fatal(err)
	}

	originalKNN, decoratedKNN := sr.decorateKNNRequest("ftsIndexA", bsr, nil)

	// originalKNN should be nil when no collections (no decoration happened)
	if originalKNN != nil {
		t.Errorf("expected originalKNN to be nil when no collections, got %v", originalKNN)
	}

	// decoratedKNN should be the same as bsr.KNN (passed through unchanged)
	decoratedKNNRequests, ok := decoratedKNN.([]*bleve.KNNRequest)
	if !ok {
		t.Fatal("expected originalKNN to be []*bleve.KNNRequest")
	}
	if len(decoratedKNNRequests) != 1 {
		t.Fatalf("expected 1 KNN request, got %d", len(decoratedKNNRequests))
	}
}

func TestDecorateKNNRequestNilKNN(t *testing.T) {
	// When KNN is nil, the function should return nil for both
	queryStr := `{"query": {"query": "california"}}`
	var sr *SearchRequest
	err := json.Unmarshal([]byte(queryStr), &sr)
	if err != nil {
		t.Fatal(err)
	}
	sr.Collections = []string{"colA"}

	bsr, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		t.Fatal(err)
	}

	originalKNN, _ := sr.decorateKNNRequest("ftsIndexA", bsr, nil)

	if originalKNN != nil {
		t.Errorf("expected originalKNN to be nil when KNN is nil, got %v", originalKNN)
	}
}

func TestSetKNNRequest(t *testing.T) {
	// Test setKNNRequest properly sets KNN on bleve.SearchRequest
	knnRequests := []*bleve.KNNRequest{
		{
			Field:  "embedding",
			Vector: []float32{1, 2, 3},
			K:      5,
		},
	}

	bsr := &bleve.SearchRequest{}
	setKNNRequest(bsr, knnRequests)

	if bsr.KNN == nil || len(bsr.KNN) != 1 {
		t.Fatalf("expected KNN to be set with 1 request, got %v", bsr.KNN)
	}
	if bsr.KNN[0].Field != "embedding" {
		t.Errorf("expected field to be 'embedding', got %s", bsr.KNN[0].Field)
	}
}

func TestSetKNNRequestInvalidType(t *testing.T) {
	// Test setKNNRequest does nothing for invalid types
	bsr := &bleve.SearchRequest{}
	setKNNRequest(bsr, "not a knn request")

	if bsr.KNN != nil {
		t.Errorf("expected KNN to remain nil for invalid type, got %v", bsr.KNN)
	}
}

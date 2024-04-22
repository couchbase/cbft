//  Copyright 2023-Present Couchbase, Inc.
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

	"github.com/blevesearch/bleve/v2"
)

// v2: 7.6.2
const featuresVectorBase64Dims4096 = "vector_base64_dims:4096"

func FeatureVectorSearchSupport() string {
	return "," + featureVectorSearch +
		"," + featuresVectorBase64Dims4096
}

// method will return appropriate flag to check cluster wide
// if & when dims' ceiling is raised in the future
func featureFlagForDims(dims int) string {
	if dims <= 2048 {
		return ""
	}

	if dims <= 4096 {
		return featuresVectorBase64Dims4096
	}

	return ""
}

func interpretKNNForRequest(knn, knnOperator json.RawMessage, r *bleve.SearchRequest) (
	*bleve.SearchRequest, error) {
	if knn != nil && r != nil {
		if err := UnmarshalJSON(knn, &r.KNN); err != nil {
			return nil, err
		}
	}
	if knnOperator != nil && r != nil {
		if err := UnmarshalJSON(knnOperator, &r.KNNOperator); err != nil {
			return nil, err
		}
	}

	return r, nil
}

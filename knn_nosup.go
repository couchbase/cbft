//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !vectors
// +build !vectors

package cbft

import (
	"encoding/json"

	"github.com/blevesearch/bleve/v2"
)

const featuresVectorBase64Dims4096 = ""

const featureVectorCosineSimilarity = ""

var kNNThrottleLimit int64

func FeatureVectorSearchSupport() string {
	return ""
}

func featureFlagForDims(int) string {
	return ""
}

// -----------------------------------------------------------------------------

func interpretKNNForRequest(knn, knnOperator json.RawMessage, r *bleve.SearchRequest) (
	*bleve.SearchRequest, error) {
	// Not supported
	return r, nil
}

// extractKNNQueryFields is not supported
func extractKNNQueryFields(sr *bleve.SearchRequest,
	queryFields map[indexProperty]struct{}) (map[indexProperty]struct{}, error) {
	return queryFields, nil
}

func QueryHasKNN(req []byte) bool {
	// Not supported
	return false
}

func indexHasVectorFields(params string) bool {
	// Not supported
	return false
}

func InitKNNQueryThrottlerOptions(options map[string]string) error {
	// Not supported
	return nil
}

func GetKNNThrottleLimit() int64 {
	return 0
}

func SetKNNThrottleLimit(val int64) {
}

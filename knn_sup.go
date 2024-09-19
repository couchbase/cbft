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
	"regexp"
	"strconv"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/search/query"
	log "github.com/couchbase/clog"
)

// 7.6.2+
const featuresVectorBase64Dims4096 = "vector_base64_dims:4096"

// 7.6.4+
const featureVectorCosineSimilarity = "vector_cosine"

// A knn regex to check against and determine if a query has knn fields
var knnRegex *regexp.Regexp
var kNNThrottleLimit int64

func init() {
	var err error
	knnRegex, err = regexp.Compile(`"knn":\[{"`)
	if err != nil {
		log.Warnf("knn regex compilation failed, knn query throttler will be disabled")
	}
}

func FeatureVectorSearchSupport() string {
	return "," + featureVectorSearch +
		"," + featuresVectorBase64Dims4096 +
		"," + featureVectorCosineSimilarity
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

// -----------------------------------------------------------------------------

func interpretKNNForRequest(knn, knnOperator json.RawMessage, r *bleve.SearchRequest) (
	*bleve.SearchRequest, error) {
	if knn != nil && r != nil {
		type tempKNNReq struct {
			Field        string          `json:"field"`
			Vector       []float32       `json:"vector"`
			VectorBase64 string          `json:"vector_base64"`
			K            int64           `json:"k"`
			Boost        *query.Boost    `json:"boost,omitempty"`
			Params       json.RawMessage `json:"params,omitempty"`
			FilterQuery  json.RawMessage `json:"filter,omitempty"`
		}

		var tmp []tempKNNReq
		err := UnmarshalJSON(knn, &tmp)
		if err != nil {
			return nil, err
		}

		r.KNN = make([]*bleve.KNNRequest, len(tmp))
		for i, knnReq := range tmp {
			r.KNN[i] = &bleve.KNNRequest{}
			r.KNN[i].Field = knnReq.Field
			r.KNN[i].Vector = knnReq.Vector
			r.KNN[i].VectorBase64 = knnReq.VectorBase64
			r.KNN[i].K = knnReq.K
			r.KNN[i].Boost = knnReq.Boost
			r.KNN[i].Params = knnReq.Params
			if len(knnReq.FilterQuery) == 0 {
				// Setting this to nil to avoid ParseQuery() setting it to a match none
				r.KNN[i].FilterQuery = nil
			} else {
				r.KNN[i].FilterQuery, err = query.ParseQuery(knnReq.FilterQuery)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if knnOperator != nil && r != nil {
		if err := UnmarshalJSON(knnOperator, &r.KNNOperator); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// extractKNNQueryFields extracts KNN query fields from the search request.
func extractKNNQueryFields(sr *bleve.SearchRequest,
	queryFields map[indexProperty]struct{}) (map[indexProperty]struct{}, error) {
	var err error
	if sr.KNN != nil {
		for _, entry := range sr.KNN {
			if entry != nil {
				if entry.Vector == nil && entry.VectorBase64 != "" {
					entry.Vector, err = document.DecodeVector(entry.VectorBase64)
					if err != nil {
						return nil, err
					}
				}

				if queryFields == nil {
					queryFields = map[indexProperty]struct{}{}
				}

				queryFields[indexProperty{
					Name: entry.Field,
					Type: "vector",
					Dims: len(entry.Vector),
				}] = struct{}{}
			}
		}
	}
	return queryFields, nil
}

func QueryHasKNN(req []byte) bool {
	if knnRegex != nil && knnRegex.Match(req) {
		return true
	}
	return false
}

func InitKNNQueryThrottlerOptions(options map[string]string) error {
	if options["KNNSearchRequestConcurrencyLimit"] != "" {
		val, err := strconv.Atoi(options["KNNSearchRequestConcurrencyLimit"])
		if err != nil {
			return err
		}
		SetKNNThrottleLimit(int64(val))
	}
	return nil
}

func GetKNNThrottleLimit() int64 {
	return atomic.LoadInt64(&kNNThrottleLimit)
}

func SetKNNThrottleLimit(val int64) {
	atomic.StoreInt64(&kNNThrottleLimit, val)
}

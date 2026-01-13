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
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

// 7.6.2+
const featuresVectorBase64Dims4096 = "vector_base64_dims:4096"

// 7.6.4+
const featureVectorCosineSimilarity = "vector_cosine"

// A knn regex to check against and determine if a query has knn fields
var knnRegex *regexp.Regexp
var kNNThrottleLimit int64

// Vector index regexes to check against and determine if an index has
// vector fields
var vectorIndexRegexes []*regexp.Regexp

func init() {
	var err error
	knnRegex, err = regexp.Compile(`"knn":\[{"`)
	if err != nil {
		log.Warnf("knn regex compilation failed, knn query throttler will be disabled")
	}

	if runtime.GOOS == "windows" {
		scorch.BleveMaxKNNConcurrency = 1
	}

	vectorRegex, err := regexp.Compile(`"type":"vector"`)
	if err != nil {
		log.Warnf("vector index regex compilation failed")
	}
	vectorIndexRegexes = append(vectorIndexRegexes, vectorRegex)

	vectorBase64Regex, err := regexp.Compile(`"type":"vector_base64"`)
	if err != nil {
		log.Warnf("vector_base64 index regex compilation failed")
	}
	vectorIndexRegexes = append(vectorIndexRegexes, vectorBase64Regex)
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
		var tmp []struct {
			Field        string          `json:"field"`
			Vector       []float32       `json:"vector"`
			VectorBase64 string          `json:"vector_base64"`
			K            int64           `json:"k"`
			Boost        *query.Boost    `json:"boost,omitempty"`
			Params       json.RawMessage `json:"params,omitempty"`
			FilterQuery  json.RawMessage `json:"filter,omitempty"`
		}

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

func indexHasVectorFields(params string) bool {
	for _, regex := range vectorIndexRegexes {
		if regex.Match([]byte(params)) {
			return true
		}
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

// -----------------------------------------------------------------------------

type IndexInsightsHandler struct {
	mgr *cbgt.Manager
}

func NewIndexInsightsHandler(mgr *cbgt.Manager) *IndexInsightsHandler {
	return &IndexInsightsHandler{mgr: mgr}
}

const (
	insightTermFrequencies       = "termFrequencies"
	insightCentroidCardinalities = "centroidCardinalities"
)

type IndexInsightsRequest struct {
	Field      string `json:"field"`
	Insight    string `json:"insight"`
	Limit      *int   `json:"limit,omitempty"`
	Descending *bool  `json:"descending,omitempty"`

	LocalOnly bool `json:"local-only,omitempty"`
}

func (h *IndexInsightsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := rest.IndexNameLookup(req)
	if indexName == "" {
		rest.ShowError(w, req, "indexInsights, index name is required", http.StatusBadRequest)
		return
	}

	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		rest.ShowError(w, nil, fmt.Sprintf("indexInsights,"+
			" could not read request body, indexName: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	var idxInsightsReq IndexInsightsRequest
	err = UnmarshalJSON(requestBody, &idxInsightsReq)
	if err != nil {
		rest.ShowError(w, nil, fmt.Sprintf("indexInsights,"+
			" error parsing request body, indexName: %s, err: %v",
			indexName, err), http.StatusBadRequest)
		return
	}

	if len(idxInsightsReq.Field) == 0 {
		rest.ShowError(w, req, "indexInsights, field name required", http.StatusBadRequest)
		return
	}

	if idxInsightsReq.Insight != insightTermFrequencies &&
		idxInsightsReq.Insight != insightCentroidCardinalities {
		rest.ShowError(w, req,
			"indexInsights, supported insights: 'termFrequencies'/'centroidCardinalities'",
			http.StatusBadRequest)
		return
	}

	rcAdder := addIndexClients
	if idxInsightsReq.LocalOnly {
		// no index clients in case of a "local-only" request
		rcAdder = nil
	}

	alias, _, _, err := bleveIndexAlias(h.mgr, indexName, "", false, nil, nil,
		true, nil, "", rcAdder)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("indexInsights, indexName: %s, err : %v",
			indexName, err), http.StatusInternalServerError)
		return
	}

	insightsIndex, ok := alias.(bleve.InsightsIndex)
	if !ok {
		rest.ShowError(w, req, "indexInsights, cannot produce insights for index",
			http.StatusNotImplemented)
		return
	}

	// defaults
	limit, descending := 1, true
	if idxInsightsReq.Limit != nil {
		limit = *idxInsightsReq.Limit
	}
	if idxInsightsReq.Descending != nil {
		descending = *idxInsightsReq.Descending
	}

	if idxInsightsReq.Insight == insightTermFrequencies {
		termFreqs, err := insightsIndex.TermFrequencies(
			idxInsightsReq.Field, limit, descending)
		if err != nil {
			rest.ShowError(w, req, fmt.Sprintf("indexInsights, indexName: %s, field: %s,"+
				" limit: %d, descending: %v, termFrequencies err: %v",
				indexName, idxInsightsReq.Field, limit, descending, err),
				http.StatusInternalServerError)
			return
		}

		rv := struct {
			Status          string               `json:"status"`
			Request         IndexInsightsRequest `json:"request"`
			TermFrequencies []index.TermFreq     `json:"termFrequencies"`
		}{
			Status:          "ok",
			Request:         idxInsightsReq,
			TermFrequencies: termFreqs,
		}
		rest.MustEncode(w, rv)
		return
	}

	// centroidCardinalities
	centroidCardinalities, err := insightsIndex.CentroidCardinalities(
		idxInsightsReq.Field, limit, descending)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("IndexInsights, indexName: %s, field: %s,"+
			" limit: %d, descending: %v, centroidCardinalities err: %v",
			indexName, idxInsightsReq.Field, limit, descending, err),
			http.StatusInternalServerError)
		return
	}

	rv := struct {
		Status                string                      `json:"status"`
		Request               IndexInsightsRequest        `json:"request"`
		CentroidCardinalities []index.CentroidCardinality `json:"centroidCardinalities"`
	}{
		Status:                "ok",
		Request:               idxInsightsReq,
		CentroidCardinalities: centroidCardinalities,
	}
	rest.MustEncode(w, rv)
}

// -----------------------------------------------------------------------------

// returns the original KNN request along with the decorated KNN request
func (sr *SearchRequest) decorateKNNRequest(indexName string, searchRequest *bleve.SearchRequest, cache *collMetaFieldCache) (interface{}, interface{}) {
	if len(sr.Collections) == 0 || sr.KNN == nil {
		return nil, searchRequest.KNN
	}

	if cache == nil {
		cache = metaFieldValCache
	}

	// filter must be a disjunction of collections
	djnq := query.NewDisjunctionQuery(nil)

	for _, col := range sr.Collections {
		queryStr := cache.getMetaFieldValue(indexName, col)
		mq := query.NewMatchQuery(queryStr)
		mq.Analyzer = "keyword"
		mq.SetField(CollMetaFieldName)
		djnq.AddQuery(mq)
	}
	djnq.SetMin(1)

	decoratedKnnRequests := make([]*bleve.KNNRequest, 0, len(searchRequest.KNN))
	for _, knnQ := range searchRequest.KNN {
		// if prefilter already exists, conjunct it with the collections disjunction
		var filterQuery query.Query
		if knnQ.FilterQuery != nil {
			filterQuery = query.NewConjunctionQuery(
				[]query.Query{
					knnQ.FilterQuery,
					djnq,
				},
			)
		} else {
			filterQuery = djnq
		}

		decoratedKnnRequests = append(decoratedKnnRequests, &bleve.KNNRequest{
			Field:        knnQ.Field,
			Vector:       knnQ.Vector,
			VectorBase64: knnQ.VectorBase64,
			K:            knnQ.K,
			Boost:        knnQ.Boost,
			Params:       knnQ.Params,
			FilterQuery:  filterQuery,
		})
	}

	return searchRequest.KNN, decoratedKnnRequests
}

func setKNNRequest(sr *bleve.SearchRequest, knn interface{}) {
	knnRequest, ok := knn.([]*bleve.KNNRequest)

	if !ok {
		return
	}

	sr.KNN = knnRequest
}

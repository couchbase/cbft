package cbft

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blevesearch/bleve/v2/search/query"
	jsoniter "github.com/json-iterator/go"
)

type customQueryParams struct {
	Fields []string               `json:"fields,omitempty"`
	Params map[string]interface{} `json:"params,omitempty"`
	Source string                 `json:"source,omitempty"`
}

type customQueryInput struct {
	Query json.RawMessage `json:"query"`
	customQueryParams
}

func parseCustomFilterQuery(input []byte) (query.Query, error) {
	rv, child, err := parseCustomQueryInput(input, "custom_filter")
	if err != nil {
		return nil, err
	}
	filterFn, err := buildFilterFunc(rv.Source, rv.Params, rv.Fields)
	if err != nil {
		return nil, err
	}
	return query.NewCustomFilterQueryWithFilter(child, filterFn, rv.Fields,
		makeCustomQueryPayload(rv.customQueryParams)), nil
}

func parseCustomScoreQuery(input []byte) (query.Query, error) {
	rv, child, err := parseCustomQueryInput(input, "custom_score")
	if err != nil {
		return nil, err
	}
	scoreFn, err := buildScoreFunc(rv.Source, rv.Params, rv.Fields)
	if err != nil {
		return nil, err
	}
	return query.NewCustomScoreQueryWithScorer(child, scoreFn, rv.Fields,
		makeCustomQueryPayload(rv.customQueryParams)), nil
}

func parseCustomQueryInput(input []byte, key string) (*customQueryInput, query.Query, error) {
	var rv struct {
		Custom customQueryInput `json:"-"`
	}
	switch key {
	case "custom_filter":
		var payload struct {
			Custom customQueryInput `json:"custom_filter"`
		}
		if err := jsoniter.Unmarshal(input, &payload); err != nil {
			return nil, nil, err
		}
		rv.Custom = payload.Custom
	case "custom_score":
		var payload struct {
			Custom customQueryInput `json:"custom_score"`
		}
		if err := jsoniter.Unmarshal(input, &payload); err != nil {
			return nil, nil, err
		}
		rv.Custom = payload.Custom
	default:
		return nil, nil, fmt.Errorf("unsupported custom query type %q", key)
	}

	if rv.Custom.Query == nil {
		return nil, nil, fmt.Errorf("%s query must have a query", strings.ReplaceAll(key, "_", " "))
	}
	if rv.Custom.Source == "" {
		return nil, nil, fmt.Errorf("%s query must have source", strings.ReplaceAll(key, "_", " "))
	}

	// Keep the child as RawMessage so nested queries recurse through cbft's
	// local parseQuery() path instead of bypassing it with bleve's generic
	// JSON unmarshal path.
	child, err := parseQuery(rv.Custom.Query)
	if err != nil {
		return nil, nil, err
	}

	return &rv.Custom, child, nil
}

func makeCustomQueryPayload(params customQueryParams) map[string]interface{} {
	payload := make(map[string]interface{}, 2)

	if len(params.Params) > 0 {
		payload["params"] = params.Params
	}
	if params.Source != "" {
		payload["source"] = params.Source
	}

	if len(payload) == 0 {
		return nil
	}

	return payload
}

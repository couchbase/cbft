package cbft

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
)

func TestRefreshCustomScriptQuerySettings(t *testing.T) {
	customScriptQueriesEnabled.Store(false)

	RefreshCustomScriptQuerySettings(map[string]string{"customScriptQueriesEnabled": "true"})
	if !customScriptQueriesEnabled.Load() {
		t.Fatalf("expected customScriptQueriesEnabled to be true")
	}

	RefreshCustomScriptQuerySettings(map[string]string{"customScriptQueriesEnabled": "false"})
	if customScriptQueriesEnabled.Load() {
		t.Fatalf("expected customScriptQueriesEnabled to be false")
	}
}

func TestBuildFilterFuncDisabledError(t *testing.T) {
	restore := saveCustomScriptQueriesState()
	defer restore()

	InitJSEvaluator = func() {}
	CustomScriptFilterBuilder = func(source string, params map[string]interface{}, fields []string) (searcher.CustomFilterFunc, error) {
		return func(d *search.DocumentMatch) bool { return true }, nil
	}
	customScriptQueriesEnabled.Store(false)

	_, err := buildFilterFunc("test-source", nil, nil)
	if err == nil || err.Error() != "custom script queries are disabled" {
		t.Fatalf("expected custom script queries disabled error, got %v", err)
	}
}

func TestBuildFilterFuncEnterpriseOnlyError(t *testing.T) {
	restore := saveCustomScriptQueriesState()
	defer restore()

	InitJSEvaluator = func() {}
	CustomScriptFilterBuilder = func(source string, params map[string]interface{}, fields []string) (searcher.CustomFilterFunc, error) {
		return nil, errors.New("custom script queries are available only in enterprise edition")
	}
	customScriptQueriesEnabled.Store(true)

	_, err := buildFilterFunc("test-source", nil, nil)
	if err == nil || err.Error() != "custom script queries are available only in enterprise edition" {
		t.Fatalf("expected enterprise-only error, got %v", err)
	}
}

func TestBuildScoreFuncEnabled(t *testing.T) {
	restore := saveCustomScriptQueriesState()
	defer restore()

	InitJSEvaluator = func() {}
	CustomScriptScoreBuilder = func(source string, params map[string]interface{}, fields []string) (searcher.CustomScoreFunc, error) {
		return func(d *search.DocumentMatch) float64 { return d.Score }, nil
	}
	customScriptQueriesEnabled.Store(true)

	_, err := buildScoreFunc("score-src", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParseQueryBindsCustomScriptCallbacks(t *testing.T) {
	restore := saveCustomScriptQueriesState()
	defer restore()

	var filterSources, scoreSources []string
	CustomScriptFilterBuilder = func(source string, params map[string]interface{}, fields []string) (searcher.CustomFilterFunc, error) {
		filterSources = append(filterSources, source)
		return func(d *search.DocumentMatch) bool { return true }, nil
	}
	CustomScriptScoreBuilder = func(source string, params map[string]interface{}, fields []string) (searcher.CustomScoreFunc, error) {
		scoreSources = append(scoreSources, source)
		return func(d *search.DocumentMatch) float64 { return d.Score }, nil
	}
	InitJSEvaluator = func() {}
	customScriptQueriesEnabled.Store(true)

	q, err := parseQuery([]byte(`{
		"conjuncts": [{
			"custom_filter": {
				"query": {
					"custom_score": {
						"query": { "match_all": {} },
						"source": "score-src"
					}
				},
				"source": "filter-src"
			}
		}]
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(filterSources) != 1 || filterSources[0] != "filter-src" {
		t.Fatalf("expected filter source [filter-src], got %v", filterSources)
	}
	if len(scoreSources) != 1 || scoreSources[0] != "score-src" {
		t.Fatalf("expected score source [score-src], got %v", scoreSources)
	}

	conj, ok := q.(*query.ConjunctionQuery)
	if !ok || len(conj.Conjuncts) != 1 {
		t.Fatalf("expected outer conjunction with 1 child, got %T", q)
	}
	if _, ok := conj.Conjuncts[0].(*query.CustomFilterQuery); !ok {
		t.Fatalf("expected custom filter child, got %T", conj.Conjuncts[0])
	}
}

func TestParseQueryPreservesCustomScriptPayloadForMarshalRoundTrip(t *testing.T) {
	restore := saveCustomScriptQueriesState()
	defer restore()

	CustomScriptScoreBuilder = func(source string, params map[string]interface{}, fields []string) (searcher.CustomScoreFunc, error) {
		return func(d *search.DocumentMatch) float64 { return d.Score }, nil
	}
	InitJSEvaluator = func() {}
	customScriptQueriesEnabled.Store(true)

	q, err := parseQuery([]byte(`{
		"custom_score": {
			"query": { "match": "ipa" },
			"fields": ["abv"],
			"params": { "weight": 0.05 },
			"source": "score-src"
		}
	}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var decoded struct {
		CustomScore struct {
			Query  json.RawMessage        `json:"query"`
			Fields []string               `json:"fields"`
			Params map[string]interface{} `json:"params"`
			Source string                 `json:"source"`
		} `json:"custom_score"`
	}
	err = json.Unmarshal(out, &decoded)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if decoded.CustomScore.Source != "score-src" {
		t.Fatalf("expected source to survive round-trip, got %q", decoded.CustomScore.Source)
	}
	if len(decoded.CustomScore.Fields) != 1 || decoded.CustomScore.Fields[0] != "abv" {
		t.Fatalf("unexpected fields: %v", decoded.CustomScore.Fields)
	}
	if got := decoded.CustomScore.Params["weight"]; got != 0.05 {
		t.Fatalf("unexpected params: %v", decoded.CustomScore.Params)
	}
}

func saveCustomScriptQueriesState() func() {
	savedInitHook := InitJSEvaluator
	savedFilterHook := CustomScriptFilterBuilder
	savedScoreHook := CustomScriptScoreBuilder
	savedEnabled := customScriptQueriesEnabled.Load()

	return func() {
		InitJSEvaluator = savedInitHook
		CustomScriptFilterBuilder = savedFilterHook
		CustomScriptScoreBuilder = savedScoreHook
		customScriptQueriesEnabled.Store(savedEnabled)
	}
}

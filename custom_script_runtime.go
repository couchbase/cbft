//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with this file, use of this software will
//  be governed by the Apache License, Version 2.0, included in the file
//  licenses/APL2.txt.

package cbft

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"

	bleveQuery "github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
	log "github.com/couchbase/clog"
)

// CustomScriptQueriesEngineInitializer allows enterprise extensions (cbftx) to
// install an engine-specific initializer.
type CustomScriptQueriesEngineInitializer func()

// CustomScriptQueriesEngineShutdown allows enterprise extensions (cbftx) to
// install an engine-specific shutdown handler.
type CustomScriptQueriesEngineShutdown func()

// CustomScriptFilterBuilderFn builds a filter callback from source, params, and fields.
type CustomScriptFilterBuilderFn func(source string, params map[string]interface{}, fields []string) (searcher.CustomFilterFunc, error)

// CustomScriptScoreBuilderFn builds a score callback from source, params, and fields.
type CustomScriptScoreBuilderFn func(source string, params map[string]interface{}, fields []string) (searcher.CustomScoreFunc, error)

var (
	customScriptCEWarnOnce     sync.Once
	customScriptQueriesEnabled atomic.Bool
)

func init() {
	bleveQuery.CustomFilterQueryParser = parseCustomFilterQuery
	bleveQuery.CustomScoreQueryParser = parseCustomScoreQuery
}

// InitJSEvaluator is overridden in enterprise builds.
var InitJSEvaluator CustomScriptQueriesEngineInitializer = initCustomScriptCE

// ShutdownJSEvaluator is overridden in enterprise builds to tear down the engine.
var ShutdownJSEvaluator CustomScriptQueriesEngineShutdown = func() {}

// CustomScriptFilterBuilder defaults to the unsupported path and is overridden in enterprise builds.
var CustomScriptFilterBuilder CustomScriptFilterBuilderFn = func(source string,
	params map[string]interface{}, fields []string) (searcher.CustomFilterFunc, error) {
	return nil, errors.New("custom script queries are available only in enterprise edition")
}

// CustomScriptScoreBuilder defaults to the unsupported path and is overridden in enterprise builds.
var CustomScriptScoreBuilder CustomScriptScoreBuilderFn = func(source string,
	params map[string]interface{}, fields []string) (searcher.CustomScoreFunc, error) {
	return nil, errors.New("custom script queries are available only in enterprise edition")
}

func initCustomScriptCE() {
	customScriptCEWarnOnce.Do(func() {
		log.Warnf("custom script query: JS-Evaluator is unavailable in community build")
	})
}

func RefreshCustomScriptQuerySettings(options map[string]string) error {
	if options == nil {
		return nil
	}

	v := options["customScriptQueriesEnabled"]
	// CAS fires Init/Shutdown only on a real state flip; async so the admin
	// toggle doesn't block on engine setup/teardown.
	if vBool, err := strconv.ParseBool(v); err == nil {
		if customScriptQueriesEnabled.CompareAndSwap(false, vBool) {
			go InitJSEvaluator()
		} else if customScriptQueriesEnabled.CompareAndSwap(true, vBool) {
			go ShutdownJSEvaluator()
		}
	}
	return nil
}

// buildFilterFunc invokes the registered builder or returns a disabled/error
// callback depending on the current feature state.
func buildFilterFunc(source string, params map[string]interface{}, fields []string) (searcher.CustomFilterFunc, error) {
	if !customScriptQueriesEnabled.Load() {
		return nil, errors.New("custom script queries are disabled")
	}
	return CustomScriptFilterBuilder(source, params, fields)
}

// buildScoreFunc invokes the registered builder or returns a disabled/error
// callback depending on the current feature state.
func buildScoreFunc(source string, params map[string]interface{}, fields []string) (searcher.CustomScoreFunc, error) {
	if !customScriptQueriesEnabled.Load() {
		return nil, errors.New("custom script queries are disabled")
	}
	return CustomScriptScoreBuilder(source, params, fields)
}

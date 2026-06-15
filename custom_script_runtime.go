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

	// A single long-lived reconciler goroutine owns Init/Shutdown so the engine
	// state always converges to customScriptQueriesEnabled, regardless of how
	// the admin toggles the setting. reconcileCustomScript is buffered (size 1)
	// and coalescing: each signal makes the reconciler re-read the latest
	// desired state, so a dropped (already-pending) signal still yields
	// convergence on the next pass. This replaces direction-specific
	// "go Init/Shutdown" calls which, being unordered goroutines, could
	// otherwise apply Init/Shutdown in an order that left the engine
	// inconsistent with the flag (e.g. flag=true but engine never started).
	reconcileCustomScriptOnce sync.Once
	reconcileCustomScript     = make(chan struct{}, 1)
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

	v, ok := options["customScriptQueriesEnabled"]
	if !ok {
		return nil
	}
	vBool, err := strconv.ParseBool(v)
	if err != nil {
		return nil
	}

	// Record the desired state and nudge the reconciler. Storing unconditionally
	// (instead of CAS-gating a direction-specific goroutine) means concurrent
	// Refresh calls from startup, the REST handler, and ns_server propagation
	// all converge on the final value: InitJSEvaluator/ShutdownJSEvaluator are
	// idempotent, so a duplicate Refresh with the same value is a no-op and
	// never tears down an engine the user just enabled.
	customScriptQueriesEnabled.Store(vBool)
	startCustomScriptReconciler()
	select {
	case reconcileCustomScript <- struct{}{}:
	default:
	}
	return nil
}

// startCustomScriptReconciler lazily starts the single goroutine that drives
// the engine to match customScriptQueriesEnabled. Running Init/Shutdown from
// one goroutine guarantees the last desired state is the one that ends up
// applied, even under rapid toggles, while keeping the admin toggle
// non-blocking.
func startCustomScriptReconciler() {
	reconcileCustomScriptOnce.Do(func() {
		go func() {
			for range reconcileCustomScript {
				if customScriptQueriesEnabled.Load() {
					InitJSEvaluator() // no-op when already running
				} else {
					ShutdownJSEvaluator() // no-op when already stopped
				}
			}
		}()
	})
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

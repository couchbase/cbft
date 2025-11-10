//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"

	bleveSearcher "github.com/blevesearch/bleve/v2/search/searcher"
	"github.com/couchbase/cbft/search_history"
)

// List of log levels that maps strings to integers.
// (Works with clog - https://github.com/couchbase/clog)
var LogLevels map[string]uint32

func init() {
	LogLevels = make(map[string]uint32)

	LogLevels["DEBU"] = 0
	LogLevels["INFO"] = 1
	LogLevels["CRIT"] = 4
	LogLevels["ERRO"] = 3
	LogLevels["FATA"] = 4
	LogLevels["WARN"] = 2
}

// ManagerOptionsExt is a REST handler that serves as a wrapper for
// ManagerOptions - where it sets the manager options, and updates
// the logLevel upon request.
type ManagerOptionsExt struct {
	mgr        *cbgt.Manager
	mgrOptions *rest.ManagerOptions
}

func NewManagerOptionsExt(mgr *cbgt.Manager) *ManagerOptionsExt {
	mgrOptions := rest.NewManagerOptions(mgr)
	mgrOptions.Validate = func(options map[string]string) (map[string]string, error) {
		// Validate logLevel
		logLevelStr := options["logLevel"]
		if logLevelStr != "" {
			_, exists := LogLevels[logLevelStr]
			if !exists {
				return nil, fmt.Errorf("invalid setting for"+
					" logLevel: %v", logLevelStr)
			}
		}

		// Validate maxReplicasAllowed
		maxReplicasAllowed := mgr.GetOption("maxReplicasAllowed")
		if options["maxReplicasAllowed"] != maxReplicasAllowed {
			return nil, fmt.Errorf("maxReplicasAllowed setting is at '%v',"+
				" but request is for '%v'", maxReplicasAllowed,
				options["maxReplicasAllowed"])
		}

		// Validate bucketTypesAllowed
		bucketTypesAllowed := mgr.GetOption("bucketTypesAllowed")
		if options["bucketTypesAllowed"] != bucketTypesAllowed {
			return nil, fmt.Errorf("bucketTypesAllowed setting is at '%v',"+
				" but request is for: '%v'", bucketTypesAllowed,
				options["bucketTypesAllowed"])
		}

		// Validate gcMinThreshold
		if options["gcMinThreshold"] != "" {
			gcMinThreshold, err := strconv.Atoi(options["gcMinThreshold"])
			if err != nil || gcMinThreshold < 0 {
				return nil, fmt.Errorf("illegal value for gcMinThreshold: '%v'",
					options["gcMinThreshold"])
			}
		}

		// Validate gcTriggerPct
		if options["gcTriggerPct"] != "" {
			gcTriggerPct, err := strconv.Atoi(options["gcTriggerPct"])
			if err != nil || gcTriggerPct < 0 {
				return nil, fmt.Errorf("illegal value for gcTriggerPct: '%v'",
					options["gcTriggerPct"])
			}
		}

		// Validate memStatsLoggingInterval
		if options["memStatsLoggingInterval"] != "" {
			memStatsLoggingInterval, err := strconv.Atoi(options["memStatsLoggingInterval"])
			if err != nil || memStatsLoggingInterval < 0 {
				return nil, fmt.Errorf("illegal value for memStatsLoggingInterval: '%v'",
					options["memStatsLoggingInterval"])
			}
		}

		if options["bleveMaxClauseCount"] != "" {
			bleveMaxClauseCount, err := strconv.Atoi(options["bleveMaxClauseCount"])
			if err != nil || bleveMaxClauseCount <= 0 {
				return nil, fmt.Errorf("illegal value for bleveMaxClauseCount: '%v'",
					options["bleveMaxClauseCount"])
			}
		}

		if options["bleveMaxResultWindow"] != "" {
			bleveMaxResultWindow, err := strconv.Atoi(options["bleveMaxResultWindow"])
			if err != nil || bleveMaxResultWindow <= 0 {
				return nil, fmt.Errorf("illegal value for bleveMaxResultWindow: '%v'",
					options["bleveMaxResultWindow"])
			}
		}

		if options["maxFeedsPerDCPAgent"] != "" {
			maxFeedsPerDCPAgent, err := strconv.Atoi(options["maxFeedsPerDCPAgent"])
			if err != nil || uint32(maxFeedsPerDCPAgent) < 0 {
				return nil, fmt.Errorf("illegal value for maxFeedsPerDCPAgent: '%v'",
					options["maxFeedsPerDCPAgent"])
			}
		}

		return options, nil
	}

	return &ManagerOptionsExt{
		mgr:        mgr,
		mgrOptions: mgrOptions,
	}
}

func (h *ManagerOptionsExt) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	h.mgrOptions.ServeHTTP(w, req)

	// Update log level if requested.
	logLevelStr := h.mgr.GetOption("logLevel")
	if logLevelStr != "" {
		logLevel, _ := LogLevels[logLevelStr]
		log.SetLevel(log.LogLevel(logLevel))
	}

	// Update bleveMaxClauseCount if requested.
	bleveMaxClauseCountStr := h.mgr.GetOption("bleveMaxClauseCount")
	if bleveMaxClauseCountStr != "" {
		bleveMaxClauseCount, _ := strconv.Atoi(bleveMaxClauseCountStr)
		bleveSearcher.DisjunctionMaxClauseCount = bleveMaxClauseCount
	}

	// Update search history settings if requested.
	if search_history.Service != nil {
		var enabledPtr *bool
		var maxRecordsPtr *int
		if v := h.mgr.GetOption("searchHistoryEnabled"); v != "" {
			if enabled, err := strconv.ParseBool(v); err == nil {
				enabledPtr = &enabled
			}
		}
		if v := h.mgr.GetOption("searchHistoryMaxRecords"); v != "" {
			if maxRecords, err := strconv.Atoi(v); err == nil {
				maxRecordsPtr = &maxRecords
			}
		}

		if enabledPtr != nil || maxRecordsPtr != nil {
			search_history.Service.UpdateSettings(enabledPtr, maxRecordsPtr)
		}
	}
}

type ConciseOptions struct {
	mgr *cbgt.Manager
}

func NewConciseOptions(mgr *cbgt.Manager) *ConciseOptions {
	return &ConciseOptions{mgr: mgr}
}

func (h *ConciseOptions) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	options := h.mgr.Options()

	maxReplicasAllowed, _ := strconv.Atoi(options["maxReplicasAllowed"])
	bucketTypesAllowed := options["bucketTypesAllowed"]
	bleveMaxResultWindow, _ := strconv.Atoi(options["bleveMaxResultWindow"])
	bleveMaxClauseCount, _ := strconv.Atoi(options["bleveMaxClauseCount"])

	var deploymentModel string
	if val, exists := options["deploymentModel"]; exists {
		deploymentModel = val
	}

	if bleveMaxClauseCount == 0 {
		// in case bleveMaxClauseCount wasn't updated by user
		bleveMaxClauseCount = DefaultBleveMaxClauseCount
	}

	var collectionsSupported bool
	if isClusterCompatibleFor(FeatureCollectionVersion) {
		if nodeDefs, _, err := cbgt.CfgGetNodeDefs(
			h.mgr.Cfg(), cbgt.NODE_DEFS_WANTED); err == nil {
			if cbgt.IsFeatureSupportedByCluster(FeatureCollections, nodeDefs) {
				collectionsSupported = true
			}
		}
	}

	var scopedIndexesSupport bool
	if isClusterCompatibleFor(FeatureScopedIndexNamesVersion) {
		scopedIndexesSupport = true
	}

	rv := struct {
		Status               string `json:"status"`
		MaxReplicasAllowed   int    `json:"maxReplicasAllowed"`
		BucketTypesAllowed   string `json:"bucketTypesAllowed"`
		CollectionsSupport   bool   `json:"collectionsSupport"`
		BleveMaxResultWindow int    `json:"bleveMaxResultWindow"`
		BleveMaxClauseCount  int    `json:"bleveMaxClauseCount"`
		DeploymentModel      string `json:"deploymentModel,omitempty"`
		ScopedIndexesSupport bool   `json:"scopedIndexesSupport"`
	}{
		Status:               "ok",
		MaxReplicasAllowed:   maxReplicasAllowed,
		BucketTypesAllowed:   bucketTypesAllowed,
		CollectionsSupport:   collectionsSupported,
		BleveMaxResultWindow: bleveMaxResultWindow,
		BleveMaxClauseCount:  bleveMaxClauseCount,
		DeploymentModel:      deploymentModel,
		ScopedIndexesSupport: scopedIndexesSupport,
	}
	rest.MustEncode(w, rv)
}

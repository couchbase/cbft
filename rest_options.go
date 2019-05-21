//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"

	bleveSearcher "github.com/blevesearch/bleve/search/searcher"
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
		if options["maxReplicasAllowed"] != mgr.Options()["maxReplicasAllowed"] {
			return nil, fmt.Errorf("maxReplicasAllowed setting is at '%v',"+
				" but request is for '%v'", mgr.Options()["maxReplicasAllowed"],
				options["maxReplicasAllowed"])
		}

		// Validate bucketTypesAllowed
		if options["bucketTypesAllowed"] != mgr.Options()["bucketTypesAllowed"] {
			return nil, fmt.Errorf("bucketTypesAllowed setting is at '%v',"+
				" but request is for: '%v'", mgr.Options()["bucketTypesAllowed"],
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
			if err != nil || bleveMaxClauseCount < 0 {
				return nil, fmt.Errorf("illegal value for bleveMaxClauseCount: '%v'",
					options["bleveMaxClauseCount"])
			}
		}

		if options["bleveMaxResultWindow"] != "" {
			bleveMaxResultWindow, err := strconv.Atoi(options["bleveMaxResultWindow"])
			if err != nil || bleveMaxResultWindow < 0 {
				return nil, fmt.Errorf("illegal value for bleveMaxResultWindow: '%v'",
					options["bleveMaxResultWindow"])
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
	logLevelStr := h.mgr.Options()["logLevel"]
	if logLevelStr != "" {
		logLevel, _ := LogLevels[logLevelStr]
		log.SetLevel(log.LogLevel(logLevel))
	}

	// Update bleveMaxClauseCount if requested.
	bleveMaxClauseCountStr := h.mgr.Options()["bleveMaxClauseCount"]
	if bleveMaxClauseCountStr != "" {
		bleveMaxClauseCount, _ := strconv.Atoi(bleveMaxClauseCountStr)
		bleveSearcher.DisjunctionMaxClauseCount = bleveMaxClauseCount
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
	maxReplicasAllowed, _ := strconv.Atoi(h.mgr.Options()["maxReplicasAllowed"])
	bucketTypesAllowed := h.mgr.Options()["bucketTypesAllowed"]
	hideUI := h.mgr.Options()["hideUI"]
	vbuckets, _ := strconv.Atoi(h.mgr.Options()["vbuckets"])

	rv := struct {
		Status             string `json:"status"`
		MaxReplicasAllowed int    `json:"maxReplicasAllowed"`
		BucketTypesAllowed string `json:"bucketTypesAllowed"`
		HideUI             string `json:"hideUI"`
		VBuckets           int    `json:"vbuckets"`
	}{
		Status:             "ok",
		MaxReplicasAllowed: maxReplicasAllowed,
		BucketTypesAllowed: bucketTypesAllowed,
		HideUI:             hideUI,
		VBuckets:           vbuckets,
	}
	rest.MustEncode(w, rv)
}

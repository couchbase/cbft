// Copyright (c) 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You
// may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"expvar"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index/scorch"
	bleveMapping "github.com/blevesearch/bleve/mapping"
	bleveSearcher "github.com/blevesearch/bleve/search/searcher"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

func initBleveOptions(options map[string]string) error {
	bleveMapping.StoreDynamic = false
	bleveMapping.MappingJSONStrict = true
	bleveSearcher.DisjunctionMaxClauseCount = 1024

	bleveKVStoreMetricsAllow := options["bleveKVStoreMetricsAllow"]
	if bleveKVStoreMetricsAllow != "" {
		v, err := strconv.ParseBool(bleveKVStoreMetricsAllow)
		if err != nil {
			return err
		}

		cbft.BleveKVStoreMetricsAllow = v
	}

	bleveMaxOpsPerBatch := options["bleveMaxOpsPerBatch"]
	if bleveMaxOpsPerBatch != "" {
		v, err := strconv.Atoi(bleveMaxOpsPerBatch)
		if err != nil {
			return err
		}

		cbft.BleveMaxOpsPerBatch = v
	}

	bleveAnalysisQueueSize := runtime.NumCPU()

	bleveAnalysisQueueSizeStr := options["bleveAnalysisQueueSize"]
	if bleveAnalysisQueueSizeStr != "" {
		v, err := strconv.Atoi(bleveAnalysisQueueSizeStr)
		if err != nil {
			return err
		}

		if v > 0 {
			bleveAnalysisQueueSize = v
		} else {
			bleveAnalysisQueueSize = bleveAnalysisQueueSize - v
		}
	}

	if bleveAnalysisQueueSize < 1 {
		bleveAnalysisQueueSize = 1
	}

	bleve.Config.SetAnalysisQueueSize(bleveAnalysisQueueSize)

	// set scorch index's OnEvent callbacks using the app herder
	scorch.RegistryEventCallbacks["scorchEventCallbacks"] =
		ftsHerder.ScorchHerderOnEvent()

	scorch.RegistryAsyncErrorCallbacks["scorchAsyncErrorCallbacks"] =
		func(err error) {
			log.Fatalf("scorch AsyncError, treating this as fatal, err: %v", err)
		}

	return nil
}

// bleveExpvarsAgg holds pairs of {categoryPrefix, varName}, where
// the varName'ed stats will be aggregated across all pindexes.
var bleveExpvarsAgg [][]string = [][]string{
	// Category "a" is for "application".
	{"a", "TotUpdates"},
	{"a", "TotDeletes"},

	// Category "b" is for "batch".
	{"b", "TotBatches"},
	{"b", "TotBatchesEmpty"},
	{"b", "TotBatchIntroTime"},
	{"b", "MaxBatchIntroTime"},

	// Category "i" is for "introducer".
	{"i", "TotIntroducedItems"},
	{"i", "TotIntroducedSegmentsBatch"},
	{"i", "TotIntroducedSegmentsMerge"},

	// Category "p" is for "persister".
	{"p", "CurOnDiskFiles"},
}

// bleveExpvarsCalculated holds tuples of {categoryPrefix, varName,
// sourceCounterX, sourceCounterY}, where the varName's value will be
// calculated as "sourceCounterX - sourceCounterY", and aggregated
// across all pindexes.
var bleveExpvarsCalculated [][]string = [][]string{
	// Category "i" is for "introducer".
	{"i", "CurIntroduceSegment", "TotIntroduceSegmentBeg", "TotIntroduceSegmentEnd"},
	{"i", "CurIntroduceMerge", "TotIntroduceMergeBeg", "TotIntroduceMergeEnd"},

	// Category "m" is for "merger".
	{"m", "CurFileMergePlanTasks", "TotFileMergePlanTasks", "TotFileMergePlanTasksDone"},

	// Category "p" is for "persister".
	{"p", "CurMemMerge", "TotMemMergeZapBeg", "TotMemMergeZapEnd"},
	{"p", "CurPersisterSlowMergerPaused", "TotPersisterSlowMergerPause", "TotPersisterSlowMergerResume"},

	// Category "d" is for "DCP".
	{"d", "CurDataUpdate", "TotDataUpdateBeg", "TotDataUpdateEnd"},
	{"d", "CurDataDelete", "TotDataDeleteBeg", "TotDataDeleteEnd"},
	{"d", "CurExecuteBatch", "TotExecuteBatchBeg", "TotExecuteBatchEnd"},

	// Category "h" is for "herder".
	{"h", "CurOnBatchExecuteStart", "TotOnBatchExecuteStartBeg", "TotOnBatchExecuteStartEnd"},
	{"h", "CurWaiting", "TotWaitingIn", "TotWaitingOut"},
}

// runBleveExpvarsCooker runs a timer loop that occasionally adds
// processed or cooked bleve-related stats to expvars.
//
// Example with expvarmon tool, with cbft listening on port 9200...
//
//   expvarmon -ports=9200 -vars="stats.a_TotUpdates,stats.a_TotDeletes,stats.b_TotBatches,stats.b_TotBatchesEmpty,stats.d_CurExecuteBatch,stats.h_CurOnBatchExecuteStart,stats.h_CurWaiting,stats.i_CurIntroduceSegment,stats.i_CurIntroduceMerge,stats.i_TotIntroducedItems,stats.i_TotIntroducedSegmentsBatch,stats.i_TotIntroducedSegmentsMerge,duration:stats.b_AvgBatchIntroTime,duration:stats.b_MaxBatchIntroTime,stats.m_CurFileMergePlanTasks,stats.p_CurMemMerge,stats.p_CurPersisterSlowMergerPaused,stats.p_CurOnDiskFiles"
//
func runBleveExpvarsCooker(mgr *cbgt.Manager) {
	tickCh := time.Tick(5 * time.Second)
	for {
		<-tickCh

		vars := map[string]*expvar.Int{}

		addStats := func(m map[string]interface{}) {
			for _, agg := range bleveExpvarsAgg {
				if i, exists := m[agg[1]]; exists {
					k := agg[0] + "_" + agg[1]
					v := vars[k]
					if v == nil {
						v = &expvar.Int{}
						vars[k] = v
					}
					if strings.HasPrefix(agg[1], "Max") {
						if v.Value() < int64(i.(uint64)) {
							v.Set(int64(i.(uint64)))
						}
					} else { // Assume it's summable like "TotFooBar".
						v.Add(int64(i.(uint64)))
					}
				}
			}
		}

		addStatsCalculated := func(m map[string]interface{}) {
			for _, cur := range bleveExpvarsCalculated {
				if i, exists := m[cur[2]]; exists {
					k := cur[0] + "_" + cur[1]
					v := vars[k]
					if v == nil {
						v = &expvar.Int{}
						vars[k] = v
					}
					v.Add(int64(i.(uint64)) - int64(m[cur[3]].(uint64)))
				}
			}
		}

		_, pindexes := mgr.CurrentMaps()

		for _, pindex := range pindexes {
			df, ok := pindex.Dest.(*cbgt.DestForwarder)
			if !ok || df == nil {
				continue
			}
			bd, ok := df.DestProvider.(*cbft.BleveDest)
			if !ok || bd == nil {
				continue
			}
			sm, err := bd.StatsMap()
			if err != nil || sm == nil {
				continue
			}
			bis, exists := sm["bleveIndexStats"]
			if !exists || bis == nil {
				continue
			}
			bism, ok := bis.(map[string]interface{})
			if !ok || bism == nil {
				continue
			}
			i, exists := bism["index"]
			if !exists || i == nil {
				continue
			}
			m, ok := i.(map[string]interface{})
			if !ok || m == nil {
				continue
			}

			addStats(m)
			addStatsCalculated(m)
		}

		addStatsCalculated(cbft.AggregateBleveDestPartitionStats())
		addStatsCalculated(ftsHerder.Stats())

		for k, v := range vars {
			expvars.Set(k, v)
		}

		vb, exists := vars["b_TotBatches"]
		if exists && vb != nil {
			totBatches := vb.Value()
			if totBatches > 0 {
				v := expvar.Int{}
				v.Set(vars["b_TotBatchIntroTime"].Value() / totBatches)
				expvars.Set("b_AvgBatchIntroTime", &v)
			}
		}
	}
}

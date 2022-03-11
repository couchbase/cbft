// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"expvar"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	bleveMapping "github.com/blevesearch/bleve/v2/mapping"
	bleveSearcher "github.com/blevesearch/bleve/v2/search/searcher"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

var initAnalysisQueue sync.Once

func initBleveOptions(options map[string]string) error {
	bleveMapping.StoreDynamic = false
	bleveMapping.MappingJSONStrict = true

	scorch.DefaultPersisterNapTimeMSec = 2000 // ms
	scorch.DefaultPersisterNapUnderNumFiles = 1000

	bleveMaxClauseCount := options["bleveMaxClauseCount"]
	if bleveMaxClauseCount != "" {
		v, err := strconv.Atoi(bleveMaxClauseCount)
		if err != nil {
			return err
		}

		bleveSearcher.DisjunctionMaxClauseCount = v
	} else {
		bleveSearcher.DisjunctionMaxClauseCount = cbft.DefaultBleveMaxClauseCount
	}

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

	var err error
	var v int
	// bleveAnalysisQueueSize is a one time initialisation config.
	initAnalysisQueue.Do(func() {
		bleveAnalysisQueueSize := runtime.NumCPU()

		bleveAnalysisQueueSizeStr := options["bleveAnalysisQueueSize"]
		if bleveAnalysisQueueSizeStr != "" {
			v, err = strconv.Atoi(bleveAnalysisQueueSizeStr)
			if err != nil {
				return
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
	})

	if err != nil {
		return err
	}

	// set scorch index's OnEvent callbacks using the app herder
	scorch.RegistryEventCallbacks["scorchEventCallbacks"] =
		ftsHerder.ScorchHerderOnEvent()

	scorch.RegistryAsyncErrorCallbacks["scorchAsyncErrorCallbacks"] =
		func(err error) {
			var stackDump string
			if flags.DataDir != "" {
				stackDump = DumpStack(flags.DataDir,
					fmt.Sprintf("scorch AsyncError, treating this as fatal, err: %v", err))
			}
			log.Fatalf("scorch AsyncError, treating this as fatal, err: %v,"+
				" stack dump: %s", err, stackDump)
		}

	blevePersisterNapTimeMSec := options["blevePersisterNapTimeMSec"]
	if blevePersisterNapTimeMSec != "" {
		v, err := strconv.Atoi(blevePersisterNapTimeMSec)
		if err != nil {
			return err
		}

		scorch.DefaultPersisterNapTimeMSec = v
	}

	blevePersisterNapUnderNumFiles := options["blevePersisterNapUnderNumFiles"]
	if blevePersisterNapUnderNumFiles != "" {
		v, err := strconv.Atoi(blevePersisterNapUnderNumFiles)
		if err != nil {
			return err
		}

		scorch.DefaultPersisterNapUnderNumFiles = v
	}

	bleveBatchFlushDuration := options["bleveBatchFlushDuration"]
	if bleveBatchFlushDuration != "" {
		v, err := time.ParseDuration(bleveBatchFlushDuration)
		if err != nil {
			return err
		}

		cbft.BleveBatchFlushDuration = v
	}

	return nil
}

// ---------------------------------------------------------------

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

	// Category "m" is for "merger".
	{"m", "MaxFileMergeZapTime"},

	// Category "p" is for "persister".
	{"p", "CurOnDiskFiles"},
	{"p", "MaxMemMergeZapTime"},

	// Category "h" is for "herder".
	{"h", "TotQueriesRejected"},
}

// bleveExpvarsDeltas holds tuples of {categoryPrefix, varName,
// sourceCounterX, sourceCounterY}, where the varName's value will be
// calculated as "sourceCounterX - sourceCounterY".
var bleveExpvarsDeltas [][]string = [][]string{
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

// bleveExpvarsRatios holds tuples of {categoryPrefix, varName,
// sourceNumerator, sourceDenominator}, where the varName's value will
// be calculated as "sourceNumerator / sourceDenominator".
var bleveExpvarsRatios [][]string = [][]string{
	{"b", "AvgBatchIntroTime", "TotBatchIntroTime", "TotBatches"},
	{"p", "AvgMemMergeZapTime", "TotMemMergeZapTime", "TotMemMergeZapEnd"},
	{"m", "AvgFileMergeZapTime", "TotFileMergeZapTime", "TotFileMergeZapEnd"},
}

func init() {
	// Initialize bleveExpvarsAgg with the vars used for calculations.
	for _, specs := range [][][]string{bleveExpvarsDeltas, bleveExpvarsRatios} {
		for _, spec := range specs {
		LOOP_NAMES:
			for _, dependency := range spec[2:] {
				for _, aggSpec := range bleveExpvarsAgg {
					if aggSpec[1] == dependency {
						continue LOOP_NAMES
					}
				}
				bleveExpvarsAgg = append(bleveExpvarsAgg, []string{spec[0], dependency})
			}
		}
	}
}

// runBleveExpvarsCooker runs a timer loop that occasionally adds
// processed or cooked bleve-related stats to expvars.
//
// Example with expvarmon tool, with cbft listening on port 9200...
//
//   expvarmon -ports=9200 -vars="stats.a_TotUpdates,stats.a_TotDeletes,stats.b_TotBatches,stats.b_TotBatchesEmpty,stats.d_CurExecuteBatch,stats.h_CurOnBatchExecuteStart,stats.h_CurWaiting,stats.i_CurIntroduceSegment,stats.i_CurIntroduceMerge,stats.i_TotIntroducedItems,stats.i_TotIntroducedSegmentsBatch,stats.i_TotIntroducedSegmentsMerge,duration:stats.b_AvgBatchIntroTime,duration:stats.b_MaxBatchIntroTime,stats.p_CurPersisterSlowMergerPaused,stats.p_CurOnDiskFiles,stats.p_CurMemMerge,duration:stats.p_AvgMemMergeZapTime,duration:stats.p_MaxMemMergeZapTime,stats.m_CurFileMergePlanTasks,duration:stats.m_AvgFileMergeZapTime,duration:stats.m_MaxFileMergeZapTime"
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
		}

		addStats(cbft.AggregateBleveDestPartitionStats())

		addStats(ftsHerder.Stats())

		for _, spec := range bleveExpvarsDeltas {
			a, aexists := vars[spec[0]+"_"+spec[2]]
			b, bexists := vars[spec[0]+"_"+spec[3]]
			if aexists && bexists {
				delta := &expvar.Int{}
				delta.Set(a.Value() - b.Value())
				vars[spec[0]+"_"+spec[1]] = delta
			}
		}

		for _, spec := range bleveExpvarsRatios {
			n, nexists := vars[spec[0]+"_"+spec[2]] // Numerator.
			d, dexists := vars[spec[0]+"_"+spec[3]] // Denominator.
			if nexists && dexists && d.Value() > 0 {
				ratio := &expvar.Int{}
				ratio.Set(n.Value() / d.Value())
				vars[spec[0]+"_"+spec[1]] = ratio
			}
		}

		for k, v := range vars {
			expvars.Set(k, v)
		}
	}
}

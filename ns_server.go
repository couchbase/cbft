//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unsafe"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	"github.com/dustin/go-jsonpointer"
)

// Dump stats to log once every 5min. This stat is configurable
// through the ENV_OPTION: logStatsEvery
var LogEveryNStats = 300

var SourcePartitionSeqsSleepDefault = 10 * time.Second
var SourcePartitionSeqsCacheTimeoutDefault = 60 * time.Second

// Atomic counters to keep track of the number of active
// http and https limit listeners.
var TotHTTPLimitListenersOpened uint64
var TotHTTPLimitListenersClosed uint64
var TotHTTPSLimitListenersOpened uint64
var TotHTTPSLimitListenersClosed uint64

// Atomic counters to keep track of the number of active
// grpc and grpc-ssl listeners.
var TotGRPCListenersOpened uint64
var TotGRPCListenersClosed uint64
var TotGRPCSListenersOpened uint64
var TotGRPCSListenersClosed uint64

// App-herder pertinent atomic stats.
var TotHerderWaitingIn uint64
var TotHerderWaitingOut uint64
var TotHerderOnBatchExecuteStartBeg uint64
var TotHerderOnBatchExecuteStartEnd uint64
var TotHerderQueriesRejected uint64

// Atomic stat that tracks current memory acquired, not including
// HeapIdle (memory reclaimed); updated every second;
// Used by the app_herder to track memory consumption by process.
var currentMemoryUsed uint64

// Optional callback when current memory used has dropped since the
// last sampling.
var OnMemoryUsedDropped func(curMemoryUsed, prevMemoryUsed uint64)

// PartitionSeqsProvider represents source object that can provide
// partition seqs, such as some pindex or dest implementations.
type PartitionSeqsProvider interface {
	// Returned map is keyed by partition id.
	PartitionSeqs() (map[string]cbgt.UUIDSeq, error)
}

var statsNamePrefix = []byte("\"")
var statsNameSuffix = []byte("\":")

// NsStatsHandler is a REST handler that provides stats/metrics for
// consumption by ns_server
type NsStatsHandler struct {
	statsCount int64
	mgr        *cbgt.Manager
}

func NewNsStatsHandler(mgr *cbgt.Manager) *NsStatsHandler {
	return &NsStatsHandler{mgr: mgr}
}

func (h *NsStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	currentStatsCount := atomic.AddInt64(&h.statsCount, 1)
	initNsServerCaching(h.mgr)

	rd := getRecentInfo()
	if rd.err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve defs: %v", rd.err),
			http.StatusInternalServerError)
		return
	}

	nsIndexStats, err := gatherIndexesStats(h.mgr, rd, false)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("error in retrieving defs: %v", err),
			http.StatusInternalServerError)
		return
	}

	if LogEveryNStats != 0 && currentStatsCount%int64(LogEveryNStats) == 0 {
		go func() {
			var buf bytes.Buffer
			err := rest.WriteManagerStatsJSON(h.mgr, &buf, "")
			if err != nil {
				log.Warnf("error formatting managerStatsJSON for logs: %v", err)
			} else {
				log.Printf("managerStats: %s", buf.String())
			}

			statsJSON, err := json.MarshalIndent(nsIndexStats, "", "    ")
			if err != nil {
				log.Warnf("error formatting JSON for logs: %v", err)
				return
			}
			log.Printf("stats: %s", string(statsJSON))
		}()
	}

	rest.MustEncode(w, nsIndexStats)
}

// BucketsNsStatsHandler is a REST handler that provides the list
// of buckets which have partitions on the current node.
type BucketsNsStatsHandler struct {
	mgr *cbgt.Manager
}

func NewBucketsNsStatsHandler(mgr *cbgt.Manager) *BucketsNsStatsHandler {
	return &BucketsNsStatsHandler{mgr: mgr}
}

func (h *BucketsNsStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	_, pindexes := h.mgr.CurrentMaps()
	bucketsMap := make(map[string]struct{})

	for _, pindex := range pindexes {
		bucketsMap[pindex.SourceName] = struct{}{}
	}

	result := struct {
		IndexSourceNames map[string]struct{} `json:"indexSourceNames"`
	}{
		IndexSourceNames: bucketsMap,
	}

	rest.MustEncode(w, result)
}

// IndexNsStatsHandler is a REST handler that provides stats/metrics for
// for an index
type IndexNsStatsHandler struct {
	mgr *cbgt.Manager
}

func NewIndexNsStatsHandler(mgr *cbgt.Manager) *IndexNsStatsHandler {
	return &IndexNsStatsHandler{mgr: mgr}
}

func (h *IndexNsStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	initNsServerCaching(h.mgr)

	indexName := rest.IndexNameLookup(req)
	if indexName == "" {
		rest.ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	indexDef, _, err := h.mgr.GetIndexDef(indexName, false)
	if err != nil || indexDef == nil {
		rest.ShowError(w, req,
			fmt.Sprintf("unable to obtain index definition for `%v`, err: %v", indexName, err),
			http.StatusBadRequest)
		return
	}

	rd := getRecentInfo()
	if rd.err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve defs: %v", rd.err),
			http.StatusInternalServerError)
		return
	}

	var planPIndexes []*cbgt.PlanPIndex
	if rd.planPIndexes != nil {
		for _, planPIndex := range rd.planPIndexes.PlanPIndexes {
			if planPIndex.IndexName != indexName {
				continue
			}
			planPIndexes = append(planPIndexes, planPIndex)
		}
	}

	nsIndexStat, _ := gatherIndexStats(h.mgr, indexDef, planPIndexes, nil)

	rest.MustEncode(w, nsIndexStat)
}

// -----------------------------------------------------------------------------

type NSIndexStats map[string]map[string]interface{}

// MarshalJSON formats the index stats using the
// colon separated convention found in other
// stats consumed by ns_server
func (n NSIndexStats) MarshalJSON() ([]byte, error) {
	rv := make(map[string]interface{})
	for k, nsis := range n {
		for nsik, nsiv := range nsis {
			if k == "" {
				rv[nsik] = nsiv
			} else {
				rv[k+":"+nsik] = nsiv
			}
		}
	}
	return json.Marshal(rv)
}

var statkeys = []string{
	// pindex
	"doc_count",
	"timer_batch_store_count",

	// kv store
	"batch_merge_count",
	"iterator_next_count",
	"iterator_seek_count",
	"reader_get_count",
	"reader_multi_get_count",
	"reader_prefix_iterator_count",
	"reader_range_iterator_count",
	"writer_execute_batch_count",

	// feed
	"timer_opaque_set_count",
	"timer_rollback_count",
	"timer_data_update_count",
	"timer_data_delete_count",
	"timer_snapshot_start_count",
	"timer_opaque_get_count",

	// --------------------------------------------
	// stats from "FTS Stats" spec, see:
	// https://docs.google.com/spreadsheets/d/1w8P68gLBIs0VUN4egUuUH6U_92_5xi9azfvH8pPw21s/edit#gid=104567684

	"num_mutations_to_index", // per-index stat.
	// "doc_count",           // per-index stat (see above).
	"total_bytes_indexed",             // per-index stat.
	"num_bytes_written_at_index_time", // per-index stat.
	"num_bytes_read_at_query_time",    //per-index stat
	"num_recs_to_persist",             // per-index stat.

	"num_bytes_used_disk",                     // per-index stat.
	"num_bytes_used_disk_by_root",             // per-index stat.
	"num_files_on_disk",                       // per-index stat.
	"num_bytes_live_data",                     // per-index stat, not in spec
	"num_bytes_used_disk_by_root_reclaimable", // per-index stat.
	// "num_bytes_used_ram" -- PROCESS-LEVEL stat.

	"num_pindexes_actual", // per-index stat.
	"num_pindexes_target", // per-index stat.

	"num_root_memorysegments", // per-index stat.
	"num_root_filesegments",   // per-index stat.

	"num_persister_nap_pause_completed", // per-index stat.
	"num_persister_nap_merger_break",    // per-index stat.

	"total_compactions",              // per-index stat.
	"total_compaction_written_bytes", // per-index stat.

	// "total_gc"   -- PROCESS-LEVEL stat.
	// "pct_cpu_gc" -- PROCESS-LEVEL stat.

	"total_queries",                // per-index stat.
	"avg_queries_latency",          // per-index stat.
	"total_internal_queries",       // per-index stat.
	"avg_internal_queries_latency", // per-index stat.
	"total_request_time",           // per-index stat.
	"total_queries_slow",           // per-index stat.
	"total_queries_timeout",        // per-index stat.
	"total_queries_error",          // per-index stat.

	"total_grpc_queries",                // per-index stat.
	"avg_grpc_queries_latency",          // per-index stat.
	"total_grpc_internal_queries",       // per-index stat.
	"avg_grpc_internal_queries_latency", // per-index stat.
	"total_grpc_request_time",           // per-index stat.
	"total_grpc_queries_slow",           // per-index stat.
	"total_grpc_queries_timeout",        // per-index stat.
	"total_grpc_queries_error",          // per-index stat.

	"total_bytes_query_results",     // per-index stat.
	"total_term_searchers",          // per-index stat.
	"total_term_searchers_finished", // per-index stat.

	// "curr_batches_blocked_by_herder"   -- PROCESS-LEVEL stat.
	// "total_queries_rejected_by_herder" -- PROCESS-LEVEL stat
}

// NewIndexStat ensures that all index stats
// have the same shape and 0 values to
// prevent seeing N/A in ns_server UI
func NewIndexStat() map[string]interface{} {
	rv := make(map[string]interface{})
	for _, key := range statkeys {
		rv[key] = float64(0)
	}
	return rv
}

func getRecentInfo() *recentInfo {
	return (*recentInfo)(atomic.LoadPointer(&lastRecentInfo))
}

func gatherIndexesStats(mgr *cbgt.Manager, rd *recentInfo,
	collAware bool) (NSIndexStats, error) {
	if rd == nil || mgr == nil {
		return nil, nil
	}

	indexDefsMap := rd.indexDefsMap
	planPIndexes := rd.planPIndexes
	nodeUUID := mgr.UUID()

	nsIndexStats := make(NSIndexStats, len(indexDefsMap))

	indexNameToSourceName := map[string]string{}

	indexNameToPlanPIndexes := map[string][]*cbgt.PlanPIndex{}
	if planPIndexes != nil {
		for _, planPIndex := range planPIndexes.PlanPIndexes {
			// Only focus on the planPIndex entries for this node.
			if planPIndex.Nodes[nodeUUID] != nil {
				indexNameToPlanPIndexes[planPIndex.IndexName] =
					append(indexNameToPlanPIndexes[planPIndex.IndexName], planPIndex)
			}
		}
	}

	// Keyed by sourceName, sub-key is source partition id.
	sourceNameToSourcePartitionSeqs := map[string]map[string]cbgt.UUIDSeq{}

	for indexName, indexDef := range indexDefsMap {
		var key string
		if collAware {
			key = getNsIndexStatsKey(indexName, indexDef.SourceName, indexDef.Params)
		} else {
			key = indexDef.SourceName + ":" + indexName
		}
		nsIndexStat, err := gatherIndexStats(
			mgr,
			indexDef,
			indexNameToPlanPIndexes[indexName],
			sourceNameToSourcePartitionSeqs,
		)
		if err != nil {
			continue
		}
		nsIndexStats[key] = nsIndexStat
		indexNameToSourceName[indexName] = key

	}

	nsIndexStats["regulatorStats"] = GetRegulatorStats()

	if !collAware {
		nsIndexStats[""] = gatherTopLevelStats(mgr, rd)
	}

	return nsIndexStats, nil
}

func gatherIndexStats(
	mgr *cbgt.Manager,
	indexDef *cbgt.IndexDef,
	planPIndexes []*cbgt.PlanPIndex,
	sourceNameToSourcePartitionSeqs map[string]map[string]cbgt.UUIDSeq,
) (map[string]interface{}, error) {
	if indexDef == nil {
		return nil, fmt.Errorf("gatherIndexStats: indexDef cannot be nil")
	}

	nsIndexStat := NewIndexStat()

	var totalQueries, totalInternalQueries uint64
	var totClientRequestTimeNS, totInternalRequestTimeNS uint64
	var totRequestTimeNS, totRequestSlow, totRequestTimeout, totRequestErr, totResponseBytes uint64

	for k := range queryPaths {
		// Obtain focus stats for all query paths supported
		indexQueryPathStats := MapRESTPathStats[k]

		focusStats := indexQueryPathStats.FocusStats(indexDef.Name)
		if focusStats != nil {
			totalQueries += atomic.LoadUint64(&focusStats.TotClientRequest)
			totClientRequestTimeNS += atomic.LoadUint64(&focusStats.TotClientRequestTimeNS)
			totalInternalQueries += atomic.LoadUint64(&focusStats.TotInternalRequest)
			totInternalRequestTimeNS += atomic.LoadUint64(&focusStats.TotInternalRequestTimeNS)
			totRequestTimeNS += atomic.LoadUint64(&focusStats.TotRequestTimeNS)
			totRequestSlow += atomic.LoadUint64(&focusStats.TotRequestSlow)
			totRequestTimeout += atomic.LoadUint64(&focusStats.TotRequestTimeout)
			totRequestErr += atomic.LoadUint64(&focusStats.TotRequestErr)
			totResponseBytes += atomic.LoadUint64(&focusStats.TotResponseBytes)
		}
	}

	nsIndexStat["total_queries"] = totalQueries
	if totalQueries > 0 {
		// Convert from nanosecs to millisecs for avg_queries_latency.
		nsIndexStat["avg_queries_latency"] =
			float64(totClientRequestTimeNS/totalQueries) / 1000000.0
	}
	nsIndexStat["total_internal_queries"] = totalInternalQueries
	if totalInternalQueries > 0 {
		// Convert from nanosecs to millisecs for avg_internal_queries_latency.
		nsIndexStat["avg_internal_queries_latency"] =
			float64(totInternalRequestTimeNS/totalInternalQueries) / 1000000.0
	}
	nsIndexStat["total_request_time"] = totRequestTimeNS
	nsIndexStat["total_queries_slow"] = totRequestSlow
	nsIndexStat["total_queries_timeout"] = totRequestTimeout
	nsIndexStat["total_queries_error"] = totRequestErr
	nsIndexStat["total_bytes_query_results"] = totResponseBytes

	nsIndexStat["num_pindexes_target"] = uint64(len(planPIndexes))

	rpcFocusStats := GrpcPathStats.FocusStats(indexDef.Name)
	if rpcFocusStats != nil {
		totalQueries := atomic.LoadUint64(&rpcFocusStats.TotGrpcRequest)
		nsIndexStat["total_grpc_queries"] = totalQueries
		if totalQueries > 0 {
			nsIndexStat["avg_grpc_queries_latency"] =
				float64((atomic.LoadUint64(&rpcFocusStats.TotGrpcRequestTimeNS) /
					totalQueries)) / 1000000.0 // Convert from nanosecs to millisecs.
		}
		totalInternalQueries := atomic.LoadUint64(&rpcFocusStats.TotGrpcInternalRequest)
		nsIndexStat["total_grpc_internal_queries"] = totalInternalQueries
		if totalInternalQueries > 0 {
			nsIndexStat["avg_grpc_internal_queries_latency"] =
				float64(atomic.LoadUint64(&rpcFocusStats.TotGrpcInternalRequestTimeNS)/
					totalInternalQueries) / 1000000.0 // Convert from nanosecs to millisecs.
		}

		nsIndexStat["total_grpc_request_time"] =
			atomic.LoadUint64(&rpcFocusStats.TotGrpcRequestTimeNS)
		nsIndexStat["total_grpc_queries_slow"] =
			atomic.LoadUint64(&rpcFocusStats.TotGrpcRequestSlow)
		nsIndexStat["total_grpc_queries_timeout"] =
			atomic.LoadUint64(&rpcFocusStats.TotGrpcRequestTimeout)
		nsIndexStat["total_grpc_queries_error"] =
			atomic.LoadUint64(&rpcFocusStats.TotGrpcRequestErr)
	}

	nsIndexStat["last_access_time"] =
		querySupervisor.GetLastAccessTimeForIndex(indexDef.Name)

	feeds, pindexes := mgr.CurrentMaps()
	for _, feed := range feeds {
		if feed.IndexName() != indexDef.Name {
			continue
		}

		// automatically process all the feed stats
		_ = addFeedStats(feed, nsIndexStat)
	}

	srcPartitionSeqs, exists := sourceNameToSourcePartitionSeqs[indexDef.SourceName]
	if !exists {
		feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
		if !exists || feedType == nil || feedType.PartitionSeqs == nil {
			return nsIndexStat, nil
		}

		srcPartitionSeqs = GetSourcePartitionSeqs(SourceSpec{
			SourceType:   indexDef.SourceType,
			SourceName:   indexDef.SourceName,
			SourceUUID:   indexDef.SourceUUID,
			SourceParams: indexDef.SourceParams,
			Server:       mgr.Server(),
		})
		if srcPartitionSeqs != nil && sourceNameToSourcePartitionSeqs != nil {
			sourceNameToSourcePartitionSeqs[indexDef.SourceName] = srcPartitionSeqs
		}
	}

	destPartitionSeqs := map[string]cbgt.UUIDSeq{}
	for _, pindex := range pindexes {
		if pindex.IndexName != indexDef.Name {
			continue
		}

		oldValue, ok := nsIndexStat["num_pindexes_actual"]
		if ok {
			switch oldValue := oldValue.(type) {
			case float64:
				oldValue += float64(1)
				nsIndexStat["num_pindexes_actual"] = oldValue
			}
		}

		// automatically process all the pindex dest stats
		_ = addPIndexStats(pindex, nsIndexStat)

		if pindex.Dest != nil {
			destForwarder, ok := pindex.Dest.(*cbgt.DestForwarder)
			if !ok {
				continue
			}

			partitionSeqsProvider, ok :=
				destForwarder.DestProvider.(PartitionSeqsProvider)
			if !ok {
				continue
			}

			if partitionSeqs, err := partitionSeqsProvider.PartitionSeqs(); err == nil {
				for partitionId, uuidSeq := range partitionSeqs {
					destPartitionSeqs[partitionId] = uuidSeq
				}
			}
		}
	}

	if totSeqReceived, numMutationsToIndex, err := obtainDestSeqsForIndex(
		indexDef,
		srcPartitionSeqs,
		destPartitionSeqs,
	); err == nil {
		nsIndexStat["tot_seq_received"] = totSeqReceived
		nsIndexStat["num_mutations_to_index"] = numMutationsToIndex
	}

	return nsIndexStat, nil
}

// Utility function obtains the following for an index definition from
// it's source's and destination stats:
//   - tot_seq_received
//   - num_mutations_to_index
func obtainDestSeqsForIndex(indexDef *cbgt.IndexDef,
	srcPartitionSeqs map[string]cbgt.UUIDSeq,
	destPartitionSeqs map[string]cbgt.UUIDSeq) (uint64, uint64, error) {
	var scope string
	var collections []string

	// obtain collections of interest from index definition only if
	// srcPartitionSeqs was provided; skip determining source stats for
	// the index otherwise
	if len(srcPartitionSeqs) > 0 {
		var err error
		scope, collections = metaFieldValCache.getScopeCollectionNames(indexDef.Name)
		if len(scope) == 0 || len(collections) == 0 {
			scope, collections, err = GetScopeCollectionsFromIndexDef(indexDef)
			if err != nil {
				return 0, 0, err
			}
		}
	}

	var totDestSeq, totSrcSeq uint64
	for partitionId, dstUUIDSeq := range destPartitionSeqs {
		var srcSeq uint64
		// the following loop is skipped if srcPartitionSeqs isn't available
		for i := range collections {
			uuidHighSeq, exists :=
				srcPartitionSeqs[partitionId+":"+scope+":"+collections[i]]
			if !exists {
				continue
			}

			// account this collection's high seq only if it actually holds
			// any items and is greater than the last accounted value
			if uuidHighSeq.Seq > srcSeq {
				srcSeq = uuidHighSeq.Seq
			}

		}

		if srcSeq > 0 {
			totSrcSeq += srcSeq
		}

		totDestSeq += dstUUIDSeq.Seq
	}

	if totSrcSeq >= totDestSeq {
		return totDestSeq, totSrcSeq - totDestSeq, nil
	}

	return totDestSeq, 0, nil
}

func getMemoryUtilization(memStats runtime.MemStats) uint64 {
	// Memory utilization to account for:
	// - memory alloced for the process
	// - memory released by process back to the OS
	// - memory freed by the process but NOT released back to the OS just yet
	//
	// Per https://pkg.go.dev/runtime#MemStats
	// - Sys is the total bytes of memory obtained from the OS.
	// - (HeapIdle-HeapReleased) estimates the amount of memory that
	//   could be returned to the OS.
	// - HeapReleased is bytes of physical memory returned to the OS.
	// So our equation here should be ..
	// 	Sys - (HeapIdle - HeapReleased) - HeapReleased
	return memStats.Sys - memStats.HeapIdle
}

func gatherNodeUtilStats(mgr *cbgt.Manager,
	rv map[string]interface{}) {
	var totalUnitsMetered uint64
	if val, exists := GetRegulatorStats()["total_units_metered"]; exists {
		totalUnitsMetered = val.(uint64)
	}

	options := mgr.Options()
	rd := getRecentInfo()

	if cpu, err := currentCPUPercent(); err == nil {
		numCPU, _ := strconv.ParseFloat(options["ftsCpuQuota"], 64)
		rv["utilization:cpuPercent"] = DetermineNewAverage(
			"cpuPercent", uint64(cpu/numCPU))
	}

	rv["utilization:billableUnitsRate"] = DetermineNewAverage(
		"totalUnitsMetered", totalUnitsMetered)

	rv["utilization:memoryBytes"] = DetermineNewAverage(
		"memoryBytes", getMemoryUtilization(rd.memStats))

	var size int64
	_ = filepath.Walk(mgr.DataDir(), func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	rv["utilization:diskBytes"] = uint64(size)

	if val, exists := options["resourceUtilizationHighWaterMark"]; exists {
		if valFloat64, err := strconv.ParseFloat(val, 64); err == nil {
			rv["resourceUtilizationHighWaterMark"] = valFloat64
		}
	}
	if val, exists := options["resourceUtilizationLowWaterMark"]; exists {
		if valFloat64, err := strconv.ParseFloat(val, 64); err == nil {
			rv["resourceUtilizationLowWaterMark"] = valFloat64
		}
	}
	if val, exists := options["resourceUnderUtilizationWaterMark"]; exists {
		if valFloat64, err := strconv.ParseFloat(val, 64); err == nil {
			rv["resourceUnderUtilizationWaterMark"] = valFloat64
		}
	}

	if val, exists := options["maxBillableUnitsRate"]; exists {
		if valUint64, err := strconv.ParseUint(val, 10, 64); err == nil {
			rv["limits:billableUnitsRate"] = valUint64
		}
	}
	if val, exists := options["maxDiskBytes"]; exists {
		if valUint64, err := strconv.ParseUint(val, 10, 64); err == nil {
			rv["limits:diskBytes"] = valUint64
		}
	}
	if val, exists := options["ftsMemoryQuota"]; exists {
		if valUint64, err := strconv.ParseUint(val, 10, 64); err == nil {
			rv["limits:memoryBytes"] = valUint64
		}
	}
}

func gatherTopLevelStats(mgr *cbgt.Manager, rd *recentInfo) map[string]interface{} {
	topLevelStats := map[string]interface{}{}

	topLevelStats["num_bytes_used_ram"] = getMemoryUtilization(rd.memStats)
	topLevelStats["total_gc"] = rd.memStats.NumGC
	topLevelStats["pct_cpu_gc"] = rd.memStats.GCCPUFraction
	topLevelStats["tot_remote_http"] = atomic.LoadUint64(&totRemoteHttp)
	topLevelStats["tot_remote_http2"] = atomic.LoadUint64(&totRemoteHttpSsl) // deprecated
	topLevelStats["tot_remote_http_ssl"] = atomic.LoadUint64(&totRemoteHttpSsl)
	topLevelStats["tot_queryreject_on_memquota"] =
		atomic.LoadUint64(&totQueryRejectOnNotEnoughQuota)

	topLevelStats["tot_http_limitlisteners_opened"] =
		atomic.LoadUint64(&TotHTTPLimitListenersOpened)
	topLevelStats["tot_http_limitlisteners_closed"] =
		atomic.LoadUint64(&TotHTTPLimitListenersClosed)
	topLevelStats["tot_https_limitlisteners_opened"] =
		atomic.LoadUint64(&TotHTTPSLimitListenersOpened)
	topLevelStats["tot_https_limitlisteners_closed"] =
		atomic.LoadUint64(&TotHTTPSLimitListenersClosed)

	topLevelStats["tot_remote_grpc"] = atomic.LoadUint64(&totRemoteGrpc)
	topLevelStats["tot_remote_grpc_tls"] = atomic.LoadUint64(&totRemoteGrpcSsl) // deprecated
	topLevelStats["tot_remote_grpc_ssl"] = atomic.LoadUint64(&totRemoteGrpcSsl)
	topLevelStats["tot_grpc_queryreject_on_memquota"] =
		atomic.LoadUint64(&totGrpcQueryRejectOnNotEnoughQuota)

	topLevelStats["tot_grpc_listeners_opened"] =
		atomic.LoadUint64(&TotGRPCListenersOpened)
	topLevelStats["tot_grpc_listeners_closed"] =
		atomic.LoadUint64(&TotGRPCListenersClosed)
	topLevelStats["tot_grpcs_listeners_opened"] =
		atomic.LoadUint64(&TotGRPCSListenersOpened)
	topLevelStats["tot_grpcs_listeners_closed"] =
		atomic.LoadUint64(&TotGRPCSListenersClosed)

	topLevelStats["batch_bytes_added"] = atomic.LoadUint64(&BatchBytesAdded)
	topLevelStats["batch_bytes_removed"] = atomic.LoadUint64(&BatchBytesRemoved)
	topLevelStats["num_batches_introduced"] = atomic.LoadUint64(&NumBatchesIntroduced)

	topLevelStats["tot_batches_flushed_on_maxops"] = atomic.LoadUint64(&TotBatchesFlushedOnMaxOps)
	topLevelStats["tot_batches_flushed_on_timer"] = atomic.LoadUint64(&TotBatchesFlushedOnTimer)
	topLevelStats["tot_batches_new"] = atomic.LoadUint64(&TotBatchesNew)
	topLevelStats["tot_batches_merged"] = atomic.LoadUint64(&TotBatchesMerged)

	topLevelStats["tot_bleve_dest_opened"] = atomic.LoadUint64(&TotBleveDestOpened)
	topLevelStats["tot_bleve_dest_closed"] = atomic.LoadUint64(&TotBleveDestClosed)

	topLevelStats["tot_rollback_full"] = atomic.LoadUint64(&TotRollbackFull)
	topLevelStats["tot_rollback_partial"] = atomic.LoadUint64(&TotRollbackPartial)

	topLevelStats["curr_batches_blocked_by_herder"] =
		atomic.LoadUint64(&TotHerderOnBatchExecuteStartEnd) -
			atomic.LoadUint64(&TotHerderOnBatchExecuteStartBeg)
	topLevelStats["total_queries_rejected_by_herder"] =
		atomic.LoadUint64(&TotHerderQueriesRejected)

	topLevelStats["num_gocbcore_dcp_agents"] = cbgt.NumDCPAgents()
	topLevelStats["num_gocbcore_stats_agents"] = cbgt.NumStatsAgents()

	if ServerlessMode {
		gatherNodeUtilStats(mgr, topLevelStats)
	}

	return topLevelStats
}

func addFeedStats(feed cbgt.Feed, nsIndexStat map[string]interface{}) error {
	buffer := new(bytes.Buffer)
	err := feed.Stats(buffer)
	if err != nil {
		return err
	}
	return massageStats(buffer, nsIndexStat)
}

func addPIndexStats(pindex *cbgt.PIndex, nsIndexStat map[string]interface{}) error {
	if destForwarder, ok := pindex.Dest.(*cbgt.DestForwarder); ok {
		if bp, ok := destForwarder.DestProvider.(*BleveDest); ok {
			bpsm, err := bp.StatsMap()
			if err != nil {
				return err
			}
			return extractStats(bpsm, nsIndexStat)
		}
	}
	return nil
}

func updateStat(name string, val float64, nsIndexStat map[string]interface{}) {
	oldValue, ok := nsIndexStat[name]
	if ok {
		switch oldValue := oldValue.(type) {
		case float64:
			oldValue += val
			nsIndexStat[name] = oldValue
		}
	}
}

func extractStats(bpsm, nsIndexStat map[string]interface{}) error {
	// common stats across different index types
	v := jsonpointer.Get(bpsm, "/DocCount")
	if vuint64, ok := v.(uint64); ok {
		updateStat("doc_count", float64(vuint64), nsIndexStat)
	}
	v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/term_searchers_started")
	if vuint64, ok := v.(uint64); ok {
		updateStat("total_term_searchers", float64(vuint64), nsIndexStat)
	}
	v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/term_searchers_finished")
	if vuint64, ok := v.(uint64); ok {
		updateStat("total_term_searchers_finished", float64(vuint64), nsIndexStat)
	}
	v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/num_plain_text_bytes_indexed")
	if vuint64, ok := v.(uint64); ok {
		updateStat("total_bytes_indexed", float64(vuint64), nsIndexStat)
	}

	v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/kv")
	if _, ok := v.(map[string]interface{}); ok {
		// see if metrics are enabled, they would always be at the top-level
		v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/kv/metrics")
		if metrics, ok := v.(map[string]interface{}); ok {
			extractMetricsStats(metrics, nsIndexStat)
			// look for kv stats
			v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/kv/kv")
			if kvStats, ok := v.(map[string]interface{}); ok {
				extractKVStats(kvStats, nsIndexStat)
			}
		} else {
			// look for kv here
			v = jsonpointer.Get(bpsm, "/bleveIndexStats/index/kv")
			if kvStats, ok := v.(map[string]interface{}); ok {
				extractKVStats(kvStats, nsIndexStat)
			}
		}
	} else {
		// scorch stats are available at bleveIndexStats/index
		v = jsonpointer.Get(bpsm, "/bleveIndexStats/index")
		if sstats, ok := v.(map[string]interface{}); ok {
			extractScorchStats(sstats, nsIndexStat)
		}
	}

	return nil
}

var metricStats = map[string]string{
	"/batch_merge/count":            "batch_merge_count",
	"/iterator_next/count":          "iterator_next_count",
	"/iterator_seek/count":          "iterator_seek_count",
	"/reader_get/count":             "reader_get_count",
	"/reader_multi_get/count":       "reader_multi_get_count",
	"/reader_prefix_iterator/count": "reader_prefix_iterator_count",
	"/reader_range_iterator/count":  "reader_range_iterator_count",
	"/writer_execute_batch/count":   "writer_execute_batch_count",
}

func extractMetricsStats(metrics, nsIndexStat map[string]interface{}) error {
	for path, statname := range metricStats {
		v := jsonpointer.Get(metrics, path)
		if vint64, ok := v.(int64); ok {
			updateStat(statname, float64(vint64), nsIndexStat)
		}
	}
	return nil
}

var kvStats = map[string]string{
	"/num_bytes_used_disk":            "num_bytes_used_disk",
	"/total_compaction_written_bytes": "total_compaction_written_bytes",
	"/num_files":                      "num_files_on_disk",
}

func extractKVStats(kvs, nsIndexStat map[string]interface{}) error {
	for path, statname := range kvStats {
		v := jsonpointer.Get(kvs, path)
		if vint, ok := v.(int); ok {
			updateStat(statname, float64(vint), nsIndexStat)
		} else if vuint64, ok := v.(uint64); ok {
			updateStat(statname, float64(vuint64), nsIndexStat)
		}
	}
	return nil
}

var scorchStats = map[string]string{
	"/num_bytes_used_disk":                     "num_bytes_used_disk",
	"/num_bytes_used_disk_by_root":             "num_bytes_used_disk_by_root",
	"/num_files_on_disk":                       "num_files_on_disk",
	"/total_compaction_written_bytes":          "total_compaction_written_bytes",
	"/num_recs_to_persist":                     "num_recs_to_persist",
	"/num_root_memorysegments":                 "num_root_memorysegments",
	"/num_root_filesegments":                   "num_root_filesegments",
	"/num_persister_nap_pause_completed":       "num_persister_nap_pause_completed",
	"/num_persister_nap_merger_break":          "num_persister_nap_merger_break",
	"/num_bytes_used_disk_by_root_reclaimable": "num_bytes_used_disk_by_root_reclaimable",
	"/num_bytes_read_at_query_time":            "num_bytes_read_at_query_time",
	"/num_bytes_written_at_index_time":         "num_bytes_written_at_index_time",
}

func extractScorchStats(sstats, nsIndexStat map[string]interface{}) error {
	for path, statname := range scorchStats {
		v := jsonpointer.Get(sstats, path)
		if vuint64, ok := v.(uint64); ok {
			updateStat(statname, float64(vuint64), nsIndexStat)
		} else if fuint64, ok := v.(float64); ok {
			updateStat(statname, float64(fuint64), nsIndexStat)
		}
	}

	return nil
}

func massageStats(buffer *bytes.Buffer, nsIndexStat map[string]interface{}) error {
	statsBytes := buffer.Bytes()
	pointers, err := jsonpointer.ListPointers(statsBytes)
	if err != nil {
		return err
	}

	countPointers := make([]string, 0)
	for _, pointer := range pointers {
		if strings.HasSuffix(pointer, "/count") ||
			strings.HasSuffix(pointer, "/DocCount") ||
			matchAnyFixedSuffixes(pointer) {
			countPointers = append(countPointers, pointer)
		}
	}

	countValueMap, err := jsonpointer.FindMany(statsBytes, countPointers)
	if err != nil {
		return err
	}

	for k, v := range countValueMap {
		statName := convertStatName(k)
		var statValue float64
		err := json.Unmarshal(v, &statValue)
		if err != nil {
			return err
		}
		oldValue, ok := nsIndexStat[statName]
		if ok {
			switch oldValue := oldValue.(type) {
			case float64:
				oldValue += statValue
				nsIndexStat[statName] = oldValue
			}
		}
	}

	return nil
}

var fixedSuffixToStatNameMapping = map[string]string{
	"compacts":                     "total_compactions",
	"term_searchers_started":       "total_term_searchers",
	"term_searchers_finished":      "total_term_searchers_finished",
	"estimated_space_used":         "num_bytes_used_disk",
	"CurDirtyOps":                  "num_recs_to_persist",
	"num_plain_text_bytes_indexed": "total_bytes_indexed",
}

func matchAnyFixedSuffixes(pointer string) bool {
	for k := range fixedSuffixToStatNameMapping {
		if strings.HasSuffix(pointer, "/"+k) {
			return true
		}
	}
	return false
}

func convertStatName(key string) string {
	lastSlash := strings.LastIndex(key, "/")
	if lastSlash < 0 {
		return "unknown"
	}
	statSuffix := key[lastSlash+1:]
	if fixedStatName, ok := fixedSuffixToStatNameMapping[statSuffix]; ok {
		return fixedStatName
	}
	statNameStart := strings.LastIndex(key[:lastSlash], "/")
	if statNameStart < 0 {
		return "unknown"
	}
	statPrefix := key[statNameStart+1 : lastSlash]
	if statPrefix == "basic" {
		statPrefix = ""
	}
	statName := camelToUnderscore(statSuffix)
	if statPrefix != "" {
		statName = camelToUnderscore(statPrefix) + "_" + statName
	}
	return statName
}

func camelToUnderscore(name string) string {
	rv := ""
	for i, r := range name {
		if unicode.IsUpper(r) && i != 0 {
			rv += "_"
			rv += string(unicode.ToLower(r))
		} else if unicode.IsUpper(r) && i == 0 {
			rv += string(unicode.ToLower(r))
		} else {
			rv += string(r)
		}
	}
	return rv
}

// ---------------------------------------------------

// NsStatusHandler is a REST handler that provides status for
// consumption by ns_server
type NsStatusHandler struct {
	mgr       *cbgt.Manager
	serverURL *url.URL
}

func NewNsStatusHandler(mgr *cbgt.Manager, server string) (*NsStatusHandler, error) {
	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}

	return &NsStatusHandler{
		mgr:       mgr,
		serverURL: serverURL,
	}, nil
}

func NsHostsForIndex(name string, planPIndexes *cbgt.PlanPIndexes,
	nodeDefs *cbgt.NodeDefs) []string {
	if planPIndexes == nil {
		return nil
	}

	v := struct{}{}

	// find the node UUIDs related to this index
	nodes := make(map[string]struct{})
	for _, planPIndex := range planPIndexes.PlanPIndexes {
		if planPIndex.IndexName == name {
			for planPIndexNodeUUID := range planPIndex.Nodes {
				nodes[planPIndexNodeUUID] = v
			}
		}
	}

	// look for all these nodes in the nodes wanted
	nodeExtras := make(map[string]string) // Keyed by node UUID.
	for _, nodeDef := range nodeDefs.NodeDefs {
		_, ok := nodes[nodeDef.UUID]
		if ok {
			extras, err := ParseExtras(nodeDef.Extras)
			if err == nil {
				nodeExtras[nodeDef.UUID] = extras["nsHostPort"]
			}
		}
	}

	// build slice of node extras
	nodeStrings := make(sort.StringSlice, 0, len(nodeExtras))
	for _, nodeExtra := range nodeExtras {
		nodeStrings = append(nodeStrings, nodeExtra)
	}

	// sort slice for stability
	sort.Sort(nodeStrings)

	return nodeStrings
}

func (h *NsStatusHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	initNsServerCaching(h.mgr)

	rd := getRecentInfo()
	if rd.err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve defs: %v", rd.err),
			http.StatusInternalServerError)
		return
	}

	indexDefsMap := rd.indexDefsMap
	nodeDefs := rd.nodeDefs
	planPIndexes := rd.planPIndexes

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(cbgt.JsonOpenBrace)
	w.Write(statsNamePrefix)
	w.Write([]byte("status"))
	w.Write(statsNameSuffix)
	w.Write([]byte("["))

	indexDefNames := make(sort.StringSlice, 0, len(indexDefsMap))
	for indexDefName := range indexDefsMap {
		indexDefNames = append(indexDefNames, indexDefName)
	}

	sort.Sort(indexDefNames)

	for i, indexDefName := range indexDefNames {
		indexDef := indexDefsMap[indexDefName]
		if i > 0 {
			w.Write(cbgt.JsonComma)
		}

		rest.MustEncode(w, struct {
			Hosts  []string `json:"hosts"`
			Bucket string   `json:"bucket"`
			Name   string   `json:"name"`
		}{
			Bucket: indexDef.SourceName,
			Name:   indexDefName,
			Hosts:  NsHostsForIndex(indexDefName, planPIndexes, nodeDefs),
		})
	}

	w.Write([]byte("],"))
	w.Write(statsNamePrefix)
	w.Write([]byte("code"))
	w.Write(statsNameSuffix)
	w.Write([]byte("\"success\""))
	w.Write(cbgt.JsonCloseBrace)
}

// ---------------------------------------------------------------

type SourceSpec struct {
	SourceType   string
	SourceName   string
	SourceUUID   string
	SourceParams string
	Server       string
}

type SourcePartitionSeqs struct {
	PartitionSeqs map[string]cbgt.UUIDSeq
	LastUpdated   time.Time
	LastUsed      time.Time
}

var mapSourcePartitionSeqs = map[SourceSpec]*SourcePartitionSeqs{}
var mapSourcePartitionSeqsM sync.Mutex
var runSourcePartitionSeqsOnce sync.Once

func initNsServerCaching(mgr *cbgt.Manager) {
	runSourcePartitionSeqsOnce.Do(func() {
		go RunSourcePartitionSeqs(mgr.Options(), nil)
		go RunRecentInfoCache(mgr)
	})
}

func GetSourcePartitionSeqs(sourceSpec SourceSpec) map[string]cbgt.UUIDSeq {
	mapSourcePartitionSeqsM.Lock()
	s := mapSourcePartitionSeqs[sourceSpec]
	if s == nil {
		s = &SourcePartitionSeqs{}
		mapSourcePartitionSeqs[sourceSpec] = s
	}
	s.LastUsed = time.Now()
	rv := s.PartitionSeqs
	mapSourcePartitionSeqsM.Unlock()
	return rv
}

func DropSourcePartitionSeqs(sourceName, sourceUUID string) {
	mapSourcePartitionSeqsM.Lock()
	for sourceSpec, _ := range mapSourcePartitionSeqs {
		if sourceSpec.SourceName == sourceName &&
			sourceSpec.SourceUUID == sourceUUID {
			delete(mapSourcePartitionSeqs, sourceSpec)
		}
	}
	mapSourcePartitionSeqsM.Unlock()
}

func RunSourcePartitionSeqs(options map[string]string, stopCh chan struct{}) {
	sourcePartitionSeqsSleep := SourcePartitionSeqsSleepDefault
	v, exists := options["sourcePartitionSeqsSleepMS"]
	if exists {
		sourcePartitionSeqsSleepMS, err := strconv.Atoi(v)
		if err != nil {
			log.Warnf("ns_server: parse sourcePartitionSeqsSleepMS: %q,"+
				" err: %v", v, err)
		} else {
			sourcePartitionSeqsSleep = time.Millisecond *
				time.Duration(sourcePartitionSeqsSleepMS)
		}
	}

	sourcePartitionSeqsCacheTimeout := SourcePartitionSeqsCacheTimeoutDefault
	v, exists = options["sourcePartitionSeqsCacheTimeoutMS"]
	if exists {
		sourcePartitionSeqsCacheTimeoutMS, err := strconv.Atoi(v)
		if err != nil {
			log.Warnf("ns_server: parse sourcePartitionSeqsCacheTimeoutMS: %q,"+
				" err: %v", v, err)
		} else {
			sourcePartitionSeqsCacheTimeout = time.Millisecond *
				time.Duration(sourcePartitionSeqsCacheTimeoutMS)
		}
	}

	m := &mapSourcePartitionSeqsM

	for {
		select {
		case <-stopCh:
			return
		case <-time.After(sourcePartitionSeqsSleep):
			// NO-OP.
		}

		m.Lock()
		var sourceSpecs []SourceSpec // Snapshot the wanted sourceSpecs.
		for sourceSpec := range mapSourcePartitionSeqs {
			sourceSpecs = append(sourceSpecs, sourceSpec)
		}
		m.Unlock()

		for _, sourceSpec := range sourceSpecs {
			select {
			case <-stopCh:
				return
			default:
				// NO-OP.
			}

			m.Lock()
			s := SourcePartitionSeqs{}
			v, exists := mapSourcePartitionSeqs[sourceSpec]
			if exists && v != nil {
				s = *v // Copy fields.
			}
			m.Unlock()

			if s.LastUpdated.After(s.LastUsed) {
				if s.LastUpdated.Sub(s.LastUsed) > sourcePartitionSeqsCacheTimeout {
					m.Lock()
					delete(mapSourcePartitionSeqs, sourceSpec)
					m.Unlock()
				}

				continue
			}

			if s.LastUsed.Sub(s.LastUpdated) > sourcePartitionSeqsSleep {
				next := &SourcePartitionSeqs{}
				*next = s // Copy fields.

				feedType, exists := cbgt.FeedTypes[sourceSpec.SourceType]
				if exists && feedType != nil && feedType.PartitionSeqs != nil {
					partitionSeqs, err := feedType.PartitionSeqs(
						sourceSpec.SourceType,
						sourceSpec.SourceName,
						sourceSpec.SourceUUID,
						sourceSpec.SourceParams,
						sourceSpec.Server, options)
					if err != nil {
						log.Warnf("ns_server: retrieve partition seqs: %v", err)
					} else {
						next.PartitionSeqs = partitionSeqs
					}
				}

				next.LastUpdated = time.Now()

				m.Lock()
				curr, exists := mapSourcePartitionSeqs[sourceSpec]
				if exists && curr != nil {
					next.LastUsed = curr.LastUsed
				}
				mapSourcePartitionSeqs[sourceSpec] = next
				m.Unlock()
			}
		}
	}
}

// ---------------------------------------------------------------

type recentInfo struct {
	indexDefs    *cbgt.IndexDefs
	indexDefsMap map[string]*cbgt.IndexDef
	nodeDefs     *cbgt.NodeDefs
	planPIndexes *cbgt.PlanPIndexes
	memStats     runtime.MemStats
	err          error
}

var lastRecentInfo = unsafe.Pointer(new(recentInfo))

func FetchCurMemoryUsed() uint64 {
	return atomic.LoadUint64(&currentMemoryUsed)
}

func UpdateCurMemoryUsed() uint64 {
	var memStats *runtime.MemStats
	runtime.ReadMemStats(memStats)
	return setCurMemoryUsedWith(memStats)
}

func setCurMemoryUsedWith(memStats *runtime.MemStats) uint64 {
	// (Sys - HeapIdle) best represents the amount of memory that the go process
	// is consuming at the moment that is not reusable; the go process has
	// as idle memory component that it holds on to which can be reused;
	// HeapIdle includes memory that is idle and the part that has been released.
	curMemoryUsed := memStats.Sys - memStats.HeapIdle
	atomic.StoreUint64(&currentMemoryUsed, curMemoryUsed)
	return curMemoryUsed
}

func RunRecentInfoCache(mgr *cbgt.Manager) {
	cfg := mgr.Cfg()

	cfgChangedCh := make(chan struct{}, 10)

	var indexDefsChanged uint32

	go func() { // Debounce cfg events to feed into the cfgChangedCh.
		ech := make(chan cbgt.CfgEvent)
		cfg.Subscribe(cbgt.PLAN_PINDEXES_DIRECTORY_STAMP, ech)
		cfg.Subscribe(cbgt.INDEX_DEFS_KEY, ech)
		cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_WANTED), ech)

		for {
			ev := <-ech // First, wait for a cfg event.

			// check whether it is an index defn update.
			if ev.Key == cbgt.INDEX_DEFS_KEY {
				atomic.StoreUint32(&indexDefsChanged, 1)
			}

			debounceTimeCh := time.After(500 * time.Millisecond)

		DEBOUNCE_LOOP:
			for {
				select {
				case ev = <-ech:
					// check whether it is an index defn update.
					if ev.Key == cbgt.INDEX_DEFS_KEY {
						atomic.StoreUint32(&indexDefsChanged, 1)
					}
					// NO-OP when there are more, spammy cfg events.

				case <-debounceTimeCh:
					break DEBOUNCE_LOOP
				}
			}

			cfgChangedCh <- struct{}{}
		}
	}()

	tickCh := time.Tick(1 * time.Second)

	memStatsLoggingInterval, _ := strconv.Atoi(mgr.Options()["memStatsLoggingInterval"])
	logMemStatCh := time.Tick(time.Duration(memStatsLoggingInterval) * time.Second)

	var prevMemoryUsed uint64
	var curMemoryUsed uint64

	var nodeDefs *cbgt.NodeDefs
	var planPIndexes *cbgt.PlanPIndexes

	indexDefs, indexDefsMap, err := mgr.GetIndexDefs(false)
	if err == nil {
		nodeDefs, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
		if err == nil {
			planPIndexes, _, err = cbgt.CfgGetPlanPIndexes(cfg)
		}
	}

	// initialize the metaFieldValCache.
	refreshMetaFieldValCache(indexDefs)

	for {

		rd := &recentInfo{
			indexDefs:    indexDefs,
			indexDefsMap: indexDefsMap,
			nodeDefs:     nodeDefs,
			planPIndexes: planPIndexes,
			err:          err,
		}
		atomic.StorePointer(&lastRecentInfo, unsafe.Pointer(rd))

		runtime.ReadMemStats(&rd.memStats)

		prevMemoryUsed = curMemoryUsed
		curMemoryUsed = setCurMemoryUsedWith(&rd.memStats)

		if curMemoryUsed < prevMemoryUsed &&
			OnMemoryUsedDropped != nil {
			OnMemoryUsedDropped(curMemoryUsed, prevMemoryUsed)
		}

		// Check memory quota if golang's GC needs to be triggered.
		ftsMemoryQuota, _ := strconv.Atoi(mgr.Options()["ftsMemoryQuota"])
		gcMinThreshold, _ := strconv.Atoi(mgr.Options()["gcMinThreshold"])
		gcTriggerPct, _ := strconv.Atoi(mgr.Options()["gcTriggerPct"])

		if gcTriggerPct > 0 {
			allocedBytes := rd.memStats.Alloc
			if allocedBytes > uint64(gcMinThreshold) &&
				allocedBytes >= uint64(gcTriggerPct*ftsMemoryQuota/100) {
				// Invoke golang's gargage collector through runtime/debug's
				// FreeOSMemory api which forces a garbage collection followed
				// by an attempt to return as much memory to the operating
				// system as possible.
				log.Printf("runtime/debug.FreeOSMemory() start..")
				debug.FreeOSMemory()
				log.Printf("runtime/debuf.FreeOSMemory() done.")
			}
		}

	REUSE_CACHE:
		for {
			select {
			case <-cfgChangedCh:
				var refresh bool
				if atomic.LoadUint32(&indexDefsChanged) == 1 {
					refresh = true
				}

				// refresh the configs
				indexDefs, indexDefsMap, err = mgr.GetIndexDefs(refresh)
				if err == nil {
					nodeDefs, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
					if err == nil {
						planPIndexes, _, err = cbgt.CfgGetPlanPIndexes(cfg)
					}
				}

				// update the metaFieldValCache
				if refresh {
					atomic.StoreUint32(&indexDefsChanged, 0)
					refreshMetaFieldValCache(indexDefs)
				}

				break REUSE_CACHE

			case <-tickCh:
				break REUSE_CACHE

			case <-logMemStatCh:
				logMemStatInfo(&rd.memStats)
			}
		}
	}
}

func logMemStatInfo(ms *runtime.MemStats) {
	if ms != nil {
		log.Printf("memstats:: TotalAlloc: %+v, Alloc: %+v, Sys: %+v", ms.TotalAlloc, ms.Alloc, ms.Sys)
		log.Printf("memstats:: Lookups: %+v, Mallocs: %+v, Frees: %+v", ms.Lookups, ms.Mallocs, ms.Frees)
		log.Printf("memstats:: LastGC: %+v, NextGC: %+v, NumGC: %+v", ms.LastGC, ms.NextGC, ms.NumGC)
		log.Printf("memstats:: PauseTotalNs: %+v, EnableGC: %+v, GCCPUFraction: %+v", ms.PauseTotalNs, ms.EnableGC, ms.GCCPUFraction)
		log.Printf("heapstats:: HeapAlloc: %+v, HeapSys: %+v, HeapIdle: %+v", ms.HeapAlloc, ms.HeapSys, ms.HeapIdle)
		log.Printf("heapstats:: HeapInuse: %+v, HeapReleased: %+v, HeapObjects: %+v", ms.HeapInuse, ms.HeapReleased, ms.HeapObjects)
		log.Printf("stackstats:: StackInuse: %+v, StackSys: %+v, MSpanInuse: %+v", ms.StackInuse, ms.StackSys, ms.MSpanInuse)
		log.Printf("stackstats:: MSpanSys: %+v, MCacheInuse: %+v, GCSys: %+v", ms.MSpanSys, ms.MCacheInuse, ms.GCSys)
	}
}

// ---------------------------------------------------------------

type NsSearchResultRedirct struct {
	mgr *cbgt.Manager
}

func NsSearchResultRedirctHandler(mgr *cbgt.Manager) (*NsSearchResultRedirct, error) {
	return &NsSearchResultRedirct{
		mgr: mgr,
	}, nil
}

func (h *NsSearchResultRedirct) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	allPlanPIndexes, _, err := h.mgr.GetPlanPIndexes(false)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not get plan pindexes: %v", err),
			http.StatusInternalServerError)
		return
	}

	pIndexName := rest.PIndexNameLookup(req)
	planPIndex, ok := allPlanPIndexes.PlanPIndexes[pIndexName]
	if !ok {
		rest.ShowError(w, req, fmt.Sprintf("no pindex named: %s", pIndexName),
			http.StatusBadRequest)
		return
	}

	docID := rest.DocIDLookup(req)
	source := planPIndex.SourceName
	http.Redirect(w, req, "/ui/index.html#!/buckets/documents/"+docID+"?bucket="+source, http.StatusMovedPermanently)
}

type DCPAgentsStatsHandler struct {
	mgr *cbgt.Manager
}

func NewDCPAgentsStatsHandler(mgr *cbgt.Manager) *DCPAgentsStatsHandler {
	return &DCPAgentsStatsHandler{mgr: mgr}
}

func (h *DCPAgentsStatsHandler) ServeHTTP(w http.ResponseWriter,
	req *http.Request) {
	dcpAgentsStats := cbgt.DCPAgentsStatsMap()
	if dcpAgentsStats == nil {
		rest.ShowError(w, req, fmt.Sprintf("empty dcp agents stats map"),
			http.StatusNoContent)
		return
	}
	rv := struct {
		Status           string                 `json:"status"`
		DCPAgentStatsMap map[string]interface{} `json:"gocbcore_dcp_agents_stats"`
	}{
		Status:           "ok",
		DCPAgentStatsMap: dcpAgentsStats,
	}
	rest.MustEncode(w, rv)
}

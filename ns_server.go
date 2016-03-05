//  Copyright (c) 2014 Couchbase, Inc.
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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"unicode"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	"github.com/dustin/go-jsonpointer"
)

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
	mgr *cbgt.Manager
}

func NewNsStatsHandler(mgr *cbgt.Manager) *NsStatsHandler {
	return &NsStatsHandler{mgr: mgr}
}

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
	// manual
	"num_pindexes",

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

	"num_mutations_to_index",
	// "doc_count", // per-index stat (same as before, see above).
	"total_bytes_indexed",
	"num_recs_to_persist",

	"num_bytes_used_disk",
	// "num_bytes_used_ram" -- PROCESS-LEVEL stat.

	"num_pindexes_actual", // per-index stat.
	"num_pindexes_target", // per-index stat.

	"total_compactions",

	// "total_gc" -- PROCESS-LEVEL stat.
	// "pct_cpu_gc" -- PROCESS-LEVEL stat.

	"total_queries",             // per-index stat.
	"avg_queries_latency",       // per-index stat.
	"total_queries_slow",        // per-index stat.
	"total_queries_timeout",     // per-index stat.
	"total_queries_error",       // per-index stat.
	"total_bytes_query_results", // per-index stat.
	"total_term_searchers",
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

func (h *NsStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	_, indexDefsMap, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve index defs: %v", err), 500)
		return
	}

	planPIndexes, _, err := cbgt.CfgGetPlanPIndexes(h.mgr.Cfg())
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve plan pIndexes: %v", err), 500)
		return
	}

	nodeUUID := h.mgr.UUID()

	nsIndexStats := make(NSIndexStats, len(indexDefsMap))

	indexNameToSourceName := map[string]string{}

	indexNameToPlanPIndexes := map[string][]*cbgt.PlanPIndex{}
	for _, planPIndex := range planPIndexes.PlanPIndexes {
		// Only focus on the planPIndex entries for this node.
		if planPIndex.Nodes[nodeUUID] != nil {
			indexNameToPlanPIndexes[planPIndex.IndexName] =
				append(indexNameToPlanPIndexes[planPIndex.IndexName], planPIndex)
		}
	}

	indexQueryPathStats := MapRESTPathStats[RESTIndexQueryPath]

	// Keyed by indexName, sub-key is source partition id.
	indexNameToSourcePartitionSeqs := map[string]map[string]cbgt.UUIDSeq{}

	// Keyed by indexName, sub-key is source partition id.
	indexNameToDestPartitionSeqs := map[string]map[string]cbgt.UUIDSeq{}

	for indexName, indexDef := range indexDefsMap {
		nsIndexStat := NewIndexStat()
		nsIndexStats[indexDef.SourceName+":"+indexName] = nsIndexStat

		indexNameToSourceName[indexName] = indexDef.SourceName

		focusStats := indexQueryPathStats.FocusStats(indexName)
		if focusStats != nil {
			totalQueries := atomic.LoadUint64(&focusStats.TotRequest)
			nsIndexStat["total_queries"] = totalQueries
			if totalQueries > 0 {
				nsIndexStat["avg_queries_latency"] =
					float64((atomic.LoadUint64(&focusStats.TotRequestTimeNS) /
						totalQueries)) / 1000000.0 // Convert from nanosecs to millisecs.
			}
			nsIndexStat["total_queries_slow"] =
				atomic.LoadUint64(&focusStats.TotRequestSlow)
			nsIndexStat["total_queries_timeout"] =
				atomic.LoadUint64(&focusStats.TotRequestTimeout)
			nsIndexStat["total_queries_error"] =
				atomic.LoadUint64(&focusStats.TotRequestErr)
			nsIndexStat["total_bytes_query_results"] =
				atomic.LoadUint64(&focusStats.TotResponseBytes)
			nsIndexStat["num_pindexes_target"] =
				uint64(len(indexNameToPlanPIndexes[indexName]))
		}

		feedType, exists := cbgt.FeedTypes[indexDef.SourceType]
		if !exists || feedType == nil || feedType.PartitionSeqs == nil {
			continue
		}

		partitionSeqs, err := feedType.PartitionSeqs(
			indexDef.SourceType, indexDef.SourceName, indexDef.SourceUUID,
			indexDef.SourceParams, h.mgr.Server(), h.mgr.Options())
		if err != nil {
			rest.ShowError(w, req,
				fmt.Sprintf("could not retrieve partition seqs: %v", err), 500)
			return
		}

		indexNameToSourcePartitionSeqs[indexName] = partitionSeqs
	}

	feeds, pindexes := h.mgr.CurrentMaps()

	for _, pindex := range pindexes {
		nsIndexName := pindex.SourceName + ":" + pindex.IndexName
		nsIndexStat, ok := nsIndexStats[nsIndexName]
		if ok {
			// manually track num pindexes
			oldValue, ok := nsIndexStat["num_pindexes_actual"]
			if ok {
				switch oldValue := oldValue.(type) {
				case float64:
					oldValue += float64(1)

					nsIndexStat["num_pindexes_actual"] = oldValue

					// TODO: Former name was num_pindexes, need to remove one day.
					nsIndexStat["num_pindexes"] = oldValue
				}
			}

			// automatically process all the pindex dest stats
			err := addPIndexStats(pindex, nsIndexStat)
			if err != nil {
				rest.ShowError(w, req,
					fmt.Sprintf("error processing PIndex stats: %v", err), 500)
				return
			}

			dest := pindex.Dest
			if dest != nil {
				destForwarder, ok := dest.(*cbgt.DestForwarder)
				if !ok {
					continue
				}

				partitionSeqsProvider, ok :=
					destForwarder.DestProvider.(PartitionSeqsProvider)
				if !ok {
					continue
				}

				partitionSeqs, err := partitionSeqsProvider.PartitionSeqs()
				if err == nil {
					m := indexNameToDestPartitionSeqs[pindex.IndexName]
					if m == nil {
						m = map[string]cbgt.UUIDSeq{}
						indexNameToDestPartitionSeqs[pindex.IndexName] = m
					}

					for partitionId, uuidSeq := range partitionSeqs {
						m[partitionId] = uuidSeq
					}
				}
			}
		}
	}

	for _, feed := range feeds {
		sourceName := indexNameToSourceName[feed.IndexName()]
		nsIndexName := sourceName + ":" + feed.IndexName()
		nsIndexStat, ok := nsIndexStats[nsIndexName]
		if ok {
			// automatically process all the feed stats
			err := addFeedStats(feed, nsIndexStat)
			if err != nil {
				rest.ShowError(w, req,
					fmt.Sprintf("error processing Feed stats: %v", err), 500)
				return
			}
		}
	}

	for indexName, indexDef := range indexDefsMap {
		nsIndexStat, ok := nsIndexStats[indexDef.SourceName+":"+indexName]
		if ok {
			src := indexNameToSourcePartitionSeqs[indexName]
			if src == nil {
				continue
			}

			dst := indexNameToDestPartitionSeqs[indexName]
			if dst == nil {
				continue
			}

			var totSeq uint64
			var curSeq uint64

			for partitionId, dstUUIDSeq := range dst {
				srcUUIDSeq, exists := src[partitionId]
				if exists {
					totSeq += srcUUIDSeq.Seq
					curSeq += dstUUIDSeq.Seq
				}
			}

			nsIndexStat["num_mutations_to_index"] = totSeq - curSeq
		}
	}

	// FIXME hard-coded top-level stats
	topLevelStats := map[string]interface{}{}
	topLevelStats["num_connections"] = 0
	topLevelStats["needs_restart"] = false

	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)

	topLevelStats["num_bytes_used_ram"] = memStats.Alloc
	topLevelStats["total_gc"] = memStats.NumGC
	topLevelStats["pct_cpu_gc"] = memStats.GCCPUFraction

	nsIndexStats[""] = topLevelStats

	rest.MustEncode(w, nsIndexStats)
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
	buffer := new(bytes.Buffer)
	err := pindex.Dest.Stats(buffer)
	if err != nil {
		return err
	}
	return massageStats(buffer, nsIndexStat)
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
	nodeExtras := make(map[string]string)
	for _, nodeDef := range nodeDefs.NodeDefs {
		_, ok := nodes[nodeDef.UUID]
		if ok {
			var e struct {
				NsHostPort string `json:"nsHostPort"`
			}

			err := json.Unmarshal([]byte(nodeDef.Extras), &e)
			if err != nil {
				// Early versions of ns_server integration had a
				// simple, non-JSON "host:port" format for
				// nodeDef.Extras, so fall back to that.
				e.NsHostPort = nodeDef.Extras
			}

			nodeExtras[nodeDef.UUID] = e.NsHostPort
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

	cfg := h.mgr.Cfg()
	planPIndexes, _, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve plan pIndexes: %v", err), 500)
		return
	}

	nodesDefs, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		rest.ShowError(w, req, "could not retrieve node defs (wanted)", 500)
		return
	}

	_, indexDefsMap, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		rest.ShowError(w, req, "could not retrieve index defs", 500)
		return
	}

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
			Completion int      `json:"completion"`
			Hosts      []string `json:"hosts"`
			Status     string   `json:"status"`
			Bucket     string   `json:"bucket"`
			Name       string   `json:"name"`
		}{
			Bucket: indexDef.SourceName,
			Name:   indexDefName,
			Hosts:  NsHostsForIndex(indexDefName, planPIndexes, nodesDefs),
			// FIXME hard-coded
			Completion: 100,
			Status:     "Ready",
		})
	}

	w.Write([]byte("],"))
	w.Write(statsNamePrefix)
	w.Write([]byte("code"))
	w.Write(statsNameSuffix)
	w.Write([]byte("\"success\""))
	w.Write(cbgt.JsonCloseBrace)
}

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
	"sort"
	"strings"
	"unicode"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	"github.com/dustin/go-jsonpointer"
)

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
	"timer_batch_merge_count",
	"timer_iterator_next_count", "timer_iterator_seek_count",
	"timer_reader_get_count", "timer_reader_multi_get_count",
	"timer_reader_prefix_iterator_count", "timer_reader_range_iterator_count",
	"timer_writer_execute_batch_count",

	// feed
	"timer_opaque_set_count", "timer_rollback_count", "timer_data_update_count",
	"timer_data_delete_count", "timer_snapshot_start_count", "timer_opaque_get_count",
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

func (h *NsStatsHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {

	_, indexDefsMap, err := h.mgr.GetIndexDefs(false)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("could not retrieve index defs: %v", err), 500)
		return
	}

	nsIndexStats := make(NSIndexStats, len(indexDefsMap))
	for indexDefName, indexDef := range indexDefsMap {
		nsIndexStats[indexDef.SourceName+":"+indexDefName] = NewIndexStat()
	}

	feeds, pindexes := h.mgr.CurrentMaps()

	sourceName := ""
	for _, pindex := range pindexes {
		sourceName = pindex.SourceName
		lindexName := pindex.SourceName + ":" + pindex.IndexName
		nsIndexStat, ok := nsIndexStats[lindexName]
		if ok {
			// manually track a statistic representing
			// the number of pindex in the index
			oldValue, ok := nsIndexStat["num_pindexes"]
			if ok {
				switch oldValue := oldValue.(type) {
				case float64:
					oldValue += float64(1)
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
		}
	}

	for _, feed := range feeds {
		lindexName := sourceName + ":" + feed.IndexName()
		nsIndexStat, ok := nsIndexStats[lindexName]
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

	// FIXME hard-coded top-level stats
	nsIndexStats[""] = make(map[string]interface{})
	nsIndexStats[""]["num_connections"] = 0
	nsIndexStats[""]["needs_restart"] = false

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
			strings.HasSuffix(pointer, "/DocCount") {
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

func convertStatName(key string) string {
	lastSlash := strings.LastIndex(key, "/")
	if lastSlash < 0 {
		return "unknown"
	}
	statSuffix := key[lastSlash+1:]
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

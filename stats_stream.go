//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

type statsStreamHandler struct {
	mgr *cbgt.Manager
}

func NewStatsStreamHandler(mgr *cbgt.Manager) *statsStreamHandler {
	return &statsStreamHandler{mgr: mgr}
}

type statsStreamChunk struct {
	Stats     map[string]interface{} `json:"stats"`
	Rebalance bool                   `json:"rebalance"`
}

func (h *statsStreamHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	cn, ok := w.(http.CloseNotifier)
	if !ok {
		http.NotFound(w, req)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.NotFound(w, req)
		return
	}

	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	enc := json.NewEncoder(w)

	tickerCh := time.NewTicker(time.Second).C

	stats := make(map[string]interface{})
	nsStats := []string{
		"batch_bytes_added",
		"batch_bytes_removed",
		"curr_batches_blocked_by_herder",
		"num_batches_introduced",
		"num_bytes_used_ram",
		"num_gocbcore_dcp_agents",
		"num_gocbcore_stats_agents",
		"pct_cpu_gc",
		"tot_batches_merged",
		"tot_batches_new",
		"tot_bleve_dest_closed",
		"tot_bleve_dest_opened",
		"tot_queryreject_on_memquota",
		"tot_rollback_full",
		"tot_rollback_partial",
		"total_gc",
		"total_queries_rejected_by_herder",
		"utilization:billableUnitsRate",
		"utilization:cpuPercent",
		"utilization:diskBytes",
		"utilization:memoryBytes",
	}

	for {
		select {
		case <-cn.CloseNotify():
			return
		case <-tickerCh:
			rd := getRecentInfo()
			if rd.err != nil {
				rest.ShowError(w, req, fmt.Sprintf("could not retrieve defs: %v", rd.err), http.StatusInternalServerError)
				return
			}
			nsIndexStats, err := gatherIndexesStats(h.mgr, rd, false)
			if err != nil {
				rest.ShowError(w, req, fmt.Sprintf("error in retrieving defs: %v", err), http.StatusInternalServerError)
				return
			}

			for _, stat := range nsStats {
				stats[stat] = nsIndexStats[""][stat]
			}

			rebalance, err := rest.CheckRebalanceStatus(h.mgr)

			if err != nil {
				log.Warnf("Error getting rebalance status: %v", err)
			}
			m := statsStreamChunk{
				Stats:     stats,
				Rebalance: rebalance,
			}

			err = enc.Encode(m)

			if err != nil {
				log.Warnf("Error encoding stats stream message into json: %v", err)
				return
			}

			flusher.Flush()
		}
	}
}

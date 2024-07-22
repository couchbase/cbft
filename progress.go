//  Copyright 2021-Present Couchbase, Inc.
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

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// ProgressStatsHandler is a REST handler that provides stats relevant to
// infer indexing progress.
type ProgressStatsHandler struct {
	mgr *cbgt.Manager
}

func NewProgressStatsHandler(mgr *cbgt.Manager) *ProgressStatsHandler {
	return &ProgressStatsHandler{mgr: mgr}
}

func (h *ProgressStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	indexName := rest.IndexNameLookup(req)
	if indexName == "" {
		rest.ShowError(w, req, "index name is required", http.StatusBadRequest)
		return
	}

	indexProgressStats, _ := gatherIndexProgressStats(h.mgr, indexName)

	docCount, _ := indexProgressStats["doc_count"].(uint64)
	totSeqReceived, _ := indexProgressStats["tot_seq_received"].(uint64)
	numMutationsToIndex, _ := indexProgressStats["num_mutations_to_index"].(uint64)
	ingestStatus, _ := indexProgressStats["ingest_status"].(string)

	rv := struct {
		Status              string `json:"status"`
		DocCount            uint64 `json:"doc_count"`
		TotSeqReceived      uint64 `json:"tot_seq_received"`
		NumMutationsToIndex uint64 `json:"num_mutations_to_index"`
		IngestStatus        string `json:"ingest_status"`
	}{
		Status:              "ok",
		DocCount:            docCount,
		NumMutationsToIndex: numMutationsToIndex,
		TotSeqReceived:      totSeqReceived,
		IngestStatus:        ingestStatus,
	}
	rest.MustEncode(w, rv)
}

// ---------------------------------------------------------------

func gatherIndexProgressStats(mgr *cbgt.Manager, indexName string) (
	map[string]interface{}, error) {
	if mgr == nil {
		return nil, fmt.Errorf("manager not available")
	}

	indexDef, pindexImplType, err := mgr.GetIndexDef(indexName, false)
	if err != nil || indexDef == nil {
		return nil,
			fmt.Errorf("unable to obtain index def for `%v`, err: %v", indexName, err)
	}

	count, err := pindexImplType.Count(mgr, indexName, "")
	if err != nil {
		return nil, err
	}

	rv := map[string]interface{}{}
	rv["doc_count"] = count

	sourcePartitionSeqs := GetSourcePartitionSeqs(SourceSpec{
		SourceType:   indexDef.SourceType,
		SourceName:   indexDef.SourceName,
		SourceUUID:   indexDef.SourceUUID,
		SourceParams: indexDef.SourceParams,
		Server:       mgr.Server(),
	})

	destPartitionSeqs := map[string]cbgt.UUIDSeq{}
	_, pindexes := mgr.CurrentMaps()
	for _, pindex := range pindexes {
		if pindex.IndexName != indexDef.Name {
			continue
		}

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

	totSeqReceived, numMutationsToIndex, err := obtainDestSeqsForIndex(
		indexDef, sourcePartitionSeqs, destPartitionSeqs)
	if err != nil {
		return rv, err
	}

	rv["tot_seq_received"] = totSeqReceived
	rv["num_mutations_to_index"] = numMutationsToIndex

	idxPlanParams := indexDef.PlanParams.NodePlanParams[""][""]
	if idxPlanParams != nil && !idxPlanParams.CanWrite {
		rv["ingest_status"] = "paused"
	} else if numMutationsToIndex > 0 {
		rv["ingest_status"] = "active"
	} else {
		rv["ingest_status"] = "idle"
	}

	return rv, nil
}

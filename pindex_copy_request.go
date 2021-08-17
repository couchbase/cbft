//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
package cbft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

var FeatureFileTransferRebalance = "fileTransferRebalance"

// CopyPartitionRequest represents the partition copy request.
type CopyPartitionRequest struct {
	SourceNodes      []*cbgt.NodeDef // ordered by preference
	SourcePIndexName string
	SourceUUID       string
	CancelCh         chan struct{}
	Stats            *CopyPartitionStats
}

type CopyPartitionStats struct {
	TotCopyPartitionStart   int32
	TotCopyPartitionSkipped int32

	TotCopyPartitionServerErrors   int32
	TotCopyPartitionChecksumErrors int32
	TotCopyPartitionErrors         int32

	TotCopyPartitionRetries   int32
	TotCopyPartitionFailed    int32
	TotCopyPartitionFinished  int32
	TotCopyPartitionCancelled int32
	TotCopyPartitionTimeInMs  int32
	TotCopyPartitionOnHttp2   int32

	CopyPartitionNumBytesExpected int32
	CopyPartitionNumBytesReceived int32
}

func (s *CopyPartitionStats) WriteJSON(w io.Writer) {
	t := atomic.LoadInt32(&s.TotCopyPartitionStart)
	stats := fmt.Sprintf(`{"TotCopyPartitionStart":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionFinished)
	stats += fmt.Sprintf(`,"TotCopyPartitionFinished":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionTimeInMs)
	stats += fmt.Sprintf(`,"TotCopyPartitionTimeInMs":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionFailed)
	stats += fmt.Sprintf(`,"TotCopyPartitionFailed":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionRetries)
	stats += fmt.Sprintf(`,"TotCopyPartitionRetries":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionErrors)
	stats += fmt.Sprintf(`,"TotCopyPartitionErrors":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionSkipped)
	stats += fmt.Sprintf(`,"TotCopyPartitionSkipped":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionCancelled)
	stats += fmt.Sprintf(`,"TotCopyPartitionCancelled":%d`, t)
	t = atomic.LoadInt32(&s.TotCopyPartitionOnHttp2)
	stats += fmt.Sprintf(`,"TotCopyPartitionOnHttp2":%d`, t)
	if atomic.LoadInt32(&s.CopyPartitionNumBytesExpected) > 0 {
		prog := math.Round(float64(atomic.LoadInt32(&s.CopyPartitionNumBytesReceived)) /
			float64(atomic.LoadInt32(&s.CopyPartitionNumBytesExpected)) * 100 / 100)
		stats += `,"TransferProgress":` + fmt.Sprintf("%f", prog)
	}
	w.Write([]byte(stats))
	w.Write(cbgt.JsonCloseBrace)
}

// buildCopyPartitionRequest returns the partition copy request.
// Currently this is a naive implementation which just checks the existence
// of pindexes among the known node lists and then sort them based on the
// partition state, rack and number of total partitions/load on those nodes.
func buildCopyPartitionRequest(pindexName string, stats *CopyPartitionStats,
	mgr *cbgt.Manager, stopCh chan struct{}) (*CopyPartitionRequest, error) {
	planPIndexes, _, err := cbgt.CfgGetPlanPIndexes(mgr.Cfg())
	if err != nil {
		return nil, fmt.Errorf("pindex_copy_request:"+
			" CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexes == nil {
		return nil, fmt.Errorf("pindex_copy_request:" +
			" skipped on nil planPIndexes")
	}

	var planPIndex *cbgt.PlanPIndex
	var exists bool
	if planPIndex, exists = planPIndexes.PlanPIndexes[pindexName]; !exists ||
		len(planPIndex.Nodes) == 0 {
		return nil, fmt.Errorf("pindex_copy_request:"+
			" missing pindex: %s in planPIndexes", pindexName)
	}
	nodeDefs, _, err := cbgt.CfgGetNodeDefs(mgr.Cfg(), cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		return nil, err
	}

	formerPrimary := ""
	// get all the potential source nodes except self.
	uuids := make([]string, len(planPIndex.Nodes))
	for uuid, node := range planPIndex.Nodes {
		if node.Priority == 0 && node.CanRead && node.CanWrite {
			formerPrimary = uuid
		}
		// if the current node is the primary
		// then no file copying is needed.
		if mgr.UUID() == formerPrimary {
			atomic.StoreInt32(&stats.TotCopyPartitionSkipped, int32(1))
			return nil, nil
		}
		// skip self.
		if mgr.UUID() == uuid {
			continue
		}

		uuids = append(uuids, uuid)
	}

	// check explicitly whether the requested pindex resides in those
	// nodes, and if so gives back a set of potential source node uuids.
	sourceUUIDs := getNodesHostingPIndex(uuids, pindexName,
		mgr.Options()["authType"], nodeDefs)
	if len(sourceUUIDs) == 0 {
		atomic.StoreInt32(&stats.TotCopyPartitionSkipped, int32(1))
		return nil, nil
	}

	return buildCopyPartitionRequestUtil(planPIndexes, pindexName, stats,
		nodeDefs, sourceUUIDs, formerPrimary, mgr, stopCh)
}

// buildCopyPartitionRequestUtil creates the partition copy request
// with a sorted list of source nodeDefs for the given pindex.
func buildCopyPartitionRequestUtil(planPIndexes *cbgt.PlanPIndexes,
	pindexName string, stats *CopyPartitionStats, nodeDefs *cbgt.NodeDefs,
	sourceUUIDs []string, formerPrimary string, mgr *cbgt.Manager,
	stopCh chan struct{}) (*CopyPartitionRequest, error) {
	// compute the given partition load on the cluster nodes.
	nodeParitionCount := make(map[string]int)
	for _, pp := range planPIndexes.PlanPIndexes {
		for node := range pp.Nodes {
			if _, ok := nodeParitionCount[node]; !ok {
				nodeParitionCount[node] = 1
			} else {
				nodeParitionCount[node]++
			}
		}
	}

	// form the source partition candidates.
	planPIndex := planPIndexes.PlanPIndexes[pindexName]
	var sps []*srcPartition
	for _, uuid := range sourceUUIDs {
		if nodeDef, ok := nodeDefs.NodeDefs[uuid]; ok {
			node := planPIndex.Nodes[uuid]
			sp := &srcPartition{nodeDef: nodeDef,
				partitionCountOnNode: nodeParitionCount[uuid]}
			if node.Priority == 0 {
				sp.state = "primary"
			} else if node.Priority > 0 {
				sp.state = "replica"
			}
			sps = append(sps, sp)
		}
	}

	// sort them in the order of preference.
	sort.Sort(&srcPartitionSorter{
		sps:   sps,
		state: "primary",
		rack:  mgr.Container()})

	if len(sps) > 0 {
		var sourceNodes []*cbgt.NodeDef
		for _, sp := range sps {
			sourceNodes = append(sourceNodes, sp.nodeDef)
		}
		return &CopyPartitionRequest{
			SourceNodes:      sourceNodes,
			SourcePIndexName: planPIndex.Name,
			SourceUUID:       planPIndex.UUID,
			Stats:            stats,
			CancelCh:         stopCh,
		}, nil
	}

	return nil, nil
}

// getNodesHostingPIndex confirms the list of node UUIDs
// hosting the given pindex.
func getNodesHostingPIndex(uuids []string,
	pindex, authType string, nodeDefs *cbgt.NodeDefs) []string {
	if len(uuids) == 0 {
		return nil
	}
	urlMap := getStatsUrls(uuids, authType, nodeDefs)
	var wg sync.WaitGroup
	size := len(urlMap)

	type statsReq struct {
		uuid string
		url  string
	}

	type statsResp struct {
		uuid   string
		status bool
		err    error
	}

	requestCh := make(chan *statsReq, size)
	responseCh := make(chan *statsResp, size)
	nWorkers := getWorkerCount(size)
	// spawn the stats get workers
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			for reqs := range requestCh {
				ctx, cancel := context.WithTimeout(context.Background(),
					60*time.Second)
				defer cancel()
				req, _ := http.NewRequestWithContext(ctx, "GET", reqs.url, nil)

				httpClient := cbgt.HttpClient()
				res, err := httpClient.Do(req)
				if err != nil {
					responseCh <- &statsResp{
						uuid:   reqs.uuid,
						status: false,
						err: fmt.Errorf("pindex_copy_request:"+
							" getNodesHostingPIndex, get stats for pindex: %s,"+
							" err: %v", reqs.url, err),
					}
					continue
				}
				// parse the response to get the pindex status
				pindexesData := struct {
					PIndexes map[string]struct {
						Partitions map[string]struct {
							UUID string `json:"uuid"`
							Seq  uint64 `json:"seq"`
						} `json:"partitions"`
						Basic struct {
							DocCount uint64 `json:"DocCount"`
						} `json:"basic"`
					} `json:"pindexes"`
				}{}
				data, derr := ioutil.ReadAll(res.Body)
				if err == nil && derr != nil {
					responseCh <- &statsResp{
						uuid:   reqs.uuid,
						status: false,
						err:    derr,
					}
					log.Printf("pindex_copy_request: getNodesHostingPIndex,"+
						" grab stats for pindex: %s, err: %v", reqs.url, err)
					continue
				}
				err = json.Unmarshal(data, &pindexesData)
				if err != nil {
					log.Printf("pindex_copy_request: getNodesHostingPIndex,"+
						" get stats for pindex: %s, err: %v", reqs.url, err)
					continue
				}

				if _, exists := pindexesData.PIndexes[pindex]; exists {
					log.Printf("pindex_copy_request: pindex: %s found on node:"+
						" %s", reqs.url, pindex)

					responseCh <- &statsResp{
						uuid:   reqs.uuid,
						status: true,
						err:    nil,
					}
					continue
				}

				// no pindex found on host
				responseCh <- &statsResp{
					uuid:   reqs.uuid,
					status: false,
					err:    fmt.Errorf("pindex not found"),
				}
				continue
			}
			wg.Done()
		}()
	}

	for node, url := range urlMap {
		requestCh <- &statsReq{uuid: node, url: url}
	}
	close(requestCh)
	wg.Wait()
	close(responseCh)

	var UUIDs []string
	for resp := range responseCh {
		if resp.status {
			UUIDs = append(UUIDs, resp.uuid)
		} else {
			log.Printf("pindex_copy_request: missing pindex on node %s,"+
				" err: %v", resp.uuid, resp.err)
		}
	}

	return UUIDs
}

// getStatsUrls forms the stats url map.
func getStatsUrls(uuids []string, authType string,
	nodeDefs *cbgt.NodeDefs) map[string]string {
	nodeURL := make(map[string]string)
	if nodeDefs == nil {
		return nodeURL
	}

	urlWithAuth := func(authType, urlStr string) (string, error) {
		if authType == "cbauth" {
			return cbgt.CBAuthURL(urlStr)
		}
		return urlStr, nil
	}

	for _, uuid := range uuids {
		if node, ok := nodeDefs.NodeDefs[uuid]; ok {
			hostPortUrl := "http://" + node.HostPort
			if u, err := node.HttpsURL(); err == nil {
				hostPortUrl = u
			}

			urlStr := hostPortUrl + "/api/stats?partitions=true"
			urlStr, err := urlWithAuth(authType, urlStr)
			if err != nil {
				continue
			}

			nodeURL[uuid] = urlStr
		}
	}

	return nodeURL
}

// srcPartition wraps the potential source partition
// with additional info to help prioritising it.
type srcPartition struct {
	nodeDef              *cbgt.NodeDef
	partitionCountOnNode int
	state                string
}

// srcPartitionSorter helps to prioritise the
// order of candidate source nodes.
// Preference order of nodes is dervied from,
// - same rack presence,
// - filtering state like primary/replica and
// - current number of partitions/load on the node.
type srcPartitionSorter struct {
	sps   []*srcPartition
	rack  string
	state string
}

func (ns *srcPartitionSorter) Len() int {
	return len(ns.sps)
}

func (ns *srcPartitionSorter) Less(i, j int) bool {
	si := ns.Score(i)
	sj := ns.Score(j)
	if si > sj {
		return true
	}
	if si > sj {
		return false
	}

	return false
}

func (ns srcPartitionSorter) Swap(i, j int) {
	ns.sps[i], ns.sps[j] = ns.sps[j], ns.sps[i]
}

func (ns *srcPartitionSorter) Score(i int) float64 {
	sp := ns.sps[i]

	// weightage is given to the preferred target state.
	stateSkipFactor := 1.0
	if ns.state != "" && sp.state != ns.state {
		stateSkipFactor = 0
	}

	// same rack nodes are higher scored.
	localRackBoostFactor := 0.2
	if sp.nodeDef.Container == ns.rack {
		localRackBoostFactor = 1
	}

	// nodes with high number existing partition load gets
	// lower score and vice versa.
	nodeLoadBalanceFactor := .3
	if sp.partitionCountOnNode != 0 {
		nodeLoadBalanceFactor *= float64(1) / float64(sp.partitionCountOnNode)
	}

	score := localRackBoostFactor *
		stateSkipFactor * nodeLoadBalanceFactor

	return score
}

func getWorkerCount(itemCount int) int {
	ncpu := runtime.NumCPU()
	if itemCount < ncpu {
		return itemCount
	}
	return ncpu
}

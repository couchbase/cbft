//  Copyright 2021-Present Couchbase, Inc.
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
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

type rateLimiter struct {
	mgr *cbgt.Manager
}

var limiter *rateLimiter

// To be set on process init ONLY
func InitRateLimiter(mgr *cbgt.Manager) error {
	if mgr == nil {
		return fmt.Errorf("manager not available")
	}
	limiter = &rateLimiter{mgr: mgr}
	return nil
}

// To be set on process init ONLY
var ServerlessMode bool
var IndexLimitPerSource = math.MaxInt
var ActivePartitionLimit = math.MaxInt
var ReplicaPartitionLimit = 3

// -----------------------------------------------------------------------------

// Pre-processing for a request
func processRequest(username, path string, req *http.Request) (bool, string) {
	if limiter == nil || !ServerlessMode {
		// rateLimiter not initialized or non-serverless mode
		return true, ""
	}

	if !isIndexPath(path) && !isQueryPath(path) {
		return true, ""
	}

	return limiter.processRequestInServerless(username, path, req)
}

// Post-processing after a response is shipped for a request
func completeRequest(username, path string, req *http.Request, egress int64) {
	// no-op
	return
}

// Limits index definitions from being introduced into the system based on
// the rateLimiter settings
func LimitIndexDef(mgr *cbgt.Manager, indexDef *cbgt.IndexDef) (*cbgt.IndexDef, error) {
	if limiter == nil || !ServerlessMode {
		// rateLimiter not initialized or non-serverless mode
		return indexDef, nil
	}

	if indexDef == nil {
		return nil, fmt.Errorf("indexDef not available")
	}

	// Applicable to fulltext indexes only.
	if indexDef.Type != "fulltext-index" {
		return indexDef, nil
	}

	return limiter.limitIndexDefInServerlessMode(indexDef)

}

// -----------------------------------------------------------------------------

// limitIndexDefInServerlessMode to be invoked ONLY when deploymentModel is
// "serverless".
func (r *rateLimiter) limitIndexDefInServerlessMode(indexDef *cbgt.IndexDef) (
	*cbgt.IndexDef, error) {
	if indexDef.PlanParams.IndexPartitions == 0 {
		indexDef.PlanParams.IndexPartitions = ActivePartitionLimit
	}
	if indexDef.PlanParams.NumReplicas == 0 {
		indexDef.PlanParams.NumReplicas = ReplicaPartitionLimit
	}

	if indexDef.PlanParams.IndexPartitions != ActivePartitionLimit ||
		indexDef.PlanParams.NumReplicas != ReplicaPartitionLimit {
		return nil, fmt.Errorf("limitIndexDef: support for indexes with" +
			" 1 active + 1 replica partitions only in serverless mode")
	}

	nodeDefs, err := r.mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, false)
	if err != nil || nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		return nil, fmt.Errorf("limitIndexDef: GetNodeDefs,"+
			" nodeDefs: %+v, err: %v", nodeDefs, err)
	}

	nodesOverHWM := NodeUUIDsWithUsageOverHWM(nodeDefs)
	if len(nodesOverHWM) != 0 {
		for _, nodeInfo := range nodesOverHWM {
			delete(nodeDefs.NodeDefs, nodeInfo.NodeUUID)
		}

		if len(nodeDefs.NodeDefs) < (ReplicaPartitionLimit + 1) {
			nodes, _ := json.Marshal(nodesOverHWM)

			// at least 2 nodes with usage below HWM needed to allow this index request
			return nil, fmt.Errorf("limitIndexDef: Cannot accommodate"+
				" index request: %v, resource utilization over limit(s) for nodes: %s",
				indexDef.Name, nodes)
		}

		// now track number of server groups in nodes with usage below HWM
		serverGroups := map[string]struct{}{}
		for _, nodeDef := range nodeDefs.NodeDefs {
			serverGroups[nodeDef.Container] = struct{}{}
		}

		if len(serverGroups) < (ReplicaPartitionLimit + 1) {
			nodes, _ := json.Marshal(nodesOverHWM)

			// nodes from at least 2 server groups needed to allow index request
			return nil, fmt.Errorf("limitIndexDef: Cannot accommodate"+
				" index request: %v, at least 2 nodes in separate server groups"+
				" with resource utilization below limit(s) needed, nodes above HWM: %s",
				indexDef.Name, nodes)
		}

	}

	return indexDef, nil
}

type CheckResult uint

const (
	CheckResultNormal CheckResult = iota
	CheckResultThrottle
	CheckResultReject
	CheckResultError
	CheckAccessNormal
	CheckAccessNoIngress
	CheckAccessError
)

func (r *rateLimiter) limitIndexCount(bucket string) (CheckResult, error) {
	_, indexDefsByName, err := r.mgr.GetIndexDefs(false)
	if err != nil {
		return CheckResultError, fmt.Errorf("failed to retrieve index defs")
	}

	maxIndexCountPerSource, found :=
		cbgt.ParseOptionsInt(r.mgr.Options(), "maxIndexCountPerSource")
	if !found {
		maxIndexCountPerSource = IndexLimitPerSource
	}

	indexCount := 0
	for _, indexDef := range indexDefsByName {
		if bucket == indexDef.SourceName {
			indexCount++
			// compare the index count of existing indexes
			// in system with the threshold.
			if indexCount >= maxIndexCountPerSource {
				return CheckResultReject, fmt.Errorf("rejecting create " +
					"request since index count limit per database has been reached")
			}
		}
	}
	return CheckResultNormal, nil
}

func (r *rateLimiter) obtainIndexDefFromIndexRequest(req *http.Request) (*cbgt.IndexDef, error) {
	requestBody, err := io.ReadAll(req.Body)
	if err != nil || len(requestBody) == 0 {
		return nil, fmt.Errorf("limiter: failed to parse the req body %v", err)
	}
	defer func() {
		req.Body = io.NopCloser(bytes.NewBuffer(requestBody))
	}()

	indexDef := cbgt.IndexDef{}
	if err := json.Unmarshal(requestBody, &indexDef); err != nil {
		return nil, fmt.Errorf("limiter: failed to unmarshal "+
			"the req body %v", err)
	}

	return &indexDef, nil
}

func (r *rateLimiter) obtainIndexSourceForQueryRequest(req *http.Request) ([]string, error) {
	bucket := rest.BucketNameLookup(req)
	if len(bucket) > 0 {
		// query request is to a scoped index
		return []string{bucket}, nil
	}

	indexName := rest.IndexNameLookup(req)

	var sources map[string]struct{}
	var err error
	if sources, err = obtainIndexSourceFromDefinition(
		r.mgr, indexName, sources); err != nil || len(sources) == 0 {
		return nil, fmt.Errorf("limiter: failed to obtain source(s) for index")
	}

	var rv []string
	for source := range sources {
		rv = append(rv, source)
	}

	return rv, nil
}

func (r *rateLimiter) regulateRequest(username, path string,
	req *http.Request) (CheckResult, time.Duration, error) {
	// No need to throttle/limit the request incase of delete op
	// Since it ultimately leads to cleaning up of resources.
	// Furthermore, no need to throttle/limit the GET request of index listing
	// which basically displays the index definition, since it doesn't impact the
	// disk usage in the system.
	if req.Method == "DELETE" || req.Method == "GET" {
		return CheckResultNormal, 0, nil
	}

	if isQueryPath(path) {
		// index names on query paths are always decorated, otherwise
		// there is a index not found error
		buckets, err := r.obtainIndexSourceForQueryRequest(req)
		if err != nil {
			return CheckResultError, 0, fmt.Errorf("failed to get index"+
				" source for request %v", err)
		}
		if len(buckets) > 1 {
			// index alias over indexes against multiple buckets
			return CheckResultError, 0, fmt.Errorf("querying indexes over " +
				" multiple buckets is not allowed")
		}
		return CheckQuotaRead(buckets[0], username, req)
	}

	// using the indexDef of the request to obtain the necessary info,
	// given that it is the only reliable source
	indexDef, err := r.obtainIndexDefFromIndexRequest(req)
	if err != nil {
		return CheckResultError, 0, fmt.Errorf("failed to get index "+
			"info from request %v", err)
	}

	// Regulator applicable to full text indexes ONLY
	if indexDef.Type != "fulltext-index" {
		return CheckResultNormal, 0, nil
	}

	action, duration, err := CheckQuotaWrite(nil, indexDef.SourceName, username, false, req)
	if action == CheckResultNormal {
		action, err = CheckAccess(indexDef.SourceName, username)
		if indexDef.UUID == "" {
			// This is a CREATE INDEX request and NOT an UPDATE INDEX request.
			createReqResult, err := r.limitIndexCount(indexDef.SourceName)
			return createReqResult, 0, err
		}
	}
	return action, duration, err
}

// custom function just to check out the LMT.
// can be removed and made part of the original rate Limiter later on, if necessary.
func (r *rateLimiter) processRequestInServerless(username, path string,
	req *http.Request) (bool, string) {
	action, _, err := r.regulateRequest(username, path, req)

	switch action {
	// support only limiting of indexing and query requests at the request
	// admission layer. Furthermore, reject those indexing requests if the
	// tenant has hit a storage limit.
	case CheckResultReject, CheckResultThrottle, CheckAccessNoIngress:
		return false, fmt.Sprintf("limiting/throttling: the request has been "+
			"rejected according to regulator, msg: %v", err)
	case CheckResultError, CheckAccessError:
		return false, fmt.Sprintf("limiting/throttling: failed to regulate the "+
			"request err: %v", err)
	default:
	}

	return true, ""
}

// -----------------------------------------------------------------------------

type nodeDetails struct {
	NodeUUID  string
	NodeStats *UtilizationStats
}

func NodeUUIDsWithUsageOverHWM(nodeDefs *cbgt.NodeDefs) []*nodeDetails {
	nodesStats := NodesUtilStats(nodeDefs)

	if len(nodesStats) == 0 {
		return nil
	}

	nodesOverHWM := []*nodeDetails{}
	for k, v := range nodesStats {
		if v.IsUtilizationOverHWM() {
			nodesOverHWM = append(nodesOverHWM, &nodeDetails{
				NodeUUID: k,
				NodeStats: &UtilizationStats{
					DiskUsage:   v.DiskUsage,
					MemoryUsage: v.MemoryUsage,
					CPUUsage:    v.CPUUsage,
				},
			})
		} else {
			// remove entries of nodes whose usage is within limits
			delete(nodesStats, k)
		}
	}

	if len(nodesOverHWM) > 0 {
		if out, err := json.Marshal(nodesStats); err == nil {
			log.Warnf("Nodes showing resource utilization higher than HWM: %s",
				string(out))
		}
	}

	return nodesOverHWM
}

// Override-able for unit testing
var NodesUtilStats = func(nodeDefs *cbgt.NodeDefs) map[string]*NodeUtilStats {
	if nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
		return nil
	}

	var m sync.Mutex
	rv := map[string]*NodeUtilStats{}
	addToRV := func(n *cbgt.NodeDef, s *NodeUtilStats) {
		m.Lock()
		rv[n.UUID] = s
		m.Unlock()
	}

	var wg sync.WaitGroup
	for _, nodeDef := range nodeDefs.NodeDefs {
		wg.Add(1)
		go func(n *cbgt.NodeDef) {
			if stats, err := obtainNodeUtilStats(n); err == nil {
				addToRV(n, stats)
			} else {
				log.Warnf("limits: error getting node %s stats: %v", n.UUID, err)
			}
			wg.Done()
		}(nodeDef)
	}
	wg.Wait()

	return rv
}

// Override-able for unit testing
var CanNodeAccommodateRequest = func(nodeDef *cbgt.NodeDef) bool {
	stats, err := obtainNodeUtilStats(nodeDef)
	if err != nil {
		log.Errorf("limits: unable to get stats for nodeDef: %+v, err: %v", nodeDef, err)
		return false
	}

	return !stats.IsUtilizationOverHWM()
}

type UtilizationStats struct {
	DiskUsage   uint64 `json:"utilization:diskBytes"`
	MemoryUsage uint64 `json:"utilization:memoryBytes"`
	CPUUsage    uint64 `json:"utilization:cpuPercent"`
}

type NodeUtilStats struct {
	UtilizationStats

	HighWaterMark             float64 `json:"resourceUtilizationHighWaterMark"`
	LowWaterMark              float64 `json:"resourceUtilizationLowWaterMark"`
	UnderUtilizationWaterMark float64 `json:"resourceUnderUtilizationWaterMark"`

	BillableUnitsRate uint64 `json:"utilization:billableUnitsRate"`

	LimitBillableUnitsRate uint64 `json:"limits:billableUnitsRate"`
	LimitDiskUsage         uint64 `json:"limits:diskBytes"`
	LimitMemoryUsage       uint64 `json:"limits:memoryBytes"`
}

type nodeUtilStatsHandler struct {
	mgr *cbgt.Manager
}

func NewNodeUtilStatsHandler(mgr *cbgt.Manager) *nodeUtilStatsHandler {
	return &nodeUtilStatsHandler{mgr: mgr}
}

func (nh *nodeUtilStatsHandler) ServeHTTP(w http.ResponseWriter,
	req *http.Request) {
	rv := make(map[string]interface{})
	gatherNodeUtilStats(nh.mgr, rv)

	rest.MustEncode(w, rv)
}

func (ns *NodeUtilStats) IsUtilizationOverHWM() bool {
	if ns.LimitMemoryUsage > 0 &&
		ns.MemoryUsage >= uint64(ns.HighWaterMark*float64(ns.LimitMemoryUsage)) {
		// memory usage exceeds limit
		return true
	}

	if ns.CPUUsage >= uint64(ns.HighWaterMark*100) {
		// cpu usage exceeds limit
		return true
	}

	return false
}

func obtainNodeUtilStats(nodeDef *cbgt.NodeDef) (*NodeUtilStats, error) {
	if nodeDef == nil || len(nodeDef.HostPort) == 0 {
		return nil, fmt.Errorf("nodeDef unavailable")
	}

	hostPortUrl := "http://" + nodeDef.HostPort
	if cbgt.GetSecuritySetting().EncryptionEnabled {
		if u, err := nodeDef.HttpsURL(); err == nil {
			hostPortUrl = u
		}
	}
	url := hostPortUrl + "/api/nodeUtilStats"

	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := cbgt.HttpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET /api/nodeUtilStats for node %s, status code: %v",
			nodeDef.UUID, resp.StatusCode)
	}

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(respBuf) == 0 {
		return nil, fmt.Errorf("response was empty")
	}

	var stats *NodeUtilStats
	if err := json.Unmarshal(respBuf, &stats); err != nil {
		return nil, err
	}

	return stats, nil
}

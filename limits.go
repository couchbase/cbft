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
	"io/ioutil"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

const bytesPerMB = 1048576
const windowLength = 1 * time.Minute
const indexPath = "/api/index/{indexName}"
const queryPath = "/api/index/{indexName}/query"

// -----------------------------------------------------------------------------

var indexPartitionsLimit = uint32(math.MaxUint32)
var indexReplicasLimit = uint32(math.MaxUint32)

func getIndexPartitionsLimit() uint32 {
	return atomic.LoadUint32(&indexPartitionsLimit)
}

func setIndexPartitionsLimit(to uint32) {
	atomic.StoreUint32(&indexPartitionsLimit, to)
}

func getIndexReplicasLimit() uint32 {
	return atomic.LoadUint32(&indexReplicasLimit)
}

func setIndexReplicasLimit(to uint32) {
	atomic.StoreUint32(&indexReplicasLimit, to)
}

// -----------------------------------------------------------------------------

func SubscribeToLimits(mgr *cbgt.Manager) {
	limiter = &rateLimiter{
		mgr:            mgr,
		requestCache:   make(map[string]*requestStats),
		indexCache:     make(map[string]map[string]struct{}),
		pendingIndexes: make(map[string]string),
	}
	cbgt.RegisterConfigRefreshCallback("fts/limits", limiter.refreshLimits)
}

// -----------------------------------------------------------------------------

// Pre-processing for a request
func processRequest(username, path string, req *http.Request) (bool, string) {
	if ServerlessMode {
		if path != indexPath && path != queryPath {
			return true, ""
		}
		return limiter.processRequestForLimiting(username, path, req)
	}

	if limiter == nil || !limiter.isActive() {
		// rateLimiter not active
		return true, ""
	}

	// evaluate requests at endpoints ..
	//     index creation/update/deletion: /api/index/{indexName}
	//     search request:                 /api/index/{indexName}/query
	if path != indexPath && path != queryPath {
		return true, ""
	}

	return limiter.processRequest(username, path, req)
}

// Post-processing after a response is shipped for a request
func completeRequest(username, path string, req *http.Request, egress int64) {
	if ServerlessMode {
		limiter.completeRequestProcessing(path, req)
		return
	}

	if limiter == nil || !limiter.isActive() {
		// rateLimiter not active
		return
	}

	// evaluate request completion at endpoints ..
	//     index creation/update/deletion: /api/index/{indexName}
	//     search request:                 /api/index/{indexName}/query
	if path != indexPath && path != queryPath {
		return
	}

	limiter.completeRequest(username, path, req, egress)
}

// -----------------------------------------------------------------------------

// Limits index definitions from being introduced into the system based on
// the rateLimiter settings
func limitIndexDef(mgr *cbgt.Manager, indexDef *cbgt.IndexDef) (*cbgt.IndexDef, error) {
	if indexDef == nil {
		return nil, fmt.Errorf("indexDef not available")
	}

	if ServerlessMode {
		return limitIndexDefInServerlessMode(mgr, indexDef)
	}

	if limiter == nil || !limiter.isActive() {
		// rateLimiter not active
		return indexDef, nil
	}

	partitionsLimit := int(getIndexPartitionsLimit())
	if indexDef.PlanParams.IndexPartitions > partitionsLimit {
		return nil,
			fmt.Errorf("partition limit exceeded (%v > %v)",
				indexDef.PlanParams.IndexPartitions, partitionsLimit)
	}

	replicasLimit := int(getIndexReplicasLimit())
	if indexDef.PlanParams.NumReplicas > replicasLimit {
		return nil,
			fmt.Errorf("replica limit exceeded (%v > %v)",
				indexDef.PlanParams.NumReplicas, replicasLimit)
	}

	bucket := indexDef.SourceName
	scope, _, _ := GetScopeCollectionsFromIndexDef(indexDef)
	numIndexesLimit, err := obtainIndexesLimitForScope(limiter.mgr, bucket, scope)
	if err != nil {
		return nil, err
	}

	key := getBucketScopeKey(bucket, scope)

	limiter.m.Lock()
	defer limiter.m.Unlock()

	entry, exists := limiter.indexCache[key]
	if !exists {
		limiter.indexCache[key] = make(map[string]struct{})
		limiter.pendingIndexes[indexDef.Name] = key
		return indexDef, nil
	}

	if _, exists := entry[indexDef.Name]; exists {
		// allow index update
		return indexDef, nil
	}

	numActiveIndexes := len(entry)
	numPendingIndexes := 0
	for _, v := range limiter.pendingIndexes {
		if v == key {
			numPendingIndexes++
		}
	}

	if numIndexesLimit > 0 &&
		(numActiveIndexes >= numIndexesLimit ||
			(numActiveIndexes+numPendingIndexes) >= numIndexesLimit) {
		return nil, fmt.Errorf("Exceeds indexes limit for scope: %s,"+
			" num_fts_indexes (active + pending): (%v + %v), limit: %v",
			scope, numActiveIndexes, numPendingIndexes, numIndexesLimit)
	}
	limiter.pendingIndexes[indexDef.Name] = key

	return indexDef, nil
}

// limitIndexDefInServerlessMode to be invoked ONLY when deploymentModel is
// "serverless".
func limitIndexDefInServerlessMode(mgr *cbgt.Manager, indexDef *cbgt.IndexDef) (
	*cbgt.IndexDef, error) {
	if indexDef.PlanParams.IndexPartitions != 1 ||
		indexDef.PlanParams.NumReplicas != 1 {
		return nil, fmt.Errorf("limitIndexDef: support for indexes with" +
			" 1 active + 1 replica partitions only in serverless mode")
	}

	if mgr != nil {
		nodeDefs, err := mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, false)
		if err != nil || nodeDefs == nil || len(nodeDefs.NodeDefs) == 0 {
			return nil, fmt.Errorf("limitIndexDef: GetNodeDefs,"+
				" nodeDefs: %+v, err: %v", nodeDefs, err)
		}

		nodesOverHWM := NodeUUIDsWithUsageOverHWM(nodeDefs)
		if len(nodesOverHWM) != 0 {
			for _, nodeUUID := range nodesOverHWM {
				delete(nodeDefs.NodeDefs, nodeUUID)
			}

			if len(nodeDefs.NodeDefs) < 2 {
				// at least 2 nodes with usage below HWM needed to allow this index request
				return nil, fmt.Errorf("limitIndexDef: Cannot accommodate"+
					" index request: %v, resource utilization over limit(s)", indexDef.Name)
			}

			// now track number of server groups in nodes with usage below HWM
			serverGroups := map[string]struct{}{}
			for _, nodeDef := range nodeDefs.NodeDefs {
				serverGroups[nodeDef.Container] = struct{}{}
			}

			if len(serverGroups) < 2 {
				// nodes from at least 2 server groups needed to allow index request
				return nil, fmt.Errorf("limitIndexDef: Cannot accommodate"+
					" index request: %v, at least 2 nodes in separate server groups"+
					" with resource utilization below limit(s) needed", indexDef.Name)
			}

		}
	}

	return indexDef, nil
}

// -----------------------------------------------------------------------------

func NodeUUIDsWithUsageOverHWM(nodeDefs *cbgt.NodeDefs) []string {
	nodesStats := NodesUtilStats(nodeDefs)

	if len(nodesStats) == 0 {
		return nil
	}

	nodesOverHWM := []string{}
	for k, v := range nodesStats {
		if v.IsUtilizationOverHWM() {
			nodesOverHWM = append(nodesOverHWM, k)
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
		// unable to get node stats!?
		return false
	}

	return !stats.IsUtilizationOverHWM()
}

type NodeUtilStats struct {
	HighWaterMark             float64 `json:"resourceUtilizationHighWaterMark"`
	LowWaterMark              float64 `json:"resourceUtilizationLowWaterMark"`
	UnderUtilizationWaterMark float64 `json:"resourceUnderUtilizationWaterMark"`

	BillableUnitsRate uint64 `json:"utilization:billableUnitsRate"`
	DiskUsage         uint64 `json:"utilization:diskBytes"`
	MemoryUsage       uint64 `json:"utilization:memoryBytes"`
	CPUUsage          uint64 `json:"utilization:cpuPercent"`

	LimitBillableUnitsRate uint64 `json:"limits:billableUnitsRate"`
	LimitDiskUsage         uint64 `json:"limits:diskBytes"`
	LimitMemoryUsage       uint64 `json:"limits:memoryBytes"`
}

func (ns *NodeUtilStats) IsUtilizationOverHWM() bool {
	if ns.LimitBillableUnitsRate > 0 &&
		ns.BillableUnitsRate >= uint64(ns.HighWaterMark*float64(ns.LimitBillableUnitsRate)) {
		// billableUnitsRate exceeds limit
		return true
	}

	if ns.LimitDiskUsage > 0 &&
		ns.DiskUsage >= uint64(ns.HighWaterMark*float64(ns.LimitDiskUsage)) {
		// disk usage exceeds limit
		return true
	}

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

	url := "http://" + nodeDef.HostPort + "/api/nsstats"

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

	respBuf, err := ioutil.ReadAll(resp.Body)
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

// -----------------------------------------------------------------------------

type requestStats struct {
	live                   int
	stamp                  time.Time
	countSinceStamp        int
	ingressBytesSinceStamp int64
	egressBytesSinceStamp  int64
}

type rateLimiter struct {
	active uint32

	mgr *cbgt.Manager

	m sync.Mutex // Protects the fields that follow.

	// Cache of request stats mapped at a user level.
	requestCache map[string]*requestStats

	// Cache of index names mapped at bucket:scope level.
	// (use getBucketScopeKey(..) for map lookup)
	indexCache map[string]map[string]struct{}

	// Pending index names mapped to bucket:scope.
	pendingIndexes map[string]string
}

var limiter *rateLimiter

// -----------------------------------------------------------------------------

func (e *rateLimiter) isActive() bool {
	return atomic.LoadUint32(&e.active) != 0
}

func (e *rateLimiter) refreshLimits(status int) error {
	if status&cbgt.AuthChange_limits != 0 {
		ss := cbgt.GetSecuritySetting()
		if ss.EnforceLimits {
			atomic.StoreUint32(&e.active, 1)
			setIndexPartitionsLimit(1) // allow only single partition
			setIndexReplicasLimit(0)   // do not allow replica partitions
		} else {
			atomic.StoreUint32(&e.active, 0)
			setIndexPartitionsLimit(math.MaxUint32)
			setIndexReplicasLimit(math.MaxUint32)
			e.reset()
		}
	}
	return nil
}

func (e *rateLimiter) reset() {
	e.m.Lock()
	e.requestCache = make(map[string]*requestStats)
	e.indexCache = make(map[string]map[string]struct{})
	e.m.Unlock()
}

// -----------------------------------------------------------------------------

var ServerlessMode bool

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

func (e *rateLimiter) getIndexKeyLOCKED(indexName string) string {
	for key, entry := range e.indexCache {
		if _, ok := entry[indexName]; ok {
			return key
		}
	}
	return ""
}

func extractSourceNameFromReq(req *http.Request) (string, error) {
	requestBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return "", fmt.Errorf("limiter: failed to parse the req body %v", err)
	}

	indexDef := cbgt.IndexDef{}

	if len(requestBody) > 0 {
		err := json.Unmarshal(requestBody, &indexDef)
		if err != nil {
			return "", fmt.Errorf("limiter: failed to unmarshal "+
				"the req body %v", err)
		}
	}
	req.Body = ioutil.NopCloser(bytes.NewBuffer(requestBody))
	return indexDef.SourceName, nil
}

func checkAndSplitDecoratedIndexName(indexName string) (bool, string, string, string) {
	if strings.Contains(indexName, ".") {
		uIndexPos := strings.LastIndex(indexName, ".")
		userIndex := indexName[uIndexPos+1:]
		scopeIndex := strings.LastIndex(indexName[:uIndexPos], ".")
		bucket := indexName[:scopeIndex]

		return true, bucket, indexName[scopeIndex+1 : uIndexPos], userIndex
	}
	return false, "", "", indexName
}

func (e *rateLimiter) limitIndexCount(bucket string) (CheckResult, error) {
	_, indexDefsByName, err := e.mgr.GetIndexDefs(false)
	if err != nil {
		return CheckResultError, fmt.Errorf("failed to retrieve index defs")
	}

	maxIndexCountPerSource := 20
	v, found := cbgt.ParseOptionsInt(e.mgr.Options(), "maxIndexCountPerSource")
	if found {
		maxIndexCountPerSource = v
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

func (e *rateLimiter) regulateRequest(username, path string,
	req *http.Request) (CheckResult, time.Duration, error) {

	// No need to throttle/limit the request incase of delete op
	// Since it ultimately leads to cleaning up of resources.
	// Furthermore, no need to throttle/limit the GET request of index listing
	// which basically displays the index definition, since it doesn't impact the
	// disk usage in the system.
	if req.Method == "DELETE" || req.Method == "GET" {
		return CheckResultNormal, 0, nil
	}

	indexName := rest.IndexNameLookup(req)
	// need to see which bucket EXACTLY is this request for.
	decorated, bucket, _, _ := checkAndSplitDecoratedIndexName(indexName)
	if !decorated {
		bucketScopeKey := e.getIndexKeyLOCKED(indexName)
		bucket = strings.Split(bucketScopeKey, ":")[0]
	}

	if path == queryPath {
		return CheckQuotaRead(bucket, username, req)
	}

	var createReq bool
	if bucket == "" {
		createReq = true
		// bucket is empty string only while CREATE index case
		sourceName, err := extractSourceNameFromReq(req)
		if err != nil {
			return CheckResultError, 0, fmt.Errorf("failed to get index "+
				"info from request %v", err)
		}
		bucket = sourceName
	}

	action, duration, err := CheckQuotaWrite(nil, bucket, username, false, req)
	if action == CheckResultNormal {
		action, err = CheckAccess(bucket, username)
		if createReq {
			createReqResult, err := e.limitIndexCount(bucket)
			return createReqResult, 0, err
		}
	}
	return action, duration, err
}

// custom function just to check out the LMT.
// can be removed and made part of the original rate Limiter later on, if necessary.
func (e *rateLimiter) processRequestForLimiting(username, path string,
	req *http.Request) (bool, string) {
	e.m.Lock()
	defer e.m.Unlock()

	if path == indexPath {
		// Refresh the indexCache of the rateLimiter at this point,
		// to track updates that have been received at other nodes.
		//
		// Also, pre-processing here for a DELETE INDEX request only
		// (pre-processing for CREATE and UPDATE INDEX requests is handled
		//  within PrepareIndexDef callback for the IndexDef via limitIndexDef).
		e.updateIndexCacheLOCKED(req)
	}

	action, _, err := e.regulateRequest(username, path, req)

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

// custom function just to check out the LMT.
// can be removed and made part of the original rate Limiter later on, if necessary.
func (e *rateLimiter) completeRequestProcessing(path string, req *http.Request) {
	e.m.Lock()
	defer e.m.Unlock()

	if path == indexPath {
		// post-processing for INDEX requests
		e.completeIndexRequestLOCKED(req)
	}
}

// -----------------------------------------------------------------------------

func (e *rateLimiter) processRequest(username, path string, req *http.Request) (bool, string) {
	limits, _ := cbauth.GetUserLimits(username, "local", "fts")

	e.m.Lock()
	defer e.m.Unlock()

	if path == indexPath {
		// Refresh the indexCache of the rateLimiter at this point,
		// to track updates that have been received at other nodes.
		//
		// Also, pre-processing here for a DELETE INDEX request only
		// (pre-processing for CREATE and UPDATE INDEX requests is handled
		//  within PrepareIndexDef callback for the IndexDef via limitIndexDef).
		e.updateIndexCacheLOCKED(req)
	}

	now := time.Now()
	ingress := req.ContentLength

	entry, exists := e.requestCache[username]
	if !exists {
		entry = &requestStats{stamp: now}
		e.requestCache[username] = entry
	} else {
		maxConcurrentRequests, _ := limits["num_concurrent_requests"]
		if maxConcurrentRequests > 0 &&
			entry.live >= maxConcurrentRequests {
			// reject, surpassed the concurrency limit
			return false, fmt.Sprintf("num_concurrent_requests: %v, limit: %v",
				entry.live, maxConcurrentRequests)
		}

		if now.Sub(entry.stamp) < windowLength {
			maxQueriesPerMin, _ := limits["num_queries_per_min"]
			if path == queryPath && maxQueriesPerMin > 0 &&
				entry.countSinceStamp >= maxQueriesPerMin {
				// reject, surpassed the queries per minute limit
				return false, fmt.Sprintf("num_queries_per_min: %v, limit: %v",
					entry.countSinceStamp, maxQueriesPerMin)
			}

			maxIngressPerMin := int64(limits["ingress_mib_per_min"] * bytesPerMB)
			if maxIngressPerMin > 0 &&
				entry.ingressBytesSinceStamp >= maxIngressPerMin {
				// reject, surpassed the ingress per minute limit
				return false, fmt.Sprintf("ingress_mib_per_min: %v, limit: %v",
					entry.ingressBytesSinceStamp, maxIngressPerMin)
			}

			maxEgressPerMin := int64(limits["egress_mib_per_min"] * bytesPerMB)
			if maxEgressPerMin > 0 &&
				entry.egressBytesSinceStamp >= maxEgressPerMin {
				// reject, surpassed the egress per minute limit
				return false, fmt.Sprintf("egress_mib_per_min: %v, limit: %v",
					entry.egressBytesSinceStamp, maxEgressPerMin)
			}
		} else {
			entry.stamp = now
			entry.countSinceStamp = 0
			entry.ingressBytesSinceStamp = 0
			entry.egressBytesSinceStamp = 0
		}
	}

	entry.live++
	if path == queryPath {
		entry.countSinceStamp++
	}
	entry.ingressBytesSinceStamp += ingress

	return true, ""
}

func (e *rateLimiter) completeRequest(username, path string, req *http.Request, egress int64) {
	e.m.Lock()
	defer e.m.Unlock()

	if path == indexPath {
		// post-processing for INDEX requests
		e.completeIndexRequestLOCKED(req)
	}

	entry, exists := e.requestCache[username]
	if !exists {
		return
	}

	now := time.Now()

	if entry.live > 0 {
		entry.live--
	}

	if now.Sub(entry.stamp) < windowLength {
		entry.egressBytesSinceStamp += egress
	} else {
		entry.stamp = now
		entry.countSinceStamp = 0
		entry.ingressBytesSinceStamp = 0
		entry.egressBytesSinceStamp = 0
	}
}

// -----------------------------------------------------------------------------

func (e *rateLimiter) updateIndexCacheLOCKED(req *http.Request) {
	// GetIndexDefs(..) without a refresh should be a fast operation
	_, indexDefsByName, err := e.mgr.GetIndexDefs(false)
	if err != nil {
		return
	}

	// Clear out the indexCache entries for all bucket:scope keys;
	// This cache is updated with the latest subsequently.
	e.indexCache = make(map[string]map[string]struct{})

	for indexName, indexDef := range indexDefsByName {
		scope, _, _ := GetScopeCollectionsFromIndexDef(indexDef)
		key := getBucketScopeKey(indexDef.SourceName, scope)
		if _, exists := e.indexCache[key]; !exists {
			e.indexCache[key] = map[string]struct{}{indexName: struct{}{}}
		} else {
			e.indexCache[key][indexName] = struct{}{}
		}
	}

	// Update index cache if request received was for deleting an index
	// definition.
	if req.Method != "DELETE" {
		return
	}
	// This is invoked prior to the index deletion, so the index definition
	// should still be available in the system.

	indexName := rest.IndexNameLookup(req)
	if indexDef, exists := indexDefsByName[indexName]; exists {
		scope, _, _ := GetScopeCollectionsFromIndexDef(indexDef)
		key := getBucketScopeKey(indexDef.SourceName, scope)
		if entry, exists := e.indexCache[key]; exists {
			delete(entry, indexName)
		}
	}
}

func (e *rateLimiter) completeIndexRequestLOCKED(req *http.Request) {
	if req.Method != "PUT" {
		// skip GET, DELETE
		return
	}

	// This is invoked after an index introduction/update, so the index
	// definition should be available in the system.
	indexName := rest.IndexNameLookup(req)
	if !strings.Contains(indexName, ".") {
		bucketScope := e.pendingIndexes[indexName]
		delete(e.pendingIndexes, indexName)
		delimPos := strings.LastIndex(bucketScope, ":")
		indexName = decorateIndexNameWithKeySpace(bucketScope[:delimPos],
			bucketScope[delimPos+1:], indexName, false)
	}

	if _, indexDefsByName, err := e.mgr.GetIndexDefs(false); err == nil {
		if indexDef, exists := indexDefsByName[indexName]; exists {
			scope, _, _ := GetScopeCollectionsFromIndexDef(indexDef)
			key := getBucketScopeKey(indexDef.SourceName, scope)
			entry, exists := e.indexCache[key]
			if !exists {
				e.indexCache[key] = map[string]struct{}{indexName: struct{}{}}
			} else {
				// adds or updates index entry
				entry[indexName] = struct{}{}
			}
		}
		// whether index creation was successful or not, the pending
		// entry will need to be removed
		delete(e.pendingIndexes, indexName)
	}
}

// -----------------------------------------------------------------------------

// Generates key to use for rateLimiter's indexCache map.
func getBucketScopeKey(bucket, scope string) string {
	return bucket + ":" + scope
}

// -----------------------------------------------------------------------------

func obtainIndexesLimitForScope(mgr *cbgt.Manager, bucket, scope string) (int, error) {
	if mgr == nil {
		return 0, fmt.Errorf("manager is nil")
	}

	if len(bucket) == 0 || len(scope) == 0 {
		return 0, fmt.Errorf("bucket/scope not available")
	}

	manifest, err := GetBucketManifest(bucket)
	if err != nil {
		return 0, err
	}

	for i := range manifest.Scopes {
		if manifest.Scopes[i].Name == scope {
			numIndexesLimit := 0
			if limits, exists := manifest.Scopes[i].Limits["fts"]; exists {
				numIndexesLimit = limits["num_fts_indexes"]
			}

			return numIndexesLimit, nil
		}
	}

	return 0, fmt.Errorf("scope not found")
}

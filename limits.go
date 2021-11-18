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
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
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
func limitIndexDef(indexDef *cbgt.IndexDef) (*cbgt.IndexDef, error) {
	if limiter == nil || !limiter.isActive() {
		// rateLimiter not active
		return indexDef, nil
	}

	if indexDef == nil {
		return nil, fmt.Errorf("indexDef not available")
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
		return nil, fmt.Errorf("num_fts_indexes (active + pending), (%v + %v) >= %v",
			numActiveIndexes, numPendingIndexes, numIndexesLimit)
	}

	limiter.pendingIndexes[indexDef.Name] = key

	return indexDef, nil
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
			return false, fmt.Sprintf("num_concurrent_requests, %v >= %v",
				entry.live, maxConcurrentRequests)
		}

		if now.Sub(entry.stamp) < windowLength {
			maxQueriesPerMin, _ := limits["num_queries_per_min"]
			if maxQueriesPerMin > 0 &&
				entry.countSinceStamp >= maxQueriesPerMin {
				// reject, surpassed the queries per minute limit
				return false, fmt.Sprintf("num_queries_per_min, %v >= %v",
					entry.countSinceStamp, maxQueriesPerMin)
			}

			maxIngressPerMin := int64(limits["ingress_mib_per_min"] * bytesPerMB)
			if maxIngressPerMin > 0 &&
				entry.ingressBytesSinceStamp >= maxIngressPerMin {
				// reject, surpassed the ingress per minute limit
				return false, fmt.Sprintf("ingress_mib_per_min, %v >= %v",
					entry.ingressBytesSinceStamp, maxIngressPerMin)
			}

			maxEgressPerMin := int64(limits["egress_mib_per_min"] * bytesPerMB)
			if maxEgressPerMin > 0 &&
				entry.egressBytesSinceStamp >= maxEgressPerMin {
				// reject, surpassed the egress per minute limit
				return false, fmt.Sprintf("egress_mib_per_min, %v >= %v",
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
	entry.countSinceStamp++
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

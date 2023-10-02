//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt/rest"

	log "github.com/couchbase/clog"
)

type sizeFunc func(interface{}) uint64

type appHerder struct {
	memQuota             int64
	appQuota             int64
	indexQuota           int64
	queryQuota           int64
	concurrentMergeLimit int64 // max number of concurrent merges

	overQuotaCh chan struct{}

	m                sync.Mutex
	waitCond         *sync.Cond
	waitingMutations int
	waitingBatches   int

	indexes map[interface{}]sizeFunc

	// Tracks estimated memory used by running queries
	runningQueryUsed uint64

	// states for log deduplication
	memUsedPrev        int64
	pimPrev            int64 // pre indexing memory
	waitingBatchesPrev int64
	indexesPrev        int64

	mm sync.Mutex

	// map of index name -> count of ongoing merges.
	// Will be decremented on merger progress
	indexesMergeCount map[interface{}]int
}

func newAppHerder(memQuota uint64, appRatio, indexRatio,
	queryRatio float64, overQuotaCh chan struct{}, concurrentMergeLimit int64,
	mergeCapChangech chan int64) *appHerder {
	ah := &appHerder{
		memQuota:          int64(memQuota),
		overQuotaCh:       overQuotaCh,
		indexes:           map[interface{}]sizeFunc{},
		indexesMergeCount: map[interface{}]int{},
	}

	ah.appQuota = int64(float64(ah.memQuota) * appRatio)
	ah.indexQuota = int64(float64(ah.appQuota) * indexRatio)
	ah.queryQuota = int64(float64(ah.appQuota) * queryRatio)
	ah.concurrentMergeLimit = concurrentMergeLimit
	// Start a new routine to receive the new cap - runs for the lifetime of the cbft process
	// Should acquire the mm lock(a briefly held lock since it's only for rejections)
	// and change it
	go func(ch chan int64) {
		for {
			select {
			case newCap := <-ch:
				if newCap != ah.concurrentMergeLimit {
					ah.mm.Lock()
					log.Printf("app_herder: updating concurrent merge limit to "+
						" %d \n", newCap)
					ah.concurrentMergeLimit = newCap
					ah.mm.Unlock()
				}
			}
		}
	}(mergeCapChangech)

	ah.waitCond = sync.NewCond(&ah.m)

	log.Printf("app_herder: memQuota: %d, appQuota: %d, indexQuota: %d, "+
		"queryQuota: %d", memQuota, ah.appQuota, ah.indexQuota, ah.queryQuota)

	if ah.appQuota <= 0 {
		log.Printf("app_herder: appQuota disabled")
	}
	if ah.indexQuota <= 0 {
		log.Printf("app_herder: indexQuota disabled")
	}
	if ah.indexQuota < 0 {
		log.Printf("app_herder: indexing also ignores appQuota")
	}
	if ah.queryQuota <= 0 {
		log.Printf("app_herder: queryQuota disabled")
	}
	if ah.queryQuota < 0 {
		log.Printf("app_herder: querying also ignores appQuota")
	}
	if ah.concurrentMergeLimit <= 0 {
		log.Printf("app_herder: concurrent merging limit disabled")
	}
	if ah.concurrentMergeLimit < 0 {
		log.Printf("app_herder: merging also ignores concurrent merging limit")
	}

	return ah
}

func (a *appHerder) Stats() map[string]interface{} {
	return map[string]interface{}{
		"TotWaitingIn":              atomic.LoadUint64(&cbft.TotHerderWaitingIn),
		"TotWaitingOut":             atomic.LoadUint64(&cbft.TotHerderWaitingOut),
		"TotOnBatchExecuteStartBeg": atomic.LoadUint64(&cbft.TotHerderOnBatchExecuteStartBeg),
		"TotOnBatchExecuteStartEnd": atomic.LoadUint64(&cbft.TotHerderOnBatchExecuteStartEnd),
		"TotQueriesRejected":        atomic.LoadUint64(&cbft.TotHerderQueriesRejected),
		"TotMergesSkipped":          atomic.LoadUint64(&cbft.TotMergesSkipped),
	}
}

// *** Indexing Callbacks

func (a *appHerder) onClose(c interface{}) bool {
	a.m.Lock()
	delete(a.indexes, c)
	a.awakeWaitersLOCKED("closing index")
	a.m.Unlock()

	// remove the indexes' merge count key on deletion
	a.mm.Lock()
	delete(a.indexesMergeCount, c)
	a.mm.Unlock()

	return true
}

func (a *appHerder) onMemoryUsedDropped(curMemoryUsed, prevMemoryUsed uint64) {
	a.awakeWaiters("memory used dropped")
}

func (a *appHerder) awakeWaiters(msg string) bool {
	a.m.Lock()
	a.awakeWaitersLOCKED(msg)
	a.m.Unlock()
	return true
}

func (a *appHerder) awakeWaitersLOCKED(msg string) {
	if a.waitingMutations > 0 || a.waitingBatches > 0 {
		if a.waitingBatchesPrev != int64(a.waitingBatches) ||
			a.indexesPrev != int64(len(a.indexes)) {

			log.Printf("app_herder: %s, indexes: %d, batches waiting: %d, mutations waiting: %d", msg,
				len(a.indexes), a.waitingBatches, a.waitingMutations)

			a.waitingBatchesPrev = int64(a.waitingBatches)
			a.indexesPrev = int64(len(a.indexes))
		}

		a.waitCond.Broadcast()
	}
}

func (a *appHerder) onIndexStart() bool {
	// negative means ignore both appQuota and indexQuota and let the
	// incoming batch proceed.  A zero indexQuota means ignore the
	// indexQuota, but continue to check the appQuota for incoming
	// batches.
	if a.indexQuota >= 0 {
		a.checkAndBlockIndex()
	}
	return true
}

func (a *appHerder) onBatchExecuteStart(c interface{}, s sizeFunc) bool {
	// negative means ignore both appQuota and indexQuota and let the
	// incoming batch proceed.  A zero indexQuota means ignore the
	// indexQuota, but continue to check the appQuota for incoming
	// batches.
	if a.indexQuota >= 0 {
		atomic.AddUint64(&cbft.TotHerderOnBatchExecuteStartBeg, 1)
		a.checkAndBlockBatch(c, s)
		atomic.AddUint64(&cbft.TotHerderOnBatchExecuteStartEnd, 1)
	}
	return true
}

func (a *appHerder) checkAndBlockIndex() {
	isOverQuota, _, _ := a.overMemQuotaForIndexing()
	if isOverQuota {
		a.m.Lock()
		defer a.m.Unlock()
	}
	for isOverQuota {
		atomic.AddUint64(&cbft.TotHerderWaitingIn, 1)

		a.waitingMutations++
		if a.overQuotaCh != nil {
			a.overQuotaCh <- struct{}{}
		}
		a.waitCond.Wait()
		a.waitingMutations--
		isOverQuota, _, _ = a.overMemQuotaForIndexing()

		atomic.AddUint64(&cbft.TotHerderWaitingOut, 1)
	}
}

// this method checks the current memory usage and blocks if it's over the quota
// it's used by the indexer to block when it's over the quota and wait for the
// persister to make progress, which would free up memory.
func (a *appHerder) checkAndBlockBatch(c interface{}, s sizeFunc) {
	a.m.Lock()
	defer a.m.Unlock()
	a.indexes[c] = s
	// MB-29504 workaround to try and prevent indexing from becoming completely
	// stuck.  The thinking is that if the indexing memUsed is 0, all data has
	// been flushed to disk, and we should allow it to proceed (even if we're
	// over quota in the bigger picture)
	if a.indexingMemoryLOCKED() == 0 {
		return
	}

	wasWaiting := false
	isOverQuota, preIndexingMemory, memUsed := a.overMemQuotaForIndexing()

	for isOverQuota {
		wasWaiting = true

		atomic.AddUint64(&cbft.TotHerderWaitingIn, 1)
		a.waitingBatches++

		// If we're over the memory quota, then wait for persister,
		// query or other progress.
		// log only if the values change from the last time.
		if a.memUsedPrev != memUsed || a.pimPrev != preIndexingMemory ||
			a.waitingBatchesPrev != int64(a.waitingBatches) ||
			a.indexesPrev != int64(len(a.indexes)) {
			log.Printf("app_herder: indexing over indexQuota: %d, memUsed: %d,"+
				" preIndexingMemory: %d, indexes: %d, waiting batches: %d, waiting mutations: %d", a.indexQuota,
				memUsed, preIndexingMemory, len(a.indexes), a.waitingBatches, a.waitingMutations)

			// capture the previous logged values as we don't want to log them again.
			a.memUsedPrev = memUsed
			a.pimPrev = preIndexingMemory
			a.waitingBatchesPrev = int64(a.waitingBatches)
			a.indexesPrev = int64(len(a.indexes))
		}

		if a.overQuotaCh != nil {
			a.overQuotaCh <- struct{}{}
		}

		a.waitCond.Wait()

		a.waitingBatches--
		atomic.AddUint64(&cbft.TotHerderWaitingOut, 1)

		isOverQuota, preIndexingMemory, memUsed = a.overMemQuotaForIndexing()
	}

	if wasWaiting {
		log.Printf("app_herder: indexing proceeding, indexes: %d, waiting batches: %d, waiting mutations: %d, "+
			"usage: %v", len(a.indexes), a.waitingBatches, a.waitingMutations, cbft.FetchCurMemoryUsed())
	}
}

func (a *appHerder) indexingMemoryLOCKED() (rv uint64) {
	for index, indexSizeFunc := range a.indexes {
		rv += indexSizeFunc(index)
	}

	return
}

func (a *appHerder) preIndexingMemory() (rv uint64) {
	// account for overhead from documents in batches
	rv += atomic.LoadUint64(&cbft.BatchBytesAdded) -
		atomic.LoadUint64(&cbft.BatchBytesRemoved)

	return
}

func (a *appHerder) overMemQuotaForIndexing() (bool, int64, int64) {
	memUsed := int64(cbft.FetchCurMemoryUsed())
	// now account for the overhead from documents ready in batches
	// but not yet executed
	preIndexingMemory := int64(a.preIndexingMemory())
	memUsed += preIndexingMemory // TODO: NOTE: this is perhaps double-counting

	// make sure indexing doesn't exceed the index portion of the quota
	if a.indexQuota > 0 && memUsed > a.indexQuota {
		return true, preIndexingMemory, memUsed
	}

	return a.appQuota > 0 && memUsed > a.appQuota, preIndexingMemory, memUsed
}

func (a *appHerder) overLimitForConcurrentMergingLOCKED(c interface{}) (int, bool) {
	// Total number of in-progress merges.
	currMerges := 0
	for _, c := range a.indexesMergeCount {
		currMerges += c
	}

	return currMerges, currMerges > int(a.concurrentMergeLimit)
}

func (a *appHerder) onPersisterProgress() bool {
	return a.awakeWaiters("persister progress")
}

func (a *appHerder) onMergerProgress(c interface{}) bool {
	a.awakeWaiters("merger progress")

	a.mm.Lock()
	a.indexesMergeCount[c]--
	a.mm.Unlock()

	return true
}

func (a *appHerder) continueMerge(c interface{}) bool {
	if a.concurrentMergeLimit <= 0 {
		return true
	}

	a.mm.Lock()

	if _, exists := a.indexesMergeCount[c]; !exists {
		a.indexesMergeCount[c] = 0
	}

	currMerges, isOverCap := a.overLimitForConcurrentMergingLOCKED(c)

	if isOverCap {
		log.Printf("app_herder: rejecting merge since it's over concurrent merge"+
			"limit %v, current merges: %v \n", a.concurrentMergeLimit, currMerges)
		a.mm.Unlock()
		atomic.AddUint64(&cbft.TotMergesSkipped, 1)
		return false
	}

	// Decremented on merger progress
	a.indexesMergeCount[c]++

	a.mm.Unlock()
	return true
}

// *** Query Interface

func (a *appHerder) queryHerderOnEvent() func(int, cbft.QueryEvent, uint64) error {
	return func(depth int, event cbft.QueryEvent, size uint64) error {
		switch event.Kind {
		case cbft.EventQueryStart:
			return a.onQueryStart(depth, size)

		case cbft.EventQueryEnd:
			return a.onQueryEnd(depth, size)

		default:
			return nil
		}
	}
}

func (a *appHerder) onQueryStart(depth int, size uint64) error {
	// negative queryQuota means ignore both appQuota and queryQuota
	// and let the incoming query proceed.  A zero queryQuota means
	// ignore the queryQuota, but continue to check the appQuota for
	// incoming queries.
	if a.queryQuota < 0 {
		return nil
	}

	a.m.Lock()

	// MB-30954 - similar to logic for indexing / MB-29504 (see:
	// overMemQuotaForIndexing) -- this workaround tries to
	// prevent querying from becoming completely stuck, on the
	// thinking that if there aren't any size-estimated queries at all
	// yet, then allow a single query to proceed (even if we're over
	// quota in the bigger picture).
	if depth == 0 && a.runningQueryUsed > 0 {
		// fetch memory used by process
		memUsed := int64(cbft.FetchCurMemoryUsed())

		// now account for overhead from the current query
		memUsed += int64(size)

		// first make sure querying (on it's own) doesn't exceed the
		// query portion of the quota
		if a.queryQuota > 0 && memUsed > a.queryQuota {
			log.Printf("app_herder: querying over queryQuota: %d,"+
				" estimated size: %d, runningQueryUsed: %d, memUsed: %d",
				a.queryQuota, size, a.runningQueryUsed, memUsed)

			a.m.Unlock()

			if a.overQuotaCh != nil {
				a.overQuotaCh <- struct{}{}
			}

			atomic.AddUint64(&cbft.TotHerderQueriesRejected, 1)
			return rest.ErrorQueryReqRejected
		}

		if a.appQuota > 0 && memUsed > a.appQuota {
			log.Printf("app_herder: querying over appQuota: %d,"+
				" estimated size: %d, runningQueryUsed: %d, memUsed: %d",
				a.appQuota, size, a.runningQueryUsed, memUsed)

			a.m.Unlock()

			if a.overQuotaCh != nil {
				a.overQuotaCh <- struct{}{}
			}

			return rest.ErrorQueryReqRejected
		}
	}

	// record the addition
	a.runningQueryUsed += size

	a.m.Unlock()
	return nil
}

func (a *appHerder) onQueryEnd(depth int, size uint64) error {
	a.m.Lock()
	a.runningQueryUsed -= size
	a.awakeWaitersLOCKED("query ended")
	a.m.Unlock()

	return nil
}

// *** Scorch Wrapper
func (a *appHerder) ScorchHerderOnEvent() func(scorch.Event) bool {
	return func(event scorch.Event) bool { return a.onScorchEvent(event) }
}

func scorchSize(s interface{}) uint64 {
	if ss, ok := s.(*scorch.Scorch); ok {
		if stats, ok := ss.Stats().(*scorch.Stats); ok {
			curEpoch := atomic.LoadUint64(&stats.CurRootEpoch)
			lastMergedEpoch := atomic.LoadUint64(&stats.LastMergedEpoch)
			lastPersistedEpoch := atomic.LoadUint64(&stats.LastPersistedEpoch)

			if curEpoch == lastMergedEpoch &&
				lastMergedEpoch == lastPersistedEpoch {
				return 0
			}
		}
	}

	return s.(*scorch.Scorch).MemoryUsed()
}

func (a *appHerder) onScorchEvent(event scorch.Event) bool {
	switch event.Kind {
	case scorch.EventKindClose:
		return a.onClose(event.Scorch)

	case scorch.EventKindIndexStart:
		return a.onIndexStart()

	case scorch.EventKindBatchIntroductionStart:
		return a.onBatchExecuteStart(event.Scorch, scorchSize)

	case scorch.EventKindPersisterProgress:
		return a.onPersisterProgress()

	case scorch.EventKindMergerProgress:
		return a.onMergerProgress(event.Scorch)

	case scorch.EventKindPreMergeCheck:
		return a.continueMerge(event.Scorch)

	default:
		return true
	}
}

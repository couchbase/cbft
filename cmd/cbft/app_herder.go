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
	"github.com/couchbase/moss"

	log "github.com/couchbase/clog"
)

type sizeFunc func(interface{}) uint64

type appHerder struct {
	memQuota   int64
	appQuota   int64
	indexQuota int64
	queryQuota int64

	overQuotaCh chan struct{}

	m        sync.Mutex
	waitCond *sync.Cond
	waiting  int

	indexes map[interface{}]sizeFunc

	// Tracks estimated memory used by running queries
	runningQueryUsed uint64

	// states for log deduplication
	memUsedPrev int64
	pimPrev     int64
	waitingPrev int64
	indexesPrev int64
}

func newAppHerder(memQuota uint64, appRatio, indexRatio,
	queryRatio float64, overQuotaCh chan struct{}) *appHerder {
	ah := &appHerder{
		memQuota:    int64(memQuota),
		overQuotaCh: overQuotaCh,
		indexes:     map[interface{}]sizeFunc{},
	}

	ah.appQuota = int64(float64(ah.memQuota) * appRatio)
	ah.indexQuota = int64(float64(ah.appQuota) * indexRatio)
	ah.queryQuota = int64(float64(ah.appQuota) * queryRatio)

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

	return ah
}

func (a *appHerder) Stats() map[string]interface{} {
	return map[string]interface{}{
		"TotWaitingIn":              atomic.LoadUint64(&cbft.TotHerderWaitingIn),
		"TotWaitingOut":             atomic.LoadUint64(&cbft.TotHerderWaitingOut),
		"TotOnBatchExecuteStartBeg": atomic.LoadUint64(&cbft.TotHerderOnBatchExecuteStartBeg),
		"TotOnBatchExecuteStartEnd": atomic.LoadUint64(&cbft.TotHerderOnBatchExecuteStartEnd),
		"TotQueriesRejected":        atomic.LoadUint64(&cbft.TotHerderQueriesRejected),
	}
}

// *** Indexing Callbacks

func (a *appHerder) onClose(c interface{}) {
	a.m.Lock()
	delete(a.indexes, c)
	a.awakeWaitersLOCKED("closing index")
	a.m.Unlock()
}

func (a *appHerder) onMemoryUsedDropped(curMemoryUsed, prevMemoryUsed uint64) {
	a.awakeWaiters("memory used dropped")
}

func (a *appHerder) awakeWaiters(msg string) {
	a.m.Lock()
	a.awakeWaitersLOCKED(msg)
	a.m.Unlock()
}

func (a *appHerder) awakeWaitersLOCKED(msg string) {
	if a.waiting > 0 {
		log.Printf("app_herder: %s, indexes: %d, waiting: %d", msg,
			len(a.indexes), a.waiting)

		a.waitCond.Broadcast()
	}
}

func (a *appHerder) onBatchExecuteStart(c interface{}, s sizeFunc) {
	// negative means ignore both appQuota and indexQuota and let the
	// incoming batch proceed.  A zero indexQuota means ignore the
	// indexQuota, but continue to check the appQuota for incoming
	// batches.
	if a.indexQuota < 0 {
		return
	}

	atomic.AddUint64(&cbft.TotHerderOnBatchExecuteStartBeg, 1)

	a.m.Lock()

	a.indexes[c] = s

	wasWaiting := false
	isOverQuota, preIndexingMemory, memUsed := a.overMemQuotaForIndexingLOCKED()

	for isOverQuota {
		wasWaiting = true

		atomic.AddUint64(&cbft.TotHerderWaitingIn, 1)
		a.waiting++

		// If we're over the memory quota, then wait for persister,
		// query or other progress.
		// log only if the values change from the last time.
		if a.memUsedPrev != memUsed || a.pimPrev != preIndexingMemory ||
			a.waitingPrev != int64(a.waiting) ||
			a.indexesPrev != int64(len(a.indexes)) {
			log.Printf("app_herder: indexing over indexQuota: %d, memUsed: %d,"+
				" preIndexingMemory: %d, indexes: %d, waiting: %d", a.indexQuota,
				memUsed, preIndexingMemory, len(a.indexes), a.waiting)

			// capture the previous logged values as we don't want to log them again.
			a.memUsedPrev = memUsed
			a.pimPrev = preIndexingMemory
			a.waitingPrev = int64(a.waiting)
			a.indexesPrev = int64(len(a.indexes))
		}

		if a.overQuotaCh != nil {
			a.overQuotaCh <- struct{}{}
		}

		a.waitCond.Wait()

		a.waiting--
		atomic.AddUint64(&cbft.TotHerderWaitingOut, 1)

		isOverQuota, preIndexingMemory, memUsed = a.overMemQuotaForIndexingLOCKED()
	}

	if wasWaiting {
		log.Printf("app_herder: indexing proceeding, indexes: %d, waiting: %d, usage: %v",
			len(a.indexes), a.waiting, cbft.FetchCurMemoryUsed())
	}

	a.m.Unlock()

	atomic.AddUint64(&cbft.TotHerderOnBatchExecuteStartEnd, 1)
}

func (a *appHerder) indexingMemoryLOCKED() (rv uint64) {
	for index, indexSizeFunc := range a.indexes {
		rv += indexSizeFunc(index)
	}

	return
}

func (a *appHerder) preIndexingMemoryLOCKED() (rv uint64) {
	// account for overhead from documents in batches
	rv += atomic.LoadUint64(&cbft.BatchBytesAdded) -
		atomic.LoadUint64(&cbft.BatchBytesRemoved)

	return
}

func (a *appHerder) overMemQuotaForIndexingLOCKED() (bool, int64, int64) {
	// MB-29504 workaround to try and prevent indexing from becoming completely
	// stuck.  The thinking is that if the indexing memUsed is 0, all data has
	// been flushed to disk, and we should allow it to proceed (even if we're
	// over quota in the bigger picture)
	if a.indexingMemoryLOCKED() == 0 {
		return false, 0, 0
	}

	// fetch memory used by process
	memUsed := int64(cbft.FetchCurMemoryUsed())

	// now account for the overhead from documents ready in batches
	// but not yet executed
	preIndexingMemory := int64(a.preIndexingMemoryLOCKED())
	memUsed += preIndexingMemory // TODO: NOTE: this is perhaps double-counting

	// make sure indexing doesn't exceed the index portion of the quota
	if a.indexQuota > 0 && memUsed > a.indexQuota {
		return true, preIndexingMemory, memUsed
	}

	return a.appQuota > 0 && memUsed > a.appQuota, preIndexingMemory, memUsed
}

func (a *appHerder) onPersisterProgress() {
	a.awakeWaiters("persister progress")
}

func (a *appHerder) onMergerProgress() {
	a.awakeWaiters("merger progress")
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
	// overMemQuotaForIndexingLOCKED) -- this workaround tries to
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

// *** Moss Wrapper

func (a *appHerder) MossHerderOnEvent() func(moss.Event) {
	return func(event moss.Event) { a.onMossEvent(event) }
}

func mossSize(c interface{}) uint64 {
	s, err := c.(moss.Collection).Stats()
	if err != nil {
		log.Warnf("app_herder: moss stats, err: %v", err)
		return 0
	}
	return s.CurDirtyBytes
}

func (a *appHerder) onMossEvent(event moss.Event) {
	if event.Collection.Options().LowerLevelUpdate == nil {
		return
	}
	switch event.Kind {
	case moss.EventKindClose:
		a.onClose(event.Collection)

	case moss.EventKindBatchExecuteStart:
		a.onBatchExecuteStart(event.Collection, mossSize)

	case moss.EventKindPersisterProgress:
		a.onPersisterProgress()

	default:
		return
	}
}

// *** Scorch Wrapper
func (a *appHerder) ScorchHerderOnEvent() func(scorch.Event) {
	return func(event scorch.Event) { a.onScorchEvent(event) }
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

func (a *appHerder) onScorchEvent(event scorch.Event) {
	switch event.Kind {
	case scorch.EventKindClose:
		a.onClose(event.Scorch)

	case scorch.EventKindBatchIntroductionStart:
		a.onBatchExecuteStart(event.Scorch, scorchSize)

	case scorch.EventKindPersisterProgress:
		a.onPersisterProgress()

	case scorch.EventKindMergerProgress:
		a.onMergerProgress()

	default:
		return
	}
}

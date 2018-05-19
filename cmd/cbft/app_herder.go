//  Copyright (c) 2018 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/index/scorch"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt/rest"
	"github.com/couchbase/moss"

	log "github.com/couchbase/clog"
)

type sizeFunc func(interface{}) uint64

type appHerder struct {
	memQuota   uint64
	appQuota   uint64
	indexQuota uint64
	queryQuota uint64

	m        sync.Mutex
	waitCond *sync.Cond
	waiting  int

	indexes map[interface{}]sizeFunc

	// Flag that is to be set to true for tracking memory used by queries
	queryHerdingEnabled bool
	// Tracks the amount of memory used by running queries
	runningQueryUsed uint64

	stats appHerderStats
}

type appHerderStats struct {
	TotWaitingIn  uint64
	TotWaitingOut uint64

	TotOnBatchExecuteStartBeg uint64
	TotOnBatchExecuteStartEnd uint64
}

func newAppHerder(memQuota uint64, appRatio, indexRatio,
	queryRatio float64) *appHerder {
	ah := &appHerder{
		memQuota: memQuota,
		indexes:  map[interface{}]sizeFunc{},
	}
	ah.appQuota = uint64(float64(ah.memQuota) * appRatio)
	ah.indexQuota = uint64(float64(ah.appQuota) * indexRatio)
	ah.queryQuota = uint64(float64(ah.appQuota) * queryRatio)
	ah.waitCond = sync.NewCond(&ah.m)
	log.Printf("app_herder: memQuota: %d, appQuota: %d, indexQuota: %d, "+
		"queryQuota: %d", memQuota, ah.appQuota, ah.indexQuota, ah.queryQuota)
	return ah
}

func (a *appHerder) Stats() map[string]interface{} {
	return map[string]interface{}{
		"TotWaitingIn":              atomic.LoadUint64(&a.stats.TotWaitingIn),
		"TotWaitingOut":             atomic.LoadUint64(&a.stats.TotWaitingOut),
		"TotOnBatchExecuteStartBeg": atomic.LoadUint64(&a.stats.TotOnBatchExecuteStartBeg),
		"TotOnBatchExecuteStartEnd": atomic.LoadUint64(&a.stats.TotOnBatchExecuteStartEnd),
	}
}

// *** Indexing Callbacks

func (a *appHerder) onClose(c interface{}) {
	a.m.Lock()

	if a.waiting > 0 {
		log.Printf("app_herder: close progress, waiting: %d", a.waiting)
	}

	delete(a.indexes, c)

	a.waitCond.Broadcast()

	a.m.Unlock()
}

func (a *appHerder) onBatchExecuteStart(c interface{}, s sizeFunc) {
	atomic.AddUint64(&a.stats.TotOnBatchExecuteStartBeg, 1)

	a.m.Lock()

	a.indexes[c] = s

	for a.overMemQuotaForIndexingLOCKED() {
		// If we're over the memory quota, then wait for persister progress.

		log.Printf("app_herder: waiting for more memory to be available")

		atomic.AddUint64(&a.stats.TotWaitingIn, 1)
		a.waiting++
		a.waitCond.Wait()
		a.waiting--
		atomic.AddUint64(&a.stats.TotWaitingOut, 1)

		log.Printf("app_herder: resuming upon memory reduction")
	}

	a.m.Unlock()

	atomic.AddUint64(&a.stats.TotOnBatchExecuteStartEnd, 1)
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

func (a *appHerder) overMemQuotaForIndexingLOCKED() bool {
	memUsed := a.indexingMemoryLOCKED()

	// MB-29504 workaround to try and prevent indexing from becoming completely
	// stuck.  The thinking is that if the indexing memUsed is 0, all data has
	// been flushed to disk, and we should allow it to proceed (even if we're
	// over quota in the bigger picture)
	// For the future this is incomplete since it also means that a query load
	// would no longer be able to completely block indexing, but since query
	// memory qouta is still disabled we can live with it for now.
	if memUsed == 0 {
		return false
	}

	// now account for the overhead from documents in batches
	memUsed += a.preIndexingMemoryLOCKED()

	// first make sure indexing (on it's own) doesn't exceed the
	// index portion of the quota
	if memUsed > a.indexQuota {
		log.Printf("app_herder: indexing mem used %d over indexing quota %d",
			memUsed, a.indexQuota)
		return true
	}

	// second add in running queries and check combined app quota
	memUsed += a.runningQueryUsed
	if memUsed > a.appQuota {
		log.Printf("app_herder: indexing mem plus query %d now over app quota %d",
			memUsed, a.appQuota)
	}
	return memUsed > a.appQuota
}

func (a *appHerder) onPersisterProgress() {
	a.m.Lock()

	if a.waiting > 0 {
		log.Printf("app_herder: persistence progress, waiting: %d", a.waiting)
	}

	a.waitCond.Broadcast()

	a.m.Unlock()
}

func (a *appHerder) onMergerProgress() {
	a.m.Lock()

	if a.waiting > 0 {
		log.Printf("app_herder: merging progress, waiting: %d", a.waiting)
	}

	a.waitCond.Broadcast()

	a.m.Unlock()
}

// *** Query Interface

func (a *appHerder) setQueryHerding(to bool) {
	a.queryHerdingEnabled = to
}

func (a *appHerder) queryHerderOnEvent() func(cbft.QueryEvent, uint64) error {
	return func(event cbft.QueryEvent, size uint64) error { return a.onQueryEvent(event, size) }
}

func (a *appHerder) onQueryEvent(event cbft.QueryEvent, size uint64) error {
	switch event.Kind {
	case cbft.EventQueryStart:
		return a.onQueryStart(size)

	case cbft.EventQueryEnd:
		return a.onQueryEnd(size)

	default:
		return nil
	}
}

func (a *appHerder) onQueryStart(size uint64) error {
	if !a.queryHerdingEnabled {
		return nil
	}

	a.m.Lock()
	defer a.m.Unlock()
	memUsed := a.runningQueryUsed + size

	// first make sure querying (on it's own) doesn't exceed the
	// query portion of the quota
	if memUsed > a.queryQuota {
		log.Printf("app_herder: this query %d plus running queries: %d "+
			"would exceed query quota: %d",
			size, a.runningQueryUsed, a.queryQuota)
		return rest.ErrorSearchReqRejected
	}

	// second add in indexing and check combined app quota
	indexingMem := a.indexingMemoryLOCKED()
	memUsed += indexingMem
	if memUsed > a.appQuota {
		log.Printf("app_herder: this query %d plus running queries: %d "+
			"plus indexing: %d would exceed app quota: %d",
			size, a.runningQueryUsed, indexingMem, a.appQuota)
		return rest.ErrorSearchReqRejected
	}

	// record the addition
	a.runningQueryUsed += size
	return nil
}

func (a *appHerder) onQueryEnd(size uint64) error {
	if !a.queryHerdingEnabled {
		return nil
	}

	a.m.Lock()
	a.runningQueryUsed -= size

	if a.waiting > 0 {
		log.Printf("app_herder: query ended, waiting: %d", a.waiting)
	}

	a.waitCond.Broadcast()

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

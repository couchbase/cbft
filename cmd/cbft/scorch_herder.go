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

	log "github.com/couchbase/clog"

	"github.com/blevesearch/bleve/index/scorch"
)

// ratio of memory quota above which backpressure kicks in.
var threshold = float64(0.40)

// A scorchHerder oversees multiple bleve scorch instances by
// pausing batch ingest amongst the herd of scorch once we've
// collectively reached a given, shared memory quota.
type scorchHerder struct {
	memQuota uint64

	m        sync.Mutex
	waitCond *sync.Cond
	waiting  int

	// the map tracks scorch indexes currently being herded
	indexes map[*scorch.Scorch]struct{}
}

// newScorchHerder returns a new scorch herder instance.
func newScorchHerder(memQuota uint64) *scorchHerder {
	sh := &scorchHerder{
		memQuota: memQuota,
		indexes:  map[*scorch.Scorch]struct{}{},
	}
	sh.waitCond = sync.NewCond(&sh.m)
	return sh
}

// NewScorchHerderOnEvent returns a func closure that can be used
// as a scorch OnEvent() callback.
func NewScorchHerderOnEvent(memQuota uint64) func(scorch.Event) {
	if memQuota <= 0 {
		return nil
	}

	log.Printf("scorch_herder: memQuota: %d", memQuota)

	sh := newScorchHerder(memQuota)

	return func(event scorch.Event) { sh.OnEvent(event) }
}

func (sh *scorchHerder) OnEvent(event scorch.Event) {
	switch event.Kind {
	case scorch.EventKindClose:
		sh.OnClose(event.Scorch)

	case scorch.EventKindBatchIntroductionStart:
		sh.OnBatchIntroductionStart(event.Scorch)

	case scorch.EventKindPersisterProgress:
		sh.OnPersisterProgress(event.Scorch)

	default:
		return
	}
}

func (sh *scorchHerder) OnClose(s *scorch.Scorch) {
	sh.m.Lock()

	if sh.waiting > 0 {
		log.Printf("scorch_herder: close start progress, waiting: %d", sh.waiting)
	}

	delete(sh.indexes, s)

	sh.m.Unlock()
}

func (sh *scorchHerder) OnBatchIntroductionStart(s *scorch.Scorch) {
	sh.m.Lock()

	sh.indexes[s] = struct{}{}

	for sh.overMemQuotaLOCKED() {
		// If we're over the memory quota, then wait for persister progress.

		log.Printf("scorch_herder: waiting for persister progress, as usage"+
			" over %v%% of memQuota (%v)", threshold*float64(100), sh.memQuota)

		sh.waiting++
		sh.waitCond.Wait()
		sh.waiting--

		log.Printf("scorch_herder: resuming upon persister progress ..")
	}

	sh.m.Unlock()
}

func (sh *scorchHerder) OnPersisterProgress(s *scorch.Scorch) {
	sh.m.Lock()

	if sh.waiting > 0 {
		log.Printf("scorch_herder: persister progress, waiting: %d", sh.waiting)
	}

	sh.waitCond.Broadcast()

	sh.m.Unlock()
}

// --------------------------------------------------------

// overMemQuotaLOCKED() returns true if the number of dirty bytes is
// greater than the memory quota.
func (sh *scorchHerder) overMemQuotaLOCKED() bool {
	var memoryUsed uint64

	for s := range sh.indexes {
		memoryUsed += s.MemoryUsed()
	}

	return memoryUsed > uint64(float64(sh.memQuota)*threshold)
}

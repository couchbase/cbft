//  Copyright (c) 2014 Couchbase, Inc.
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

	"github.com/couchbase/moss"
)

// TODO: The memory quota does not account for memory taken by moss
// snapshots, which might be consuming many resources.

// A mossHerder oversees multiple moss collection instances by pausing
// batch ingest amongst the herd of moss once we've collectively
// reached a given, shared memory quota.
type mossHerder struct {
	memQuota uint64

	m        sync.Mutex // Protects the fields that follow.
	waitCond *sync.Cond
	waiting  int

	// The map tracks moss collections currently being herded
	collections map[moss.Collection]struct{}
}

// newMossHerder returns a new moss herder instance.
func newMossHerder(memQuota uint64) *mossHerder {
	mh := &mossHerder{
		memQuota:    memQuota,
		collections: map[moss.Collection]struct{}{},
	}
	mh.waitCond = sync.NewCond(&mh.m)
	return mh
}

// NewMossHerderOnEvent returns a func closure that that can be used
// as a moss OnEvent() callback.
func NewMossHerderOnEvent(memQuota uint64) func(moss.Event) {
	if memQuota <= 0 {
		return nil
	}

	log.Printf("moss_herder: memQuota: %d", memQuota)

	mh := newMossHerder(memQuota)

	return func(event moss.Event) { mh.OnEvent(event) }
}

func (mh *mossHerder) OnEvent(event moss.Event) {
	switch event.Kind {
	case moss.EventKindCloseStart:
		mh.OnCloseStart(event.Collection)

	case moss.EventKindClose:
		mh.OnClose(event.Collection)

	case moss.EventKindBatchExecuteStart:
		mh.OnBatchExecuteStart(event.Collection)

	case moss.EventKindPersisterProgress:
		mh.OnPersisterProgress(event.Collection)

	default:
		return
	}
}

func (mh *mossHerder) OnCloseStart(c moss.Collection) {
	mh.m.Lock()

	if mh.waiting > 0 {
		log.Printf("moss_herder: close start progress, waiting: %d", mh.waiting)
	}

	delete(mh.collections, c)

	mh.m.Unlock()
}

func (mh *mossHerder) OnClose(c moss.Collection) {
	mh.m.Lock()

	if mh.waiting > 0 {
		log.Printf("moss_herder: close progress, waiting: %d", mh.waiting)
	}

	delete(mh.collections, c)

	mh.m.Unlock()
}

func (mh *mossHerder) OnBatchExecuteStart(c moss.Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	mh.collections[c] = struct{}{}

	for mh.overMemQuotaLOCKED() {
		// If we're over the memory quota, then wait for persister progress.

		log.Printf("moss_herder: waiting for persister progress, as usage"+
			" over memQuota (%v)", mh.memQuota)

		mh.waiting++
		mh.waitCond.Wait()
		mh.waiting--

		log.Printf("moss_herder: resuming upon persister progress ..")
	}

	mh.m.Unlock()
}

func (mh *mossHerder) OnPersisterProgress(c moss.Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	if mh.waiting > 0 {
		log.Printf("moss_herder: persistence progress, waiting: %d", mh.waiting)
	}

	mh.waitCond.Broadcast()

	mh.m.Unlock()
}

// --------------------------------------------------------

// overMemQuotaLOCKED() returns true if the number of dirty bytes is
// greater than the memory quota.
func (mh *mossHerder) overMemQuotaLOCKED() bool {
	var totDirtyBytes uint64

	for c := range mh.collections {
		s, err := c.Stats()
		if err != nil {
			log.Warnf("moss_herder: stats, err: %v", err)
			continue
		}

		totDirtyBytes += s.CurDirtyBytes
	}

	return totDirtyBytes > mh.memQuota
}

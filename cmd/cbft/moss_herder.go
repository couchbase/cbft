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

// A MossHerder oversees multiple moss collection instances by pausing
// batch ingest amongst the herd of moss once we've collectively
// reached a given, shared memory quota.
type MossHerder struct {
	memQuota uint64

	m        sync.Mutex // Protects the fields that follow.
	waitCond *sync.Cond
	waiting  int

	// The baseProgress increases whenever there are collection
	// persistence or collection close events.
	baseProgress uint64

	// The map value (uint64) is the last baseProgress seen by each
	// Collection's merger.
	collections map[moss.Collection]uint64
}

// NewMossHerder returns a new moss herder instance.
func NewMossHerder(memQuota uint64) *MossHerder {
	mh := &MossHerder{
		memQuota:    memQuota,
		collections: map[moss.Collection]uint64{},
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

	mh := NewMossHerder(memQuota)

	return func(event moss.Event) { mh.OnEvent(event) }
}

func (mh *MossHerder) OnEvent(event moss.Event) {
	switch event.Kind {
	case moss.EventKindCloseStart:
		mh.OnCloseStart(event.Collection)

	case moss.EventKindClose:
		mh.OnClose(event.Collection)

	case moss.EventKindMergerProgress:
		mh.OnMergerProgress(event.Collection)

	case moss.EventKindPersisterProgress:
		mh.OnPersisterProgress(event.Collection)

	default:
		return
	}
}

func (mh *MossHerder) OnCloseStart(c moss.Collection) {
	log.Printf("moss_herder: OnCloseStart, collection: %p", c)

	mh.m.Lock()

	if mh.waiting > 0 {
		log.Printf("moss_herder: close start progess, waiting: %d", mh.waiting)
	}

	mh.baseProgress++
	mh.waitCond.Broadcast()

	mh.m.Unlock()
}

func (mh *MossHerder) OnClose(c moss.Collection) {
	log.Printf("moss_herder: OnClose, collection: %p", c)

	mh.m.Lock()

	if mh.waiting > 0 {
		log.Printf("moss_herder: close progess, waiting: %d", mh.waiting)
	}

	delete(mh.collections, c)

	mh.baseProgress++
	mh.waitCond.Broadcast()

	mh.m.Unlock()
}

func (mh *MossHerder) OnMergerProgress(c moss.Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	baseProgressSeen := mh.collections[c]
	mh.collections[c] = mh.baseProgress

	if mh.overMemQuotaLOCKED() &&
		baseProgressSeen > 0 &&
		baseProgressSeen >= mh.baseProgress {
		// If we're over the memory quota, and we've seen all the
		// progress so far, then wait for more progress.
		mh.waiting++
		mh.waitCond.Wait()
		mh.waiting--
	}

	mh.m.Unlock()
}

func (mh *MossHerder) OnPersisterProgress(c moss.Collection) {
	if c.Options().LowerLevelUpdate == nil {
		return
	}

	mh.m.Lock()

	if mh.waiting > 0 {
		log.Printf("moss_herder: persistence progess, waiting: %d", mh.waiting)
	}

	mh.baseProgress++
	mh.waitCond.Broadcast()

	mh.m.Unlock()
}

// --------------------------------------------------------

// overMemQuotaLOCKED() returns true if the number of dirty bytes is
// greater than the memory quota.
func (mh *MossHerder) overMemQuotaLOCKED() bool {
	var totDirtyBytes uint64

	for c := range mh.collections {
		s, err := c.Stats()
		if err != nil {
			log.Printf("moss_herder: stats, err: %v", err)

			continue
		}

		totDirtyBytes += s.CurDirtyBytes
	}

	return totDirtyBytes > mh.memQuota
}

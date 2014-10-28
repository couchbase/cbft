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
	"os"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"
)

func (pindex *PIndex) Run(mgr PIndexManager) {
	close := true
	cleanup := true

	var err error = nil

	if pindex.IndexType == "bleve" {
		close, cleanup, err = RunBleveStream(mgr, pindex, pindex.Stream,
			pindex.Impl.(bleve.Index))
		if err != nil {
			log.Printf("error: RunBleveStream, close: %b, cleanup: %b, err: %v",
				close, cleanup, err)
		} else {
			log.Printf("done: RunBleveStream, close: %b, cleanup: %b",
				close, cleanup)
		}
	} else {
		log.Printf("error: PIndex.Run() saw unknown IndexType: %s", pindex.IndexType)
	}

	// NOTE: We expect the PIndexImpl to handle any inflight, concurrent
	// queries, access and Close() correctly with its own locking.
	if close {
		pindex.Impl.Close()
	}

	if cleanup {
		os.RemoveAll(pindex.Path)
	}
}

func RunBleveStream(mgr PIndexManager, pindex *PIndex, stream Stream,
	bindex bleve.Index) (bool, bool, error) {
	for m := range stream {
		// TODO: probably need things like stream reset/rollback
		// and snapshot kinds of ops here, too.

		// TODO: maybe need a more batchy API?  Perhaps, yet another
		// goroutine that clumps up up updates into bigger batches?

		switch m := m.(type) {
		case *StreamUpdate:
			bindex.Index(string(m.Id()), m.Body())

		case *StreamDelete:
			bindex.Delete(string(m.Id()))

		case *StreamEnd:
			// Perhaps the datasource exited or is restarting?  We'll
			// keep our stream open in case a new feed is hooked up.
			if m.doneCh != nil {
				close(m.doneCh)
			}

		case *StreamFlush:
			// TODO: Need to delete all records here.  So, why not
			// implement this the same as rollback to zero?
			if m.doneCh != nil {
				close(m.doneCh)
			}

		case *StreamRollback:
			// TODO: Implement partial rollback one day.
			//
			// For now, always rollback to zero, in which we close the
			// pindex and have the janitor rebuild from scratch.
			pindex.Impl.Close()
			os.RemoveAll(pindex.Path)
			mgr.ClosePIndex(pindex)

			if m.doneCh != nil {
				close(m.doneCh)
			}
			return false, false, nil

		case *StreamSnapshot:
			// TODO: Need to ACK some snapshot?
			if m.doneCh != nil {
				close(m.doneCh)
			}
		}
	}

	return true, true, nil
}

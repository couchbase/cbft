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
	var keepFiles bool = false
	var err error = nil

	if pindex.IndexType == "bleve" {
		keepFiles, err = RunBleveStream(mgr, pindex, pindex.Stream, pindex.Impl.(bleve.Index))
		if err != nil {
			log.Printf("error: RunBleveStream, keepFiles: %b, err: %v", keepFiles, err)
		} else {
			log.Printf("done: RunBleveStream, keepFiles: %b", keepFiles)
		}
	} else {
		log.Printf("error: PIndex.Run() saw unknown IndexType: %s", pindex.IndexType)
	}

	// NOTE: We expect the PIndexImpl to handle any inflight, concurrent
	// queries, access and Close() correctly with its own locking.
	pindex.Impl.Close()

	if !keepFiles {
		os.RemoveAll(pindex.Path)
	}
}

func RunBleveStream(mgr PIndexManager, pindex *PIndex, stream Stream,
	bindex bleve.Index) (bool, error) {
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
			// TODO: Need to delete all records here.
			if m.doneCh != nil {
				close(m.doneCh)
			}
		case *StreamRollback:
			// TODO: Need to actually roll back bleve here.
			if m.doneCh != nil {
				close(m.doneCh)
			}
			return true, nil
		case *StreamSnapshot:
			// TODO: Need to ACK some snapshot?
			if m.doneCh != nil {
				close(m.doneCh)
			}
		}
	}

	return false, nil
}

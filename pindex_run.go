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
			log.Printf("error: RunBleveStream, close: %t, cleanup: %t, err: %v",
				close, cleanup, err)
		} else {
			log.Printf("done: RunBleveStream, close: %t, cleanup: %t",
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
	for req := range stream {
		var err error

		// TODO: maybe need a more batchy API?  Perhaps, yet another
		// goroutine that clumps up up updates into bigger batches?

		switch req.Op {
		case STREAM_OP_NOOP:
			// Do nothing, so stream source can use NOOP like a ping.
			log.Printf("bleve stream noop, key: %s", string(req.Key))

		case STREAM_OP_UPDATE:
			log.Printf("bleve stream udpate, key: %s", string(req.Key))
			err = bindex.Index(string(req.Key), req.Val)

		case STREAM_OP_DELETE:
			log.Printf("bleve stream delete, key: %s", string(req.Key))
			err = bindex.Delete(string(req.Key))

		case STREAM_OP_FLUSH:
			// TODO: Need to delete all records here.  So, why not
			// implement this the same as rollback to zero?

		case STREAM_OP_ROLLBACK:
			// TODO: Implement partial rollback one day.
			//
			// For now, always rollback to zero, in which we close the
			// pindex and have the janitor rebuild from scratch.
			pindex.Impl.Close()
			os.RemoveAll(pindex.Path)

			// First, respond to the stream source (example: the feed)
			// so that it can unblock.
			if req.DoneCh != nil {
				close(req.DoneCh)
			}

			// Because, here the manager/janitor will synchronously
			// ask the feed to close and we don't want a deadlock.
			mgr.ClosePIndex(pindex)
			mgr.Kick("stream-rollback")

			return false, false, nil

		case STREAM_OP_GET_META:
			v, err := bindex.GetInternal(req.Key)
			if err != nil && req.Misc != nil {
				c, ok := req.Misc.(chan []byte)
				if ok && c != nil {
					c <- v
					close(c)
				}
			}

		case STREAM_OP_SET_META:
			err = bindex.SetInternal(req.Key, req.Val)
		}

		if err != nil {
			log.Printf("error: bleve stream, op: %s, req: %#v, err: %v",
				StreamOpNames[req.Op], req, err)
		}

		if req.DoneCh != nil {
			if err != nil {
				req.DoneCh <- err
			}
			close(req.DoneCh)
		}
	}

	return true, true, nil
}

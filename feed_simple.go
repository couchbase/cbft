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
	"fmt"
	log "github.com/couchbaselabs/clog"
)

// A SimpleFeed is uses a local, in-memory channel as its Stream
// datasource.  It's useful, amongst other things, for testing.
type SimpleFeed struct {
	name    string
	pf      StreamPartitionFunc
	streams map[string]Stream
	closeCh chan bool
	doneCh  chan bool
	doneErr error
	doneMsg string
	source  Stream
}

func NewSimpleFeed(name string, source Stream, pf StreamPartitionFunc,
	streams map[string]Stream) (*SimpleFeed, error) {
	return &SimpleFeed{
		name:    name,
		pf:      pf,
		streams: streams,
		closeCh: make(chan bool),
		doneCh:  make(chan bool),
		doneErr: nil,
		doneMsg: "",
		source:  source,
	}, nil
}

func (t *SimpleFeed) Name() string {
	return t.name
}

func (t *SimpleFeed) Start() error {
	log.Printf("SimpleFeed.Start, name: %s, streams: %#v", t.Name(), t.streams)
	go t.feed()
	return nil
}

func (t *SimpleFeed) feed() {
	for {
		select {
		case <-t.closeCh:
			t.doneErr = nil
			t.doneMsg = "closeCh closed"
			close(t.doneCh)
			return

		case req, alive := <-t.source:
			if !alive {
				t.waitForClose("source closed", nil)
				return
			}

			stream, err := t.pf(req.Key, req.Partition, t.streams)
			if err != nil {
				err = fmt.Errorf("error: SimpleFeed pf on req: %#v, err: %v",
					req, err)
				log.Printf("%v", err)
				t.waitForClose("partition func error", err)
				return
			}

			doneChOrig := req.DoneCh
			doneCh := make(chan error)
			req.DoneCh = doneCh

			stream <- req
			err = <-doneCh

			req.DoneCh = doneChOrig
			if req.DoneCh != nil {
				if err != nil {
					req.DoneCh <- err
				}
				close(req.DoneCh)
			}

			if req.Op == STREAM_OP_ROLLBACK {
				t.waitForClose("stream rollback", nil)
				return
			}
		}
	}
}

func (t *SimpleFeed) waitForClose(msg string, err error) {
	<-t.closeCh
	t.doneErr = err
	t.doneMsg = msg
	close(t.doneCh)
}

func (t *SimpleFeed) Close() error {
	select {
	case <-t.doneCh:
		return t.doneErr
	default:
	}

	close(t.closeCh)
	<-t.doneCh
	return t.doneErr
}

func (t *SimpleFeed) Streams() map[string]Stream {
	return t.streams
}

func (t *SimpleFeed) Source() Stream {
	return t.source
}

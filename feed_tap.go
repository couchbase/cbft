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
	"strconv"

	"github.com/couchbase/gomemcached/client"
	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

type TAPFeed struct {
	name       string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	pf         StreamPartitionFunc
	streams    map[string]Stream
	closeCh    chan bool
	doneCh     chan bool
	doneErr    error
	doneMsg    string
}

func NewTAPFeed(name, url, poolName, bucketName, bucketUUID string,
	pf StreamPartitionFunc, streams map[string]Stream) (*TAPFeed, error) {
	return &TAPFeed{
		name:       name,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		pf:         pf,
		streams:    streams,
		closeCh:    make(chan bool),
		doneCh:     make(chan bool),
		doneErr:    nil,
		doneMsg:    "",
	}, nil
}

func (t *TAPFeed) Name() string {
	return t.name
}

func (t *TAPFeed) Start() error {
	log.Printf("TAPFeed.Start, name: %s", t.Name())

	go ExponentialBackoffLoop(t.Name(),
		func() int {
			progress, err := t.feed()
			if err != nil {
				log.Printf("TAPFeed name: %s, progress: %d, err: %v",
					t.Name(), progress, err)
			}
			return progress
		},
		FEED_SLEEP_INIT_MS,  // Milliseconds.
		FEED_BACKOFF_FACTOR, // Backoff.
		FEED_SLEEP_MAX_MS)

	return nil
}

func (t *TAPFeed) feed() (int, error) {
	select {
	case <-t.closeCh:
		t.doneErr = nil
		t.doneMsg = "closeCh closed"
		close(t.doneCh)
		return -1, nil
	default:
	}

	bucket, err := couchbase.GetBucket(t.url, t.poolName, t.bucketName)
	if err != nil {
		return 0, err
	}
	defer bucket.Close()

	if t.bucketUUID != "" && t.bucketUUID != bucket.UUID {
		bucket.Close()
		return -1, fmt.Errorf("error: mismatched bucket uuid,"+
			"bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
			t.bucketName, t.bucketUUID, bucket.UUID)
	}

	// TODO: This is (incorrectly) assuming hash/vbucket partitioning,
	// and looks like we're missing a level of parameterization of
	// partitioning: source partitioning (like vbuckets) is different
	// than stream partitioning.
	vbuckets := []uint16{}
	for partition, _ := range t.streams {
		if partition != "" {
			vbId, err := strconv.Atoi(partition)
			if err != nil {
				return -1, fmt.Errorf("error: could not parse partition: %s, err: %v",
					partition, err)
			}
			vbuckets = append(vbuckets, uint16(vbId))
		}
	}

	args := memcached.TapArguments{}
	if len(vbuckets) > 0 {
		args.VBuckets = vbuckets
	}

	feed, err := bucket.StartTapFeed(&args)
	if err != nil {
		return 0, err
	}
	defer feed.Close()

	for {
		select {
		case <-t.closeCh:
			t.doneErr = nil
			t.doneMsg = "closeCh closed"
			close(t.doneCh)
			return -1, nil

		case req, alive := <-feed.C:
			if !alive {
				t.waitForClose("source closed", nil)
				break
			}

			partition := fmt.Sprintf("%d", req.VBucket)
			stream, err := t.pf(req.Key, partition, t.streams)
			if err != nil {
				t.waitForClose("partition func error",
					fmt.Errorf("error: TAPFeed pf on req: %#v, err: %v",
						req, err))
				return 1, err
			}

			if req.Opcode == memcached.TapMutation {
				stream <- &StreamRequest{
					Op:  STREAM_OP_UPDATE,
					Key: req.Key,
					Val: req.Value,
				}
			} else if req.Opcode == memcached.TapDeletion {
				stream <- &StreamRequest{
					Op:  STREAM_OP_DELETE,
					Key: req.Key,
				}
			}
		}
	}

	return 1, nil
}

func (t *TAPFeed) waitForClose(msg string, err error) {
	<-t.closeCh
	t.doneErr = err
	t.doneMsg = msg
	close(t.doneCh)
}

func (t *TAPFeed) Close() error {
	close(t.closeCh)
	<-t.doneCh
	return t.doneErr
}

func (t *TAPFeed) Streams() map[string]Stream {
	return t.streams
}

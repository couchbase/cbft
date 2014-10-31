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

	args := memcached.TapArguments{}

	vbuckets, err := ParsePartitionsToVBucketIds(t.streams)
	if err != nil {
		return -1, err
	}
	if len(vbuckets) > 0 {
		args.VBuckets = vbuckets
	}

	feed, err := bucket.StartTapFeed(&args)
	if err != nil {
		return 0, err
	}
	defer feed.Close()

	// TODO: maybe TAPFeed should do a rollback to zero if it finds it
	// needs to do a full backfill.
	// TODO: this TAPFeed implementation currently only works against
	// a couchbase cluster that has just a single node.

	log.Printf("TapFeed: running, url: %s,"+
		" poolName: %s, bucketName: %s, vbuckets: %#v",
		t.url, t.poolName, t.bucketName, vbuckets)

loop:
	for {
		select {
		case <-t.closeCh:
			t.doneErr = nil
			t.doneMsg = "closeCh closed"
			close(t.doneCh)
			return -1, nil

		case req, alive := <-feed.C:
			if !alive {
				break loop
			}

			log.Printf("TapFeed: received from url: %s,"+
				" poolName: %s, bucketName: %s, opcode: %s, req: %#v",
				t.url, t.poolName, t.bucketName, req.Opcode, req)

			partition := fmt.Sprintf("%d", req.VBucket)
			stream, err := t.pf(req.Key, partition, t.streams)
			if err != nil {
				return 1, fmt.Errorf("error: TAPFeed:"+
					" partition func error from url: %s,"+
					" poolName: %s, bucketName: %s, req: %#v, streams: %#v, err: %v",
					t.url, t.poolName, t.bucketName, req, t.streams, err)
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

func (t *TAPFeed) Close() error {
	select {
	case <-t.doneCh:
		return t.doneErr
	default:
	}

	close(t.closeCh)
	<-t.doneCh
	return t.doneErr
}

func (t *TAPFeed) Streams() map[string]Stream {
	return t.streams
}

func ParsePartitionsToVBucketIds(streams map[string]Stream) ([]uint16, error) {
	vbuckets := make([]uint16, 0, len(streams))
	for partition, _ := range streams {
		if partition != "" {
			vbId, err := strconv.Atoi(partition)
			if err != nil {
				return nil, fmt.Errorf("error: could not parse partition: %s, err: %v",
					partition, err)
			}
			vbuckets = append(vbuckets, uint16(vbId))
		}
	}
	return vbuckets, nil
}

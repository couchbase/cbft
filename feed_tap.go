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
	streams    map[string]Stream
	closeCh    chan bool
	doneCh     chan bool
}

func NewTAPFeed(name, url, poolName, bucketName, bucketUUID string,
	streams map[string]Stream) (*TAPFeed, error) {
	return &TAPFeed{
		name:       name,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		streams:    streams,
		closeCh:    make(chan bool),
		doneCh:     make(chan bool),
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
		return -1, fmt.Errorf("mismatched bucket uuid,"+
			"bucketName: %s, bucketUUID: %s, bucket.UUID: %s",
			t.bucketName, t.bucketUUID, bucket.UUID)
	}

	args := memcached.TapArguments{}
	feed, err := bucket.StartTapFeed(&args)
	if err != nil {
		return 0, err
	}
	defer feed.Close()

	for {
		select {
		case <-t.closeCh:
			close(t.doneCh)
			return -1, nil

		case op, ok := <-feed.C:
			if !ok {
				break
			}

			if op.Opcode == memcached.TapMutation {
				// TODO: Handle dispatch to streams correctly.
				t.streams[""] <- &StreamUpdate{
					Id:   op.Key,
					Body: op.Value,
				}
			} else if op.Opcode == memcached.TapDeletion {
				// TODO: Handle dispatch to streams correctly.
				t.streams[""] <- &StreamDelete{
					Id: op.Key,
				}
			}
		}
	}

	return 1, nil
}

func (t *TAPFeed) Close() error {
	close(t.closeCh)
	<-t.doneCh
	return nil
}

func (t *TAPFeed) Streams() map[string]Stream {
	return t.streams
}

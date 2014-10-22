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
	bucket     *couchbase.Bucket
	feed       *couchbase.TapFeed
	streams    map[string]Stream
	closeCh    chan bool
}

func NewTAPFeed(name, url, poolName, bucketName, bucketUUID string,
	streams map[string]Stream) (*TAPFeed, error) {
	// TODO: All this error checking should move into channel loop
	// so that it handles reconnects and retries correctly.
	bucket, err := couchbase.GetBucket(url, poolName, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketUUID != "" && bucketUUID != bucket.UUID {
		bucket.Close()
		return nil, fmt.Errorf("mismatched bucket uuid, bucketName: %s", bucketName)
	}

	rv := TAPFeed{
		name:       name,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: "",     // bucket.UUID skipped for now as we're ahead of rest of code
		bucket:     bucket, // TODO: need to close bucket on cleanup.
		streams:    streams,
		closeCh:    make(chan bool),
	}

	log.Printf("NewTapFeed, name: %s", rv.Name())

	return &rv, nil
}

func (t *TAPFeed) Name() string {
	return t.name
}

func (t *TAPFeed) Start() error {
	log.Printf("TapFeed.Start, name: %s", t.Name())

	args := memcached.TapArguments{}
	feed, err := t.bucket.StartTapFeed(&args)
	if err != nil {
		return err
	}
	t.feed = feed
	go func() {
		for {
			select {
			case <-t.closeCh:
				break
			case op, ok := <-feed.C:
				if !ok {
					break
				}
				if op.Opcode == memcached.TapMutation {
					t.streams[""] <- &StreamUpdate{
						id:   op.Key,
						body: op.Value,
					}
				} else if op.Opcode == memcached.TapDeletion {
					t.streams[""] <- &StreamDelete{
						id: op.Key,
					}
				}
			}
		}
	}()
	return nil
}

func (t *TAPFeed) Close() error {
	if err := t.feed.Close(); err != nil {
		return err
	}
	close(t.closeCh)
	t.bucket.Close()
	return nil
}

func (t *TAPFeed) Streams() map[string]Stream {
	return t.streams
}

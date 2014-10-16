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
	"github.com/couchbaselabs/go-couchbase"
)

type TAPFeed struct {
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	bucket     *couchbase.Bucket
	feed       *couchbase.TapFeed
	requests   StreamRequests // TODO: may need to fan-out to multiple StreamRequests.
}

func NewTAPFeed(url, poolName, bucketName, bucketUUID string) (*TAPFeed, error) {
	bucket, err := couchbase.GetBucket(url, poolName, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketUUID != "" && bucketUUID != bucket.UUID {
		bucket.Close()
		return nil, fmt.Errorf("mismatched bucket uuid, bucketName: %s", bucketName)
	}
	rv := TAPFeed{
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucket.UUID,
		bucket:     bucket, // TODO: need to close bucket on cleanup.
		requests:   make(StreamRequests),
	}
	return &rv, nil
}

func (t *TAPFeed) Start() error {
	args := memcached.DefaultTapArguments()
	feed, err := t.bucket.StartTapFeed(&args)
	if err != nil {
		return err
	}
	t.feed = feed
	go func() {
		defer close(t.requests) // TODO: figure out close responsibility.
		for op := range feed.C {
			if op.Opcode == memcached.TapMutation {
				t.requests <- &StreamUpdate{
					id:   op.Key,
					body: op.Value,
				}
			} else if op.Opcode == memcached.TapDeletion {
				t.requests <- &StreamDelete{
					id: op.Key,
				}
			}
		}
	}()
	return nil
}

func (t *TAPFeed) Close() error {
	return t.feed.Close()
}

func (t *TAPFeed) Channel() StreamRequests {
	return t.requests
}

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
	"github.com/couchbase/gomemcached/client"
	"github.com/couchbaselabs/go-couchbase"
)

type TAPStream struct {
	bucket    *couchbase.Bucket
	feed      *couchbase.TapFeed
	mutations StreamMutations
}

func NewTAPStream(url, bucketName string) (*TAPStream, error) {
	bucket, err := couchbase.GetBucket(url, "default", bucketName)
	if err != nil {
		return nil, err
	}
	rv := TAPStream{
		bucket:    bucket,
		mutations: make(StreamMutations),
	}
	return &rv, nil
}

func (t *TAPStream) Start() error {
	args := memcached.DefaultTapArguments()
	feed, err := t.bucket.StartTapFeed(&args)
	if err != nil {
		return err
	}
	t.feed = feed
	go func() {
		defer close(t.mutations)
		for op := range feed.C {
			if op.Opcode == memcached.TapMutation {
				t.mutations <- &StreamUpdate{
					id:   op.Key,
					body: op.Value,
				}
			} else if op.Opcode == memcached.TapDeletion {
				t.mutations <- &StreamDelete{
					id: op.Key,
				}
			}
		}
	}()
	return nil
}

func (t *TAPStream) Close() error {
	return t.feed.Close()
}

func (t *TAPStream) Channel() StreamMutations {
	return t.mutations
}

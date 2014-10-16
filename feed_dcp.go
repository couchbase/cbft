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

	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/go-couchbase"
)

type DCPStream struct {
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	bucket     *couchbase.Bucket
	feed       *couchbase.UprFeed
	requests   StreamRequests // TODO: may need to fan-out to multiple StreamRequests
}

func NewDCPStream(url, poolName, bucketName, bucketUUID string) (*DCPStream, error) {
	bucket, err := couchbase.GetBucket(url, poolName, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketUUID != "" && bucketUUID != bucket.UUID {
		bucket.Close()
		return nil, fmt.Errorf("mismatched bucket uuid, bucketName: %s", bucketName)
	}
	rv := DCPStream{
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucket.UUID,
		bucket:     bucket, // TODO: need to close bucket on cleanup.
		requests:   make(StreamRequests),
	}
	return &rv, nil
}

func (t *DCPStream) Start() error {
	// start upr feed
	feed, err := t.bucket.StartUprFeed("index" /*name*/, 0)
	if err != nil {
		return err
	}
	err = feed.UprRequestStream(
		uint16(0),          /*vbno*/
		uint32(0),          /*opaque*/
		0,                  /*flag*/
		0,                  /*vbuuid*/
		0,                  /*seqStart*/
		0xFFFFFFFFFFFFFFFF, /*seqEnd*/
		0,                  /*snaps*/
		0)
	if err != nil {
		return err
	}
	t.feed = feed
	go func() {
		defer close(t.requests) // TODO: figure out close responsibility.
		for uprEvent := range feed.C {
			if uprEvent.Opcode == gomemcached.UPR_MUTATION {
				t.requests <- &StreamUpdate{
					id:   uprEvent.Key,
					body: uprEvent.Value,
				}
			} else if uprEvent.Opcode == gomemcached.UPR_DELETION {
				t.requests <- &StreamDelete{
					id: uprEvent.Key,
				}
			}
		}
	}()
	return nil
}

func (t *DCPStream) Close() error {
	return t.feed.Close()
}

func (t *DCPStream) Channel() StreamRequests {
	return t.requests
}

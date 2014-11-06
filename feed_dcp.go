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
	"sync"

	"github.com/couchbase/gomemcached"
	log "github.com/couchbaselabs/clog"

	"github.com/steveyen/cbdatasource"
)

type DCPMutation struct {
	delete    bool
	vbucketId uint16
	key       []byte
	seq       uint64
}

// Implements both Feed and cbdatasource.Receiver interfaces.
type DCPFeed struct {
	name       string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	pf         StreamPartitionFunc
	streams    map[string]Stream
	bds        cbdatasource.BucketDataSource

	m      sync.Mutex
	closed bool

	errs []error
	muts []*DCPMutation
	meta map[uint16][]byte

	numSnapshotStarts int
	numSetMetaDatas   int
	numGetMetaDatas   int
	numRollbacks      int
}

func NewDCPFeed(name, url, poolName, bucketName, bucketUUID string,
	pf StreamPartitionFunc, streams map[string]Stream) (*DCPFeed, error) {
	vbucketIds, err := ParsePartitionsToVBucketIds(streams)
	if err != nil {
		return nil, err
	}
	if len(vbucketIds) <= 0 {
		vbucketIds = nil
	}

	var authFunc cbdatasource.AuthFunc
	var options *cbdatasource.BucketDataSourceOptions

	feed := &DCPFeed{
		name:       name,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		pf:         pf,
		streams:    streams,
	}

	feed.bds, err = cbdatasource.NewBucketDataSource([]string{url},
		poolName, bucketName, bucketUUID,
		vbucketIds, authFunc, feed, options)
	if err != nil {
		return nil, err
	}

	return feed, nil
}

func (t *DCPFeed) Name() string {
	return t.name
}

func (t *DCPFeed) Start() error {
	log.Printf("DCPFeed.Start, name: %s", t.Name())
	return t.bds.Start()
}

func (t *DCPFeed) Close() error {
	t.m.Lock()
	if t.closed {
		t.m.Unlock()
		return fmt.Errorf("already closed")
	}
	t.closed = true
	t.m.Unlock()

	log.Printf("DCPFeed.Close, name: %s", t.Name())
	return t.bds.Close()
}

func (t *DCPFeed) Streams() map[string]Stream {
	return t.streams
}

// --------------------------------------------------------

func (r *DCPFeed) OnError(err error) {
	r.m.Lock()
	defer r.m.Unlock()

	log.Printf("DCPFeed.OnError: %s: %v\n", r.name, err)
	r.errs = append(r.errs, err)
}

func (r *DCPFeed) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	log.Printf("DCPFeed.DataUpdate: %s: vbucketId: %d, key: %s, seq: %d, req: %v\n",
		r.name, vbucketId, key, seq, req)

	r.m.Lock()
	defer r.m.Unlock()

	r.muts = append(r.muts, &DCPMutation{
		delete:    false,
		vbucketId: vbucketId,
		key:       key,
		seq:       seq,
	})
	return nil
}

func (r *DCPFeed) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	log.Printf("DCPFeed.DataDelete: %s: vbucketId: %d, key: %s, seq: %d, req: %#v",
		r.name, vbucketId, key, seq, req)

	r.m.Lock()
	defer r.m.Unlock()

	r.muts = append(r.muts, &DCPMutation{
		delete:    true,
		vbucketId: vbucketId,
		key:       key,
		seq:       seq,
	})
	return nil
}

func (r *DCPFeed) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	log.Printf("DCPFeed.SnapshotStart: %s: vbucketId: %d,"+
		" snapStart: %d, snapEnd: %d, snapType: %d",
		r.name, vbucketId, snapStart, snapEnd, snapType)

	r.numSnapshotStarts += 1
	return nil
}

func (r *DCPFeed) SetMetaData(vbucketId uint16, value []byte) error {
	r.m.Lock()
	defer r.m.Unlock()

	r.numSetMetaDatas += 1
	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value
	return nil
}

func (r *DCPFeed) GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error) {
	r.m.Lock()
	defer r.m.Unlock()

	r.numGetMetaDatas += 1
	rv := []byte(nil)
	if r.meta != nil {
		rv = r.meta[vbucketId]
	}
	for i := len(r.muts) - 1; i >= 0; i = i - 1 {
		if r.muts[i].vbucketId == vbucketId {
			return rv, r.muts[i].seq, nil
		}
	}
	return rv, 0, nil
}

func (r *DCPFeed) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	r.numRollbacks += 1
	return fmt.Errorf("bad-rollback")
}

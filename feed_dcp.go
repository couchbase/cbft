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

	m       sync.Mutex
	closed  bool
	lastErr error

	seqs map[uint16]uint64 // To track max seq #'s we received per vbucketId.

	numError         uint64
	numUpdate        uint64
	numDelete        uint64
	numSnapshotStart uint64
	numSetMetaData   uint64
	numGetMetaData   uint64
	numRollback      uint64
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
	log.Printf("DCPFeed.OnError: %s: %v\n", r.name, err)

	r.m.Lock()
	defer r.m.Unlock()
	r.numError += 1

	r.lastErr = err
}

func (r *DCPFeed) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	// log.Printf("DCPFeed.DataUpdate: %s: vbucketId: %d, key: %s, seq: %d, req: %v\n",
	// r.name, vbucketId, key, seq, req)

	partition, stream, err :=
		VBucketIdToPartitionStream(r.pf, r.streams, vbucketId, key)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numUpdate += 1
	r.updateSeqUnlocked(vbucketId, seq)
	r.m.Unlock()

	stream <- &StreamRequest{
		Op:        STREAM_OP_UPDATE,
		Partition: partition,
		SeqNo:     seq,
		Key:       req.Key,
		Val:       req.Body,
	}

	return nil
}

func (r *DCPFeed) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	// log.Printf("DCPFeed.DataDelete: %s: vbucketId: %d, key: %s, seq: %d, req: %#v",
	// r.name, vbucketId, key, seq, req)

	partition, stream, err :=
		VBucketIdToPartitionStream(r.pf, r.streams, vbucketId, key)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numDelete += 1
	r.updateSeqUnlocked(vbucketId, seq)
	r.m.Unlock()

	stream <- &StreamRequest{
		Op:        STREAM_OP_DELETE,
		Partition: partition,
		SeqNo:     seq,
		Key:       req.Key,
	}

	return nil
}

func (r *DCPFeed) updateSeqUnlocked(vbucketId uint16, seq uint64) {
	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if r.seqs[vbucketId] < seq {
		r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().
	}
}

func (r *DCPFeed) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	log.Printf("DCPFeed.SnapshotStart: %s: vbucketId: %d,"+
		" snapStart: %d, snapEnd: %d, snapType: %d",
		r.name, vbucketId, snapStart, snapEnd, snapType)

	r.m.Lock()
	defer r.m.Unlock()
	r.numSnapshotStart += 1

	return nil
}

func (r *DCPFeed) SetMetaData(vbucketId uint16, value []byte) error {
	log.Printf("DCPFeed.SetMetaData: %s: vbucketId: %d,"+
		" value: %s", r.name, vbucketId, value)

	partition, stream, err :=
		VBucketIdToPartitionStream(r.pf, r.streams, vbucketId, nil)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numSetMetaData += 1
	r.m.Unlock()

	stream <- &StreamRequest{
		Op:        STREAM_OP_SET_META,
		Partition: partition,
		Key:       []byte(partition),
		Val:       value,
	}

	return nil
}

func (r *DCPFeed) GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error) {
	log.Printf("DCPFeed.GetMetaData: %s: vbucketId: %d", r.name, vbucketId)

	partition, stream, err :=
		VBucketIdToPartitionStream(r.pf, r.streams, vbucketId, nil)
	if err != nil {
		return nil, 0, err
	}

	r.m.Lock()
	r.numGetMetaData += 1
	if r.seqs != nil {
		// TODO: Need to get this from stream instead of here.
		lastSeq = r.seqs[vbucketId]
	}
	r.m.Unlock()

	rvCh := make(chan []byte)
	stream <- &StreamRequest{
		Op:        STREAM_OP_GET_META,
		Partition: partition,
		Key:       []byte(partition),
		Misc:      rvCh,
	}
	rv, exists := <-rvCh
	if !exists {
		return nil, 0, nil
	}

	log.Printf("DCPFeed.GetMetaData: %s: vbucketId: %d, rv: %s", r.name, vbucketId, rv)

	return rv, lastSeq, nil
}

func (r *DCPFeed) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	log.Printf("DCPFeed.Rollback: %s: vbucketId: %d,"+
		" rollbackSeq: %d", r.name, vbucketId, rollbackSeq)

	r.m.Lock()
	defer r.m.Unlock()
	r.numRollback += 1

	return fmt.Errorf("bad-rollback")
}

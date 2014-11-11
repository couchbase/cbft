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
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
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
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	bds        cbdatasource.BucketDataSource

	m       sync.Mutex
	closed  bool
	lastErr error

	numError         uint64
	numUpdate        uint64
	numDelete        uint64
	numSnapshotStart uint64
	numSetMetaData   uint64
	numGetMetaData   uint64
	numRollback      uint64
}

type DCPFeedParams struct {
	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a cluster manager node.
	ClusterManagerBackoffFactor float32

	// Initial sleep time (millisecs) before first retry to cluster manager.
	ClusterManagerSleepInitMS int

	// Maximum sleep time (millisecs) between retries to cluster manager.
	ClusterManagerSleepMaxMS int

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a data manager node.
	DataManagerBackoffFactor float32

	// Initial sleep time (millisecs) before first retry to data manager.
	DataManagerSleepInitMS int

	// Maximum sleep time (millisecs) between retries to data manager.
	DataManagerSleepMaxMS int

	// Buffer size in bytes provided for UPR flow control.
	FeedBufferSizeBytes uint32

	// Used for UPR flow control and buffer-ack messages when this
	// percentage of FeedBufferSizeBytes is reached.
	FeedBufferAckThreshold float32
}

func NewDCPFeed(name, url, poolName, bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest) (*DCPFeed, error) {
	params := &DCPFeedParams{}
	if paramsStr != "" {
		err := json.Unmarshal([]byte(paramsStr), params)
		if err != nil {
			return nil, err
		}
	}

	vbucketIds, err := ParsePartitionsToVBucketIds(dests)
	if err != nil {
		return nil, err
	}
	if len(vbucketIds) <= 0 {
		vbucketIds = nil
	}

	var authFunc cbdatasource.AuthFunc // TODO: AUTH.

	options := &cbdatasource.BucketDataSourceOptions{
		Name: fmt.Sprintf("%s-%x", name, rand.Int31()),
		ClusterManagerBackoffFactor: params.ClusterManagerBackoffFactor,
		ClusterManagerSleepInitMS:   params.ClusterManagerSleepInitMS,
		ClusterManagerSleepMaxMS:    params.ClusterManagerSleepMaxMS,
		DataManagerBackoffFactor:    params.DataManagerBackoffFactor,
		DataManagerSleepInitMS:      params.DataManagerSleepInitMS,
		DataManagerSleepMaxMS:       params.DataManagerSleepMaxMS,
		FeedBufferSizeBytes:         params.FeedBufferSizeBytes,
		FeedBufferAckThreshold:      params.FeedBufferAckThreshold,
	}

	feed := &DCPFeed{
		name:       name,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		params:     params,
		pf:         pf,
		dests:      dests,
	}

	feed.bds, err = cbdatasource.NewBucketDataSource(
		strings.Split(url, ";"),
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
		return nil
	}
	t.closed = true
	t.m.Unlock()

	log.Printf("DCPFeed.Close, name: %s", t.Name())
	return t.bds.Close()
}

func (t *DCPFeed) Dests() map[string]Dest {
	return t.dests
}

// --------------------------------------------------------

func (r *DCPFeed) OnError(err error) {
	log.Printf("DCPFeed.OnError: %s: %v\n", r.name, err)

	r.m.Lock()
	r.numError += 1
	r.lastErr = err
	r.m.Unlock()
}

func (r *DCPFeed) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	// log.Printf("DCPFeed.DataUpdate: %s: vbucketId: %d, key: %s, seq: %d, req: %v\n",
	// r.name, vbucketId, key, seq, req)

	partition, dest, err :=
		VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, key)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numUpdate += 1
	r.m.Unlock()

	return dest.OnDataUpdate(partition, key, seq, req.Body)
}

func (r *DCPFeed) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	// log.Printf("DCPFeed.DataDelete: %s: vbucketId: %d, key: %s, seq: %d, req: %#v",
	// r.name, vbucketId, key, seq, req)

	partition, dest, err :=
		VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, key)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numDelete += 1
	r.m.Unlock()

	return dest.OnDataDelete(partition, key, seq)
}

func (r *DCPFeed) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	log.Printf("DCPFeed.SnapshotStart: %s: vbucketId: %d,"+
		" snapStart: %d, snapEnd: %d, snapType: %d",
		r.name, vbucketId, snapStart, snapEnd, snapType)

	partition, dest, err :=
		VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numSnapshotStart += 1
	r.m.Unlock()

	return dest.OnSnapshotStart(partition, snapStart, snapEnd)
}

func (r *DCPFeed) SetMetaData(vbucketId uint16, value []byte) error {
	log.Printf("DCPFeed.SetMetaData: %s: vbucketId: %d,"+
		" value: %s", r.name, vbucketId, value)

	partition, dest, err :=
		VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numSetMetaData += 1
	r.m.Unlock()

	return dest.SetOpaque(partition, value)
}

func (r *DCPFeed) GetMetaData(vbucketId uint16) (value []byte, lastSeq uint64, err error) {
	log.Printf("DCPFeed.GetMetaData: %s: vbucketId: %d", r.name, vbucketId)

	partition, dest, err :=
		VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
	if err != nil {
		return nil, 0, err
	}

	r.m.Lock()
	r.numGetMetaData += 1
	r.m.Unlock()

	return dest.GetOpaque(partition)
}

func (r *DCPFeed) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	log.Printf("DCPFeed.Rollback: %s: vbucketId: %d,"+
		" rollbackSeq: %d", r.name, vbucketId, rollbackSeq)

	partition, dest, err :=
		VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
	if err != nil {
		return err
	}

	r.m.Lock()
	r.numRollback += 1
	r.m.Unlock()

	return dest.Rollback(partition, rollbackSeq)
}

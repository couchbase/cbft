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
	"strconv"

	"github.com/couchbase/gomemcached/client"
	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

func init() {
	RegisterFeedType("couchbase-tap", StartTAPFeed)
}

func StartTAPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, bucketName, bucketUUID, params string, dests map[string]Dest) error {
	feed, err := NewTAPFeed(feedName, mgr.server, "default",
		bucketName, bucketUUID, params, BasicPartitionFunc, dests)
	if err != nil {
		return fmt.Errorf("error: could not prepare TAP stream to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	if err = feed.Start(); err != nil {
		return fmt.Errorf("error: could not start tap feed, server: %s, err: %v",
			mgr.server, err)
	}
	if err = mgr.registerFeed(feed); err != nil {
		feed.Close()
		return err
	}
	return nil
}

// A TAPFeed uses TAP protocol to dump data from a couchbase data source.
type TAPFeed struct {
	name       string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	params     *TAPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	closeCh    chan bool
	doneCh     chan bool
	doneErr    error
	doneMsg    string
}

type TAPFeedParams struct {
	BackoffFactor float32
	SleepInitMS   int
	SleepMaxMS    int
}

func NewTAPFeed(name, url, poolName, bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest) (*TAPFeed, error) {
	params := &TAPFeedParams{}
	if paramsStr != "" {
		err := json.Unmarshal([]byte(paramsStr), params)
		if err != nil {
			return nil, err
		}
	}

	return &TAPFeed{
		name:       name,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		params:     params,
		pf:         pf,
		dests:      dests,
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

	backoffFactor := t.params.BackoffFactor
	if backoffFactor <= 0.0 {
		backoffFactor = FEED_BACKOFF_FACTOR
	}
	sleepInitMS := t.params.SleepInitMS
	if sleepInitMS <= 0 {
		sleepInitMS = FEED_SLEEP_INIT_MS
	}
	sleepMaxMS := t.params.SleepMaxMS
	if sleepMaxMS <= 0 {
		sleepMaxMS = FEED_SLEEP_MAX_MS
	}

	go ExponentialBackoffLoop(t.Name(),
		func() int {
			progress, err := t.feed()
			if err != nil {
				log.Printf("TAPFeed name: %s, progress: %d, err: %v",
					t.Name(), progress, err)
			}
			return progress
		},
		sleepInitMS, backoffFactor, sleepMaxMS)

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

	vbuckets, err := ParsePartitionsToVBucketIds(t.dests)
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

			partition, dest, err :=
				VBucketIdToPartitionDest(t.pf, t.dests, req.VBucket, req.Key)
			if err != nil {
				return 1, err
			}

			if req.Opcode == memcached.TapMutation {
				err = dest.OnDataUpdate(partition, req.Key, 0, req.Value)
			} else if req.Opcode == memcached.TapDeletion {
				err = dest.OnDataDelete(partition, req.Key, 0)
			}
			if err != nil {
				return 1, err
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

func (t *TAPFeed) Dests() map[string]Dest {
	return t.dests
}

func ParsePartitionsToVBucketIds(dests map[string]Dest) ([]uint16, error) {
	vbuckets := make([]uint16, 0, len(dests))
	for partition, _ := range dests {
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

func VBucketIdToPartitionDest(pf DestPartitionFunc,
	dests map[string]Dest, vbucketId uint16, key []byte) (
	partition string, dest Dest, err error) {
	if vbucketId < uint16(len(vbucketIdStrings)) {
		partition = vbucketIdStrings[vbucketId]
	}
	if partition == "" {
		partition = fmt.Sprintf("%d", vbucketId)
	}
	dest, err = pf(partition, key, dests)
	if err != nil {
		return "", nil, fmt.Errorf("error: VBucketIdToPartitionDest,"+
			" partition func, vbucketId: %d, err: %v", vbucketId, err)
	}
	return partition, dest, err
}

var vbucketIdStrings []string

func init() {
	vbucketIdStrings = make([]string, 1024)
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
	}
}

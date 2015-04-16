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

package cbft

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"

	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
)

func init() {
	RegisterFeedType("couchbase", &FeedType{
		Start:      StartDCPFeed,
		Partitions: CouchbasePartitions,
		Public:     true,
		Description: "general/couchbase" +
			" - Couchbase Server data source",
		StartSample: NewDCPFeedParams(),
	})
	RegisterFeedType("couchbase-dcp", &FeedType{
		Start:      StartDCPFeed,
		Partitions: CouchbasePartitions,
		Public:     false, // Won't be listed in /api/managerMeta output.
		Description: "general/couchbase-dcp" +
			" - Couchbase Server data source, via DCP protocol",
		StartSample: NewDCPFeedParams(),
	})
}

func StartDCPFeed(mgr *Manager, feedName, indexName, indexUUID,
	sourceType, bucketName, bucketUUID, params string,
	dests map[string]Dest) error {
	feed, err := NewDCPFeed(feedName, indexName, mgr.server, "default",
		bucketName, bucketUUID, params, BasicPartitionFunc, dests,
		mgr.tagsMap != nil && !mgr.tagsMap["feed"])
	if err != nil {
		return fmt.Errorf("feed_dcp: could not prepare DCP feed to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	err = feed.Start()
	if err != nil {
		return fmt.Errorf("feed_dcp: could not start, server: %s, err: %v",
			mgr.server, err)
	}
	err = mgr.registerFeed(feed)
	if err != nil {
		feed.Close()
		return err
	}
	return nil
}

// A DCPFeed implements both Feed and cbdatasource.Receiver interfaces.
type DCPFeed struct {
	name       string
	indexName  string
	url        string
	poolName   string
	bucketName string
	bucketUUID string
	params     *DCPFeedParams
	pf         DestPartitionFunc
	dests      map[string]Dest
	disable    bool
	bds        cbdatasource.BucketDataSource

	m       sync.Mutex
	closed  bool
	lastErr error
	stats   *DestStats
}

type DCPFeedParams struct {
	AuthUser     string `json:"authUser"` // May be "" for no auth.
	AuthPassword string `json:"authPassword"`

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a cluster manager node.
	ClusterManagerBackoffFactor float32 `json:"clusterManagerBackoffFactor"`

	// Initial sleep time (millisecs) before first retry to cluster manager.
	ClusterManagerSleepInitMS int `json:"clusterManagerSleepInitMS"`

	// Maximum sleep time (millisecs) between retries to cluster manager.
	ClusterManagerSleepMaxMS int `json:"clusterManagerSleepMaxMS"`

	// Factor (like 1.5) to increase sleep time between retries
	// in connecting to a data manager node.
	DataManagerBackoffFactor float32 `json:"dataManagerBackoffFactor"`

	// Initial sleep time (millisecs) before first retry to data manager.
	DataManagerSleepInitMS int `json:"dataManagerSleepInitMS"`

	// Maximum sleep time (millisecs) between retries to data manager.
	DataManagerSleepMaxMS int `json:"dataManagerSleepMaxMS"`

	// Buffer size in bytes provided for UPR flow control.
	FeedBufferSizeBytes uint32 `json:"feedBufferSizeBytes"`

	// Used for UPR flow control and buffer-ack messages when this
	// percentage of FeedBufferSizeBytes is reached.
	FeedBufferAckThreshold float32 `json:"feedBufferAckThreshold"`
}

func NewDCPFeedParams() *DCPFeedParams {
	return &DCPFeedParams{
		ClusterManagerSleepMaxMS: 20000,
		DataManagerSleepMaxMS:    20000,
	}
}

func (d *DCPFeedParams) GetCredentials() (string, string, string) {
	// TODO: bucketName not necessarily userName.
	return d.AuthUser, d.AuthPassword, d.AuthUser
}

func NewDCPFeed(name, indexName, url, poolName,
	bucketName, bucketUUID, paramsStr string,
	pf DestPartitionFunc, dests map[string]Dest,
	disable bool) (*DCPFeed, error) {
	params := NewDCPFeedParams()
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

	var auth couchbase.AuthHandler
	if params.AuthUser != "" {
		auth = params
	}

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
		indexName:  indexName,
		url:        url,
		poolName:   poolName,
		bucketName: bucketName,
		bucketUUID: bucketUUID,
		params:     params,
		pf:         pf,
		dests:      dests,
		disable:    disable,
		stats:      NewDestStats(),
	}

	feed.bds, err = cbdatasource.NewBucketDataSource(
		strings.Split(url, ";"),
		poolName, bucketName, bucketUUID,
		vbucketIds, auth, feed, options)
	if err != nil {
		return nil, err
	}

	return feed, nil
}

func (t *DCPFeed) Name() string {
	return t.name
}

func (t *DCPFeed) IndexName() string {
	return t.indexName
}

func (t *DCPFeed) Start() error {
	if t.disable {
		log.Printf("feed_dcp: disable, name: %s", t.Name())
		return nil
	}

	log.Printf("feed_dcp: start, name: %s", t.Name())
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

	log.Printf("feed_dcp: close, name: %s", t.Name())
	return t.bds.Close()
}

func (t *DCPFeed) Dests() map[string]Dest {
	return t.dests
}

var prefixBucketDataSourceStats = []byte(`{"bucketDataSourceStats":`)
var prefixDestStats = []byte(`,"destStats":`)

func (t *DCPFeed) Stats(w io.Writer) error {
	bdss := cbdatasource.BucketDataSourceStats{}
	err := t.bds.Stats(&bdss)
	if err != nil {
		return err
	}
	w.Write(prefixBucketDataSourceStats)
	json.NewEncoder(w).Encode(&bdss)

	w.Write(prefixDestStats)
	t.stats.WriteJSON(w)

	_, err = w.Write(jsonCloseBrace)
	return err
}

// --------------------------------------------------------

func (r *DCPFeed) OnError(err error) {
	// TODO: Check the type of the error if it's something
	// serious / not-recoverable / needs user attention.
	log.Printf("feed_dcp: OnError, name: %s:"+
		" bucketName: %s, bucketUUID: %s, err: %v\n",
		r.name, r.bucketName, r.bucketUUID, err)

	atomic.AddUint64(&r.stats.TotError, 1)

	r.m.Lock()
	r.lastErr = err
	r.m.Unlock()
}

func (r *DCPFeed) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, key)
		if err != nil {
			return err
		}

		err = dest.OnDataUpdate(partition, key, seq, req.Body)
		if err != nil {
			return fmt.Errorf("feed_dcp: DataUpdate,"+
				" name: %s, partition: %s, key: %s, seq: %d, err: %v",
				r.name, partition, key, seq, err)
		}
		return nil
	}, r.stats.TimerOnDataUpdate)
}

func (r *DCPFeed) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, key)
		if err != nil {
			return err
		}

		err = dest.OnDataDelete(partition, key, seq)
		if err != nil {
			return fmt.Errorf("feed_dcp: DataDelete,"+
				" name: %s, partition: %s, key: %s, seq: %d, err: %v",
				r.name, partition, key, seq, err)
		}
		return nil
	}, r.stats.TimerOnDataDelete)
}

func (r *DCPFeed) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil {
			return err
		}

		return dest.OnSnapshotStart(partition, snapStart, snapEnd)
	}, r.stats.TimerOnSnapshotStart)
}

func (r *DCPFeed) SetMetaData(vbucketId uint16, value []byte) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil {
			return err
		}

		return dest.SetOpaque(partition, value)
	}, r.stats.TimerSetOpaque)
}

func (r *DCPFeed) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	err = Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil {
			return err
		}

		value, lastSeq, err = dest.GetOpaque(partition)

		return err
	}, r.stats.TimerGetOpaque)

	return value, lastSeq, err
}

func (r *DCPFeed) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	return Timer(func() error {
		partition, dest, err :=
			VBucketIdToPartitionDest(r.pf, r.dests, vbucketId, nil)
		if err != nil {
			return err
		}

		opaqueValue, lastSeq, err := dest.GetOpaque(partition)
		if err != nil {
			return err
		}

		log.Printf("feed_dcp: rollback, name: %s: vbucketId: %d,"+
			" rollbackSeq: %d, partition: %s, opaqueValue: %s, lastSeq: %d",
			r.name, vbucketId, rollbackSeq,
			partition, opaqueValue, lastSeq)

		return dest.Rollback(partition, rollbackSeq)
	}, r.stats.TimerRollback)
}

// -------------------------------------------------------

type vbucketMetaData struct {
	FailOverLog [][]uint64 `json:"failOverLog"`
}

func parseOpaqueToUUID(b []byte) string {
	vmd := &vbucketMetaData{}
	err := json.Unmarshal(b, &vmd)
	if err != nil {
		return ""
	}
	flogLen := len(vmd.FailOverLog)
	if flogLen < 1 || len(vmd.FailOverLog[flogLen-1]) < 1 {
		return ""
	}
	return fmt.Sprintf("%d", vmd.FailOverLog[flogLen-1][0])
}

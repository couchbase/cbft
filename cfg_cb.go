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
	"sync"

	"github.com/couchbase/gomemcached"
	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/go-couchbase/cbdatasource"
)

// CfgCB is an implementation of Cfg that uses a couchbase bucket,
// and uses DCP to get change notifications.
//
// TODO: This current implementation is race-y!  Instead of storing
// everything as a single uber key/value, we should instead be storing
// individual key/value's on every get/set/del operation.
type CfgCB struct {
	m      sync.Mutex
	url    string
	bucket string
	b      *couchbase.Bucket
	cfgMem *CfgMem

	bds  cbdatasource.BucketDataSource
	bdsm sync.Mutex
	seqs map[uint16]uint64 // To track max seq #'s we received per vbucketId.
	meta map[uint16][]byte // To track metadata blob's per vbucketId.
}

var cfgCBOptions = &cbdatasource.BucketDataSourceOptions{
	// TODO: Make these parametrized.
	ClusterManagerSleepMaxMS: 20000,
	DataManagerSleepMaxMS:    20000,
}

func NewCfgCB(url, bucket string) (*CfgCB, error) {
	c := &CfgCB{
		url:    url,
		bucket: bucket,
		cfgMem: NewCfgMem(),
	}

	_, err := c.getBucket()
	if err != nil {
		return nil, err
	}

	bds, err := cbdatasource.NewBucketDataSource(
		[]string{url},
		"default", bucket, "", nil, c, c, cfgCBOptions)
	if err != nil {
		return nil, err
	}
	c.bds = bds

	err = bds.Start()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *CfgCB) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Get(key, cas)
}

func (c *CfgCB) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	cas, err := c.cfgMem.Set(key, val, cas)
	if err != nil {
		return 0, err
	}

	err = c.unlockedSave()
	if err != nil {
		return 0, err
	}
	return cas, err
}

func (c *CfgCB) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	err := c.cfgMem.Del(key, cas)
	if err != nil {
		return err
	}
	return c.unlockedSave()
}

func (c *CfgCB) Load() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.unlockedLoad()
}

func (c *CfgCB) getBucket() (*couchbase.Bucket, error) {
	if c.b == nil {
		b, err := couchbase.GetBucket(c.url, "default", c.bucket)
		if err != nil {
			return nil, err
		}
		c.b = b
	}
	return c.b, nil
}

func (c *CfgCB) unlockedLoad() error {
	bucket, err := c.getBucket()
	if err != nil {
		return err
	}

	buf, err := bucket.GetRaw("cfg")
	if err != nil && !gomemcached.IsNotFound(err) {
		return err
	}

	cfgMem := NewCfgMem()
	if buf != nil {
		err = json.Unmarshal(buf, cfgMem)
		if err != nil {
			return err
		}
	}

	cfgMemPrev := c.cfgMem
	cfgMemPrev.m.Lock()
	defer cfgMemPrev.m.Unlock()

	cfgMem.subscriptions = cfgMemPrev.subscriptions

	c.cfgMem = cfgMem

	return nil
}

func (c *CfgCB) unlockedSave() error {
	bucket, err := c.getBucket()
	if err != nil {
		return err
	}

	return bucket.Set("cfg", 0, c.cfgMem)
}

func (c *CfgCB) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgCB) Refresh() error {
	c.m.Lock()
	defer c.m.Unlock()

	c2, err := NewCfgCB(c.url, c.bucket)
	if err != nil {
		return err
	}

	err = c2.Load()
	if err != nil {
		return err
	}

	c.cfgMem.CASNext = c2.cfgMem.CASNext
	c.cfgMem.Entries = c2.cfgMem.Entries

	return c.cfgMem.Refresh()
}

// ----------------------------------------------------------------

func (a *CfgCB) GetCredentials() (string, string, string) {
	return a.bucket, "", a.bucket
}

// ----------------------------------------------------------------

func (r *CfgCB) OnError(err error) {
	log.Printf("cfg_cb: OnError, err: %v", err)
}

func (r *CfgCB) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if string(key) == "cfg" {
		go func() {
			r.Load()
			r.cfgMem.Refresh()
		}()
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *CfgCB) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if string(key) == "cfg" {
		go func() {
			r.Load()
			r.cfgMem.Refresh()
		}()
	}
	r.updateSeq(vbucketId, seq)
	return nil
}

func (r *CfgCB) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	return nil
}

func (r *CfgCB) SetMetaData(vbucketId uint16, value []byte) error {
	r.bdsm.Lock()
	defer r.bdsm.Unlock()

	if r.meta == nil {
		r.meta = make(map[uint16][]byte)
	}
	r.meta[vbucketId] = value

	return nil
}

func (r *CfgCB) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	r.bdsm.Lock()
	defer r.bdsm.Unlock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

	return value, lastSeq, nil
}

func (r *CfgCB) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	err := r.Refresh()
	if err != nil {
		return err
	}

	r.bdsm.Lock()
	r.seqs = nil
	r.meta = nil
	r.bdsm.Unlock()

	return nil
}

// ----------------------------------------------------------------

func (r *CfgCB) updateSeq(vbucketId uint16, seq uint64) {
	r.bdsm.Lock()
	defer r.bdsm.Unlock()

	if r.seqs == nil {
		r.seqs = make(map[uint16]uint64)
	}
	if r.seqs[vbucketId] < seq {
		r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().
	}
}

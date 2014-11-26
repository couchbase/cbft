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
	"sync"

	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/go-couchbase"
)

// Implementation of Cfg that uses a couchbase bucket.

type CfgCB struct {
	m      sync.Mutex
	url    string
	bucket string
	b      *couchbase.Bucket
	cfgMem *CfgMem
}

func NewCfgCB(url, bucket string) *CfgCB {
	return &CfgCB{
		url:    url,
		bucket: bucket,
		cfgMem: NewCfgMem(),
	}
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
		if err = json.Unmarshal(buf, cfgMem); err != nil {
			return err
		}
	}

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

	c2 := NewCfgCB(c.url, c.bucket)
	err := c2.Load()
	if err != nil {
		return err
	}

	c.cfgMem.CASNext = c2.cfgMem.CASNext
	c.cfgMem.Entries = c2.cfgMem.Entries

	return c.cfgMem.Refresh()
}

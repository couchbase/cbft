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
	"io/ioutil"
	"sync"
)

// Local-only, persisted (in a single file) implementation of Cfg.

type CfgSimple struct {
	m      sync.Mutex
	path   string
	cfgMem *CfgMem
}

func NewCfgSimple(path string) *CfgSimple {
	return &CfgSimple{
		path:   path,
		cfgMem: NewCfgMem(),
	}
}

func (c *CfgSimple) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Get(key, cas)
}

func (c *CfgSimple) Set(key string, val []byte, cas uint64) (
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

func (c *CfgSimple) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	err := c.cfgMem.Del(key, cas)
	if err != nil {
		return err
	}
	return c.unlockedSave()
}

func (c *CfgSimple) Load() error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.unlockedLoad()
}

func (c *CfgSimple) unlockedLoad() error {
	buf, err := ioutil.ReadFile(c.path)
	if err != nil {
		return err
	}

	cfgMem := NewCfgMem()
	if err := json.Unmarshal(buf, cfgMem); err != nil {
		return err
	}

	c.cfgMem = cfgMem
	return nil
}

func (c *CfgSimple) unlockedSave() error {
	buf, err := json.Marshal(c.cfgMem)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(c.path, buf, 0600)
}

func (c *CfgSimple) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	return c.cfgMem.Subscribe(key, ch)
}

func (c *CfgSimple) Refresh() error {
	c.m.Lock()
	defer c.m.Unlock()

	c2 := NewCfgSimple(c.path)
	err := c2.Load()
	if err != nil {
		return err
	}

	c.cfgMem.CASNext = c2.cfgMem.CASNext
	c.cfgMem.Entries = c2.cfgMem.Entries

	return c.cfgMem.Refresh()
}

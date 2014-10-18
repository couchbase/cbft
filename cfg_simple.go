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
)

// Non-distributed, local-only, memory only implementation of Cfg
// interface, useful for development and testing.

type CfgSimple struct {
	m       sync.Mutex
	casNext uint64
	entries map[string]*CfgSimpleEntry
}

type CfgSimpleEntry struct {
	cas uint64
	val string
}

func NewCfgSimple() Cfg {
	return &CfgSimple{
		casNext: 1,
		entries: make(map[string]*CfgSimpleEntry),
	}
}

func (c *CfgSimple) Get(key string, cas uint64) (
	string, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	entry, exists := c.entries[key]
	if !exists {
		return "", 0, nil
	}
	if cas != 0 && cas != entry.cas {
		return "", 0, fmt.Errorf("error: mismatched Cfg CAS")
	}
	return entry.val, entry.cas, nil
}

func (c *CfgSimple) Set(key string, val string, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if cas != 0 {
		entry, exists := c.entries[key]
		if !exists || cas != entry.cas {
			return 0, fmt.Errorf("error: mismatched Cfg CAS")
		}
	}
	entry := &CfgSimpleEntry{
		cas: c.casNext,
		val: val,
	}
	c.entries[key] = entry
	c.casNext += 1
	return entry.cas, nil
}

func (c *CfgSimple) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	if cas != 0 {
		entry, exists := c.entries[key]
		if !exists || cas != entry.cas {
			return fmt.Errorf("error: mismatched Cfg CAS")
		}
	}
	delete(c.entries, key)
	return nil
}

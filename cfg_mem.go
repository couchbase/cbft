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

// A local-only, memory-only implementation of Cfg interface, useful
// for development and testing.

type CfgMem struct {
	m       sync.Mutex
	casNext uint64
	entries map[string]*CfgMemEntry
}

type CfgMemEntry struct {
	cas uint64
	val []byte
}

func NewCfgMem() Cfg {
	return &CfgMem{
		casNext: 1,
		entries: make(map[string]*CfgMemEntry),
	}
}

func (c *CfgMem) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	entry, exists := c.entries[key]
	if !exists {
		return nil, 0, nil
	}
	if cas != 0 && cas != entry.cas {
		return nil, 0, fmt.Errorf("error: mismatched Cfg CAS")
	}
	val := make([]byte, len(entry.val))
	copy(val, entry.val)
	return val, entry.cas, nil
}

func (c *CfgMem) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	prevEntry, exists := c.entries[key]
	if cas == 0 {
		if exists {
			return 0, fmt.Errorf("error: entry already exists, key: %s", key)
		}
	} else { // cas != 0
		if !exists {
			return 0, fmt.Errorf("error: no entry, key: %s", key)
		}
		if cas != prevEntry.cas {
			return 0, fmt.Errorf("error: mismatched cas, key: %s", key)
		}
	}
	nextEntry := &CfgMemEntry{
		cas: c.casNext,
		val: make([]byte, len(val)),
	}
	copy(nextEntry.val, val)
	c.entries[key] = nextEntry
	c.casNext += 1
	return nextEntry.cas, nil
}

func (c *CfgMem) Del(key string, cas uint64) error {
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

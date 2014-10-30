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
	m             sync.Mutex
	CASNext       uint64
	Entries       map[string]*CfgMemEntry
	subscriptions map[string][]chan<- CfgEvent
}

type CfgMemEntry struct {
	CAS uint64
	Val []byte
}

func NewCfgMem() *CfgMem {
	return &CfgMem{
		CASNext:       1,
		Entries:       make(map[string]*CfgMemEntry),
		subscriptions: make(map[string][]chan<- CfgEvent),
	}
}

func (c *CfgMem) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	entry, exists := c.Entries[key]
	if !exists {
		return nil, 0, nil
	}
	if cas != 0 && cas != entry.CAS {
		return nil, 0, &CfgCASError{}
	}
	val := make([]byte, len(entry.Val))
	copy(val, entry.Val)
	return val, entry.CAS, nil
}

func (c *CfgMem) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	c.m.Lock()
	defer c.m.Unlock()

	prevEntry, exists := c.Entries[key]
	if cas == 0 {
		if exists {
			return 0, fmt.Errorf("error: entry already exists, key: %s", key)
		}
	} else { // cas != 0
		if !exists || cas != prevEntry.CAS {
			return 0, &CfgCASError{}
		}
	}
	nextEntry := &CfgMemEntry{
		CAS: c.CASNext,
		Val: make([]byte, len(val)),
	}
	copy(nextEntry.Val, val)
	c.Entries[key] = nextEntry
	c.CASNext += 1
	c.fireEvent(key, nextEntry.CAS)
	return nextEntry.CAS, nil
}

func (c *CfgMem) Del(key string, cas uint64) error {
	c.m.Lock()
	defer c.m.Unlock()

	if cas != 0 {
		entry, exists := c.Entries[key]
		if !exists || cas != entry.CAS {
			return &CfgCASError{}
		}
	}
	delete(c.Entries, key)
	c.fireEvent(key, 0)
	return nil
}

func (c *CfgMem) Subscribe(key string, ch chan CfgEvent) error {
	c.m.Lock()
	defer c.m.Unlock()

	a, exists := c.subscriptions[key]
	if !exists || a == nil {
		a = make([]chan<- CfgEvent, 0)
	}
	c.subscriptions[key] = append(a, ch)
	return nil
}

func (c *CfgMem) fireEvent(key string, cas uint64) {
	for _, c := range c.subscriptions[key] {
		go func(c chan<- CfgEvent) { c <- CfgEvent{Key: key, CAS: cas} }(c)
	}
}

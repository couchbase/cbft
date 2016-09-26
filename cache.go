//  Copyright (c) 2016 Couchbase, Inc.
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
	"strconv"
	"sync"
)

// TODO: Have a cache config based on total memory bytes used?
// TODO: Have a cache config based on cost to create an entry?

// Default max number of entries in a result cache.
var RESULT_CACHE_DEFAULT_MAX_LEN = 2000

// Default number of lookups needed before we start caching a result.
// The idea is avoid using resources for rarely encountered lookups.
var RESULT_CACHE_DEFAULT_MIN_LOOKUPS = 3

// Default maximum size in bytes of an entry result.  Results larger
// than this are not cached.
var RESULT_CACHE_DEFAULT_MAX_BYTES_PER_ENTRY = 32 * 1024

// Global result cache per cbft process.
var ResultCache = resultCache{
	cache:            map[string]*resultCacheEntry{},
	maxLen:           RESULT_CACHE_DEFAULT_MAX_LEN,
	minLookups:       RESULT_CACHE_DEFAULT_MIN_LOOKUPS,
	maxBytesPerEntry: RESULT_CACHE_DEFAULT_MAX_BYTES_PER_ENTRY,
}

// InitResultCacheOptions initializes the global result cache.
func InitResultCacheOptions(options map[string]string) error {
	if options["resultCacheMaxLen"] != "" {
		x, err := strconv.Atoi(options["resultCacheMaxLen"])
		if err != nil {
			return err
		}
		ResultCache.maxLen = x
	}

	if options["resultCacheMinLookups"] != "" {
		x, err := strconv.Atoi(options["resultCacheMinLookups"])
		if err != nil {
			return err
		}
		ResultCache.minLookups = x
	}

	if options["resultCacheMaxBytesPerEntry"] != "" {
		x, err := strconv.Atoi(options["resultCacheMaxBytesPerEntry"])
		if err != nil {
			return err
		}
		ResultCache.maxBytesPerEntry = x
	}

	return nil
}

// A resultCache implements a simple LRU cache.
type resultCache struct {
	m sync.Mutex // Protects the fields that follow.

	// The len(cache) == length of LRU list.
	cache map[string]*resultCacheEntry

	head *resultCacheEntry // The most recently used entry.
	tail *resultCacheEntry // The least recently used entry.

	maxLen           int // Keep len(cache) <= maxLen.
	minLookups       int // Minimum lookups needed before we cache an entry.
	maxBytesPerEntry int // Max size of a entry result in bytes.
}

// A resultCacheEntry represents a cached result, which is an entry in
// the LRU doubly-linked list.
type resultCacheEntry struct {
	key string // The key for this cache entry.

	totLookups uint64
	totHits    uint64

	result     []byte // May be nil, which is still useful for totLookups tracking.
	resultRev  uint64 // Used to track when a result has been obsoleted.
	resultCost uint64 // How expensive it was to produce this result.

	next *resultCacheEntry // For LRU list.
	prev *resultCacheEntry // For LRU list.
}

// ---------------------------------------------------------------

func (c *resultCache) enabled() bool {
	c.m.Lock()
	rv := c.maxLen > 0
	c.m.Unlock()
	return rv
}

// lookup returns nil or a previously cached result.  The cached
// result will have a rev greater or equal to the rev param.
func (c *resultCache) lookup(key string, rev uint64) ([]byte, error) {
	c.m.Lock()

	e := c.cache[key]
	if e == nil {
		e = &resultCacheEntry{key: key}
		c.cache[key] = e
	}

	c.touchLOCKED(e)

	e.totLookups++

	result := e.result
	if result != nil && e.resultRev >= rev {
		e.totHits++
	} else {
		result = nil
	}

	c.trimLOCKED(rev)

	c.m.Unlock()

	return result, nil
}

// encache adds an entry to the cache, but only if there have been
// enough recent lookups for that key (e.g., minLookups).
func (c *resultCache) encache(key string,
	resultFunc func() []byte, resultRev uint64, resultCost uint64) {
	c.m.Lock()

	e := c.cache[key]
	if e != nil &&
		e.totLookups >= uint64(c.minLookups) &&
		e.resultRev <= resultRev {
		c.m.Unlock()

		// Final result prep (such as marshalling) outside of lock.
		result := resultFunc()

		c.m.Lock()

		if e.totLookups >= uint64(c.minLookups) &&
			e.resultRev <= resultRev &&
			len(result) <= c.maxBytesPerEntry {
			e.result = result
			e.resultRev = resultRev
			e.resultCost = resultCost
		}
	}

	c.m.Unlock()
}

// touchLOCKED moves the cache entry to the head of the LRU list.
func (c *resultCache) touchLOCKED(e *resultCacheEntry) {
	c.delinkLOCKED(e)

	if c.head != nil {
		c.head.prev = e
		e.next = c.head
	}
	c.head = e
	if c.tail == nil {
		c.tail = e
	}
}

// delinkLOCKED splices the cache entry out of the LRU list.
func (c *resultCache) delinkLOCKED(e *resultCacheEntry) {
	if c.head == e {
		c.head = e.next
	}
	if c.tail == e {
		c.tail = e.prev
	}

	if e.next != nil {
		e.next.prev = e.prev
	}
	if e.prev != nil {
		e.prev.next = e.next
	}

	e.next = nil
	e.prev = nil
}

// trimLOCKED removes entries from the cache when it's too big.
func (c *resultCache) trimLOCKED(rev uint64) {
	// TODO: Consider better eviction approach than simple LRU.
	for len(c.cache) > c.maxLen && c.tail != nil {
		delete(c.cache, c.tail.key)
		c.delinkLOCKED(c.tail)
	}
}

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
	"testing"
)

func rcCopy(dst, src *resultCache) {
	dst.cache = src.cache
	dst.head = src.head
	dst.tail = src.tail
	dst.maxLen = src.maxLen
	dst.minLookups = src.minLookups
	dst.maxBytesPerEntry = src.maxBytesPerEntry
}

func TestInitResultCacheOptions(t *testing.T) {
	var rcOrig resultCache
	rcCopy(&rcOrig, &ResultCache)
	defer func() {
		rcCopy(&ResultCache, &rcOrig)
	}()

	err := InitResultCacheOptions(map[string]string{
		"resultCacheMaxLen": "not-a-number",
	})
	if err == nil {
		t.Errorf("expected parse err")
	}

	err = InitResultCacheOptions(map[string]string{
		"resultCacheMinLookups": "not-a-number",
	})
	if err == nil {
		t.Errorf("expected parse err")
	}

	err = InitResultCacheOptions(map[string]string{
		"resultCacheMaxBytesPerEntry": "not-a-number",
	})
	if err == nil {
		t.Errorf("expected parse err")
	}

	if ResultCache.maxLen != RESULT_CACHE_DEFAULT_MAX_LEN ||
		ResultCache.minLookups != RESULT_CACHE_DEFAULT_MIN_LOOKUPS ||
		ResultCache.maxBytesPerEntry != RESULT_CACHE_DEFAULT_MAX_BYTES_PER_ENTRY {
		t.Errorf("expected parse error to leave options untouched")
	}

	err = InitResultCacheOptions(map[string]string{
		"resultCacheMaxLen":           "100",
		"resultCacheMinLookups":       "200",
		"resultCacheMaxBytesPerEntry": "300",
	})
	if err != nil {
		t.Errorf("expected no parse err, got: %v", err)
	}

	if ResultCache.maxLen != 100 ||
		ResultCache.minLookups != 200 ||
		ResultCache.maxBytesPerEntry != 300 {
		t.Errorf("didn't parse options right")
	}
}

func TestResultCache(t *testing.T) {
	var rcOrig resultCache
	rcCopy(&rcOrig, &ResultCache)
	defer func() {
		rcCopy(&ResultCache, &rcOrig)
	}()

	rc := &ResultCache
	rc.maxLen = 1
	rc.minLookups = 2
	rc.maxBytesPerEntry = 10

	v, err := rc.lookup("not-there", 0)
	if err != nil || v != nil {
		t.Errorf("expected no hit")
	}
	if rc.cache["not-there"].totLookups != 1 ||
		rc.cache["not-there"].totHits != 0 {
		t.Errorf("expected totLookups/totHits now 1/0")
	}
	if rc.head != rc.tail ||
		rc.head == nil ||
		rc.tail == nil ||
		rc.head.next != nil || rc.head.prev != nil ||
		rc.tail.next != nil || rc.tail.prev != nil ||
		rc.head.key != "not-there" {
		t.Errorf("expected 1 LRU entry for not-there")
	}

	v, err = rc.lookup("key0", 0)
	if err != nil || v != nil {
		t.Errorf("expected no hit for key0")
	}
	if rc.cache["not-there"] != nil {
		t.Errorf("exepected not-there to be evicted")
	}
	if rc.cache["key0"].totLookups != 1 ||
		rc.cache["key0"].totHits != 0 {
		t.Errorf("expected totLookups/totHits now 1/0 for key0")
	}
	if rc.head != rc.tail ||
		rc.head == nil ||
		rc.tail == nil ||
		rc.head.next != nil || rc.head.prev != nil ||
		rc.tail.next != nil || rc.tail.prev != nil ||
		rc.head.key != "key0" {
		t.Errorf("expected 1 LRU entry for not-there")
	}

	// ------------------------------------

	rc.encache("key0", func() []byte {
		return []byte("v")
	}, 100, 123) // Expected encache to be a no-op since minLookups not reached.

	v, err = rc.lookup("key0", 0)
	if err != nil || v != nil {
		t.Errorf("expected no hit for key0")
	}
	if rc.cache["not-there"] != nil {
		t.Errorf("exepected not-there to be evicted")
	}
	if rc.cache["key0"].totLookups != 2 ||
		rc.cache["key0"].totHits != 0 {
		t.Errorf("expected totLookups/totHits now 2/0 for key0")
	}
	if rc.head != rc.tail ||
		rc.head == nil ||
		rc.tail == nil ||
		rc.head.next != nil || rc.head.prev != nil ||
		rc.tail.next != nil || rc.tail.prev != nil ||
		rc.head.key != "key0" {
		t.Errorf("expected 1 LRU entry for key0")
	}

	// ------------------------------------

	rc.encache("key0", func() []byte {
		return []byte("vv")
	}, 200, 234) // Expected encache to work since minLookups reached.

	v, err = rc.lookup("key0", 0)
	if err != nil || v == nil {
		t.Errorf("expected hit for key0")
	}
	if string(v) != "vv" {
		t.Errorf("expected vv")
	}
	if rc.cache["not-there"] != nil {
		t.Errorf("exepected not-there to be evicted")
	}
	if rc.cache["key0"].totLookups != 3 ||
		rc.cache["key0"].totHits != 1 {
		t.Errorf("expected totLookups/totHits now 3/1 for key0")
	}
	if rc.head != rc.tail ||
		rc.head == nil ||
		rc.tail == nil ||
		rc.head.next != nil || rc.head.prev != nil ||
		rc.tail.next != nil || rc.tail.prev != nil ||
		rc.head.key != "key0" ||
		rc.head.result == nil ||
		rc.head.resultRev != 200 ||
		rc.head.resultCost != 234 {
		t.Errorf("expected 1 LRU entry for key0")
	}

	// ------------------------------------

	rc.encache("key0", func() []byte {
		return []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	}, 300, 345) // Expected encache to no-op since val too long.

	v, err = rc.lookup("key0", 0)
	if err != nil || v == nil {
		t.Errorf("expected hit for key0")
	}
	if string(v) != "vv" {
		t.Errorf("expected vv")
	}
	if rc.cache["not-there"] != nil {
		t.Errorf("exepected not-there to be evicted")
	}
	if rc.cache["key0"].totLookups != 4 ||
		rc.cache["key0"].totHits != 2 {
		t.Errorf("expected totLookups/totHits now 4/2 for key0")
	}
	if rc.head != rc.tail ||
		rc.head == nil ||
		rc.tail == nil ||
		rc.head.next != nil || rc.head.prev != nil ||
		rc.tail.next != nil || rc.tail.prev != nil ||
		rc.head.key != "key0" ||
		rc.head.result == nil ||
		rc.head.resultRev != 200 ||
		rc.head.resultCost != 234 {
		t.Errorf("expected 1 LRU entry for key0")
	}

	// ------------------------------------

	v, err = rc.lookup("key0", 100000)
	if err != nil || v != nil {
		t.Errorf("expected miss for key0 due to wrong rev")
	}
}

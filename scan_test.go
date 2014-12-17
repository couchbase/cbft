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
	"container/heap"
	"testing"
)

type testScanCursor struct {
	done bool
	key  string
	val  string
}

func (c *testScanCursor) Done() bool {
	return c.done
}

func (c *testScanCursor) Key() []byte {
	return []byte(c.key)
}

func (c *testScanCursor) Val() []byte {
	return []byte(c.val)
}

func (c *testScanCursor) Next() bool {
	return false
}

func TestScanCursors(t *testing.T) {
	s := ScanCursors{}
	heap.Init(&s)
	heap.Push(&s, &testScanCursor{
		key: "b",
	})
	heap.Push(&s, &testScanCursor{
		key: "a",
	})
	heap.Push(&s, &testScanCursor{
		key: "c",
	})
	a := heap.Pop(&s).(*testScanCursor)
	if a.key != "a" {
		t.Errorf("expected a")
	}
	b := heap.Pop(&s).(*testScanCursor)
	if b.key != "b" {
		t.Errorf("expected b")
	}
	c := heap.Pop(&s).(*testScanCursor)
	if c.key != "c" {
		t.Errorf("expected c")
	}
}

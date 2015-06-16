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
	"bytes"
)

// A ScanCursor represents a scan of a resultset.
type ScanCursor interface {
	Done() bool
	Key() []byte
	Val() []byte
	Next() bool
}

// ScanCursors implements the heap.Interface for easy merging.
type ScanCursors []ScanCursor

func (pq ScanCursors) Len() int { return len(pq) }

func (pq ScanCursors) Less(i, j int) bool {
	return bytes.Compare(pq[i].Key(), pq[j].Key()) < 0
}

func (pq ScanCursors) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *ScanCursors) Push(x interface{}) {
	*pq = append(*pq, x.(ScanCursor))
}

func (pq *ScanCursors) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

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
	"encoding/json"
	"fmt"
	"io"
	"testing"
)

type TestDest struct{}

func (s *TestDest) Close() error {
	return nil
}

func (s *TestDest) DataUpdate(partition string,
	key []byte, seq uint64, val []byte,
	extrasType DestExtrasType, extras []byte) error {
	return nil
}

func (s *TestDest) DataDelete(partition string,
	key []byte, seq uint64,
	extrasType DestExtrasType, extras []byte) error {
	return nil
}

func (s *TestDest) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	return nil
}

func (s *TestDest) OpaqueSet(partition string,
	value []byte) error {
	return nil
}

func (s *TestDest) OpaqueGet(partition string) (
	value []byte, lastSeq uint64, err error) {
	return nil, 0, nil
}

func (s *TestDest) Rollback(partition string,
	rollbackSeq uint64) error {
	return nil
}

func (s *TestDest) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	return nil
}

func (t *TestDest) Count(pindex *PIndex,
	cancelCh <-chan bool) (uint64, error) {
	return 0, nil
}

func (t *TestDest) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	return nil
}

func (t *TestDest) Stats(w io.Writer) error {
	return nil
}

func TestBasicPartitionFunc(t *testing.T) {
	dest := &TestDest{}
	dest2 := &TestDest{}
	s, err := BasicPartitionFunc("", nil, map[string]Dest{"": dest})
	if err != nil || s != dest {
		t.Errorf("expected BasicPartitionFunc to work")
	}
	s, err = BasicPartitionFunc("foo", nil, map[string]Dest{"": dest})
	if err != nil || s != dest {
		t.Errorf("expected BasicPartitionFunc to hit the catch-all dest")
	}
	s, err = BasicPartitionFunc("", nil, map[string]Dest{"foo": dest})
	if err == nil || s == dest {
		t.Errorf("expected BasicPartitionFunc to not work")
	}
	s, err = BasicPartitionFunc("foo", nil, map[string]Dest{"foo": dest})
	if err != nil || s != dest {
		t.Errorf("expected BasicPartitionFunc to work on partition hit")
	}
	s, err = BasicPartitionFunc("foo", nil, map[string]Dest{"foo": dest, "": dest2})
	if err != nil || s != dest {
		t.Errorf("expected BasicPartitionFunc to work on partition hit")
	}
}

type ErrorOnlyDestProvider struct{}

func (dp *ErrorOnlyDestProvider) Dest(partition string) (Dest, error) {
	return nil, fmt.Errorf("always error for testing")
}

func (dp *ErrorOnlyDestProvider) Count(pindex *PIndex,
	cancelCh <-chan bool) (uint64, error) {
	return 0, fmt.Errorf("always error for testing")
}

func (dp *ErrorOnlyDestProvider) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	return fmt.Errorf("always error for testing")
}

func (dp *ErrorOnlyDestProvider) Stats(io.Writer) error {
	return fmt.Errorf("always error for testing")
}

func (dp *ErrorOnlyDestProvider) Close() error {
	return fmt.Errorf("always error for testing")
}

func TestErrorOnlyDestProviderWithDestForwarder(t *testing.T) {
	df := &DestForwarder{&ErrorOnlyDestProvider{}}
	if df.DataUpdate("", nil, 0, nil, DEST_EXTRAS_TYPE_NIL, nil) == nil {
		t.Errorf("expected err")
	}
	if df.DataDelete("", nil, 0, DEST_EXTRAS_TYPE_NIL, nil) == nil {
		t.Errorf("expected err")
	}
	if df.SnapshotStart("", 0, 0) == nil {
		t.Errorf("expected err")
	}
	if df.OpaqueSet("", nil) == nil {
		t.Errorf("expected err")
	}
	value, lastSeq, err := df.OpaqueGet("")
	if err == nil || value != nil || lastSeq != 0 {
		t.Errorf("expected err")
	}
	if df.Rollback("", 0) == nil {
		t.Errorf("expected err")
	}
	if df.ConsistencyWait("", "", "", 0, nil) == nil {
		t.Errorf("expected err")
	}
}

func TestDestStatsWriteJSON(t *testing.T) {
	ds := NewDestStats()
	var buf bytes.Buffer
	ds.WriteJSON(&buf)
	m := map[string]interface{}{}
	err := json.Unmarshal(buf.Bytes(), &m)
	if err != nil {
		t.Errorf("expected clean json, err: %v", err)
	}
	if m == nil || len(m) <= 0 {
		t.Errorf("expected some m")
	}
}

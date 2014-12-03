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
	"io"
	"testing"
)

type TestDest struct{}

func (s *TestDest) Close() error {
	return nil
}

func (s *TestDest) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	return nil
}

func (s *TestDest) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	return nil
}

func (s *TestDest) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	return nil
}

func (s *TestDest) SetOpaque(partition string,
	value []byte) error {
	return nil
}

func (s *TestDest) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	return nil, 0, nil
}

func (s *TestDest) Rollback(partition string,
	rollbackSeq uint64) error {
	return nil
}

func (s *TestDest) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh chan struct{}) error {
	return nil
}

func (t *TestDest) Count(pindex *PIndex,
	cancelCh chan struct{}) (uint64, error) {
	return 0, nil
}

func (t *TestDest) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh chan struct{}) error {
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

func TestDestFeed(t *testing.T) {
	df := NewDestFeed("", BasicPartitionFunc, map[string]Dest{})
	if df.Start() != nil {
		t.Errorf("expected DestFeed start to work")
	}

	buf := make([]byte, 0, 100)
	err := df.Stats(bytes.NewBuffer(buf))
	if err != nil {
		t.Errorf("expected DestFeed stats to work")
	}

	key := []byte("k")
	seq := uint64(123)
	val := []byte("v")

	if df.OnDataUpdate("unknown-partition", key, seq, val) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.OnDataDelete("unknown-partition", key, seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.OnSnapshotStart("unknown-partition", seq, seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.SetOpaque("unknown-partition", val) == nil {
		t.Errorf("expected err on bad partition")
	}
	_, _, err = df.GetOpaque("unknown-partition")
	if err == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.Rollback("unknown-partition", seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.ConsistencyWait("unknown-partition", "level", seq, nil) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.Query(nil, nil, nil, nil) == nil {
		t.Errorf("expected err on querying a dest feed")
	}
}

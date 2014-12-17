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
	"fmt"
	"io"
	"testing"
)

type ErrorOnlyFeed struct {
	name string
}

func (t *ErrorOnlyFeed) Name() string {
	return t.name
}

func (t *ErrorOnlyFeed) Start() error {
	return fmt.Errorf("ErrorOnlyFeed Start() invoked")
}

func (t *ErrorOnlyFeed) Close() error {
	return fmt.Errorf("ErrorOnlyFeed Close() invoked")
}

func (t *ErrorOnlyFeed) Dests() map[string]Dest {
	return nil
}

func (t *ErrorOnlyFeed) Stats(w io.Writer) error {
	return fmt.Errorf("ErrorOnlyFeed Stats() invoked")
}

func TestParsePartitionsToVBucketIds(t *testing.T) {
	v, err := ParsePartitionsToVBucketIds(nil)
	if err != nil || v == nil || len(v) != 0 {
		t.Errorf("expected empty")
	}
	v, err = ParsePartitionsToVBucketIds(map[string]Dest{})
	if err != nil || v == nil || len(v) != 0 {
		t.Errorf("expected empty")
	}
	v, err = ParsePartitionsToVBucketIds(map[string]Dest{"123": nil})
	if err != nil || v == nil || len(v) != 1 {
		t.Errorf("expected one entry")
	}
	if v[0] != uint16(123) {
		t.Errorf("expected 123")
	}
	v, err = ParsePartitionsToVBucketIds(map[string]Dest{"!bad": nil})
	if err == nil || v != nil {
		t.Errorf("expected error")
	}
}

func TestDataSourcePartitions(t *testing.T) {
	a, err := DataSourcePartitions("a fake source type",
		"sourceName", "sourceUUID", "sourceParams", "serverURL")
	if err == nil || a != nil {
		t.Errorf("expected fake data source type to error")
	}

	a, err = DataSourcePartitions("couchbase",
		"sourceName", "sourceUUID", "sourceParams", "serverURL")
	if err == nil || a != nil {
		t.Errorf("expected couchbase source type to error on bad server url")
	}

	a, err = DataSourcePartitions("couchbase-dcp",
		"sourceName", "sourceUUID", "sourceParams", "serverURL")
	if err == nil || a != nil {
		t.Errorf("expected couchbase source type to error on bad server url")
	}

	a, err = DataSourcePartitions("couchbase-tap",
		"sourceName", "sourceUUID", "sourceParams", "serverURL")
	if err == nil || a != nil {
		t.Errorf("expected couchbase source type to error on bad server url")
	}

	a, err = DataSourcePartitions("nil",
		"sourceName", "sourceUUID", "sourceParams", "serverURL")
	if err != nil || a != nil {
		t.Errorf("expected nil source type to work, but have no partitions")
	}

	a, err = DataSourcePartitions("primary",
		"sourceName", "sourceUUID", "sourceParams", "serverURL")
	if err == nil || a != nil {
		t.Errorf("expected dest source type to error on non-json server params")
	}

	a, err = DataSourcePartitions("primary",
		"sourceName", "sourceUUID", "", "serverURL")
	if err != nil || a == nil {
		t.Errorf("expected dest source type to ok on empty server params")
	}

	a, err = DataSourcePartitions("primary",
		"sourceName", "sourceUUID", "{}", "serverURL")
	if err != nil || a == nil {
		t.Errorf("expected dest source type to ok on empty JSON server params")
	}
}

func TestNilFeedStart(t *testing.T) {
	if NewNILFeed("", nil).Start() != nil {
		t.Errorf("expected NILFeed.Start() to work")
	}
}

func TestPrimaryFeed(t *testing.T) {
	df := NewPrimaryFeed("", BasicPartitionFunc, map[string]Dest{})
	if df.Start() != nil {
		t.Errorf("expected PrimaryFeed start to work")
	}

	buf := make([]byte, 0, 100)
	err := df.Stats(bytes.NewBuffer(buf))
	if err != nil {
		t.Errorf("expected PrimaryFeed stats to work")
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

func TestDCPFeedParams(t *testing.T) {
	p := &DCPFeedParams{
		AuthUser:     "au",
		AuthPassword: "ap",
	}
	a, b, c := p.GetCredentials()
	if a != "au" || b != "ap" || c != "au" {
		t.Errorf("wrong creds")
	}
}

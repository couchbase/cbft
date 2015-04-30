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

func (t *ErrorOnlyFeed) IndexName() string {
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
	f := NewNILFeed("aaa", "bbb", nil)
	if f.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if f.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if f.Start() != nil {
		t.Errorf("expected NILFeed.Start() to work")
	}
}

func TestPrimaryFeed(t *testing.T) {
	df := NewPrimaryFeed("aaa", "bbb",
		BasicPartitionFunc, map[string]Dest{})
	if df.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if df.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
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

	if df.DataUpdate("unknown-partition", key, seq, val) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.DataDelete("unknown-partition", key, seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.SnapshotStart("unknown-partition", seq, seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.OpaqueSet("unknown-partition", val) == nil {
		t.Errorf("expected err on bad partition")
	}
	_, _, err = df.OpaqueGet("unknown-partition")
	if err == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.Rollback("unknown-partition", seq) == nil {
		t.Errorf("expected err on bad partition")
	}
	if df.ConsistencyWait("unknown-partition", "unknown-partition-UUID",
		"level", seq, nil) == nil {
		t.Errorf("expected err on bad partition")
	}
	df2 := NewPrimaryFeed("", "", BasicPartitionFunc, map[string]Dest{
		"some-partition": &TestDest{},
	})
	if df2.ConsistencyWait("some-partition", "some-partition-UUID",
		"level", seq, nil) != nil {
		t.Errorf("expected no err on some partition to TestDest")
	}
	_, err = df.Count(nil, nil)
	if err == nil {
		t.Errorf("expected err on counting a primary feed")
	}
	if df.Query(nil, nil, nil, nil) == nil {
		t.Errorf("expected err on querying a primary feed")
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

func TestVBucketIdToPartitionDest(t *testing.T) {
	var dests map[string]Dest

	pf_hi := func(partition string, key []byte, dests map[string]Dest) (Dest, error) {
		if string(key) != "hi" {
			t.Errorf("expected hi")
		}
		return nil, nil
	}
	partition, dest, err := VBucketIdToPartitionDest(pf_hi, dests, 0, []byte("hi"))
	if err != nil || dest != nil || partition != "0" {
		t.Errorf("expected no err, got: %v", err)
	}

	pf_bye := func(partition string, key []byte, dests map[string]Dest) (Dest, error) {
		if string(key) != "bye" {
			t.Errorf("expected bye")
		}
		return nil, nil
	}
	partition, dest, err = VBucketIdToPartitionDest(pf_bye, dests, 1025, []byte("bye"))
	if err != nil || dest != nil || partition != "1025" {
		t.Errorf("expected no err, got: %v", err)
	}

	pf_err := func(partition string, key []byte, dests map[string]Dest) (Dest, error) {
		return nil, fmt.Errorf("whoa_err")
	}
	partition, dest, err = VBucketIdToPartitionDest(pf_err, dests, 123, nil)
	if err == nil {
		t.Errorf("expected err")
	}
}

func TestTAPFeedBasics(t *testing.T) {
	df, err := NewTAPFeed("aaa", "bbb",
		"url", "poolName", "bucketName", "bucketUUID", "",
		BasicPartitionFunc, map[string]Dest{}, false)
	if err != nil {
		t.Errorf("expected NewTAPFeed to work")
	}
	if df.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if df.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if df.Dests() == nil {
		t.Errorf("expected some dests")
	}
	err = df.Stats(bytes.NewBuffer(nil))
	if err != nil {
		t.Errorf("expected stats to work")
	}
}

func TestDCPFeedBasics(t *testing.T) {
	df, err := NewDCPFeed("aaa", "bbb",
		"url", "poolName", "bucketName", "bucketUUID", "",
		BasicPartitionFunc, map[string]Dest{}, false)
	if err != nil {
		t.Errorf("expected NewDCPFeed to work")
	}
	if df.Name() != "aaa" {
		t.Errorf("expected aaa name")
	}
	if df.IndexName() != "bbb" {
		t.Errorf("expected bbb index name")
	}
	if df.Dests() == nil {
		t.Errorf("expected some dests")
	}
	err = df.Stats(bytes.NewBuffer(nil))
	if err != nil {
		t.Errorf("expected stats to work")
	}
}

func TestCouchbaseParseSourceName(t *testing.T) {
	s, p, b := CouchbaseParseSourceName("s", "p", "b")
	if s != "s" ||
		p != "p" ||
		b != "b" {
		t.Errorf("expected s, p, b")
	}

	badURL := "http://a/badURL"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools/default"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools/default/buckets"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools/default/buckets/"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	badURL = "http://a:8091/pools//buckets/theBucket"
	s, p, b = CouchbaseParseSourceName("s", "p", badURL)
	if s != "s" ||
		p != "p" ||
		b != badURL {
		t.Errorf("expected s, p, badURL: %s, got: %s %s %s",
			badURL, s, p, b)
	}

	bu := "http://a:8091/pools/myPool/buckets/theBucket"
	s, p, b = CouchbaseParseSourceName("s", "p", bu)
	if s != "http://a:8091" ||
		p != "myPool" ||
		b != "theBucket" {
		t.Errorf("expected theBucket, got: %s %s %s", s, p, b)
	}

	bu = "https://a:8091/pools/myPool/buckets/theBucket"
	s, p, b = CouchbaseParseSourceName("s", "p", bu)
	if s != "https://a:8091" ||
		p != "myPool" ||
		b != "theBucket" {
		t.Errorf("expected theBucket, got: %s %s %s", s, p, b)
	}
}

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
	"reflect"
	"testing"
)

func TestNegativeIndexClient(t *testing.T) {
	bc := &IndexClient{}
	if bc.Index("", nil) != indexClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.Delete("") != indexClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.Batch(nil) != indexClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	d, err := bc.Document("")
	if err != indexClientUnimplementedErr || d != nil {
		t.Errorf("expected unimplemented")
	}
	c, err := bc.DocCount()
	if err == nil || c != 0 {
		t.Errorf("expected count error on empty CountURL")
	}
	sr, err := bc.Search(nil)
	if err == nil || sr != nil {
		t.Errorf("expected search error on empty QueryURL")
	}
	f, err := bc.Fields()
	if err != indexClientUnimplementedErr || f != nil {
		t.Errorf("expected unimplemented")
	}
	if bc.DumpAll() != nil {
		t.Errorf("expected nil")
	}
	if bc.DumpDoc("") != nil {
		t.Errorf("expected nil")
	}
	if bc.DumpFields() != nil {
		t.Errorf("expected nil")
	}
	if bc.Close() != indexClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.Mapping() != nil {
		t.Errorf("expected nil")
	}
	if bc.Stats() != nil {
		t.Errorf("expected nil")
	}
	val, err := bc.GetInternal(nil)
	if err != indexClientUnimplementedErr || val != nil {
		t.Errorf("expected unimplemented")
	}
	if bc.SetInternal(nil, nil) != indexClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.DeleteInternal(nil) != indexClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
}

func TestBadURLsIndexClient(t *testing.T) {
	bc := &IndexClient{
		CountURL:   "bogus url",
		QueryURL:   "fake url",
		httpClient: HttpClient,
	}
	c, err := bc.DocCount()
	if err == nil || c != 0 {
		t.Errorf("expected count error on bad CountURL")
	}
	sr, err := bc.Search(nil)
	if err == nil || sr != nil {
		t.Errorf("expected search error on bad QueryURL")
	}
}

func TestGroupIndexClientsByHostPort(t *testing.T) {
	c0 := &IndexClient{
		HostPort:    "x",
		IndexName:   "indexA",
		PIndexNames: []string{"a", "b"},
		QueryURL:    "foo",
		httpClient:  HttpClient,
	}
	c1 := &IndexClient{
		HostPort:    "x",
		IndexName:   "indexA",
		PIndexNames: []string{"c"},
		QueryURL:    "bar",
		httpClient:  HttpClient,
	}
	c2 := &IndexClient{
		HostPort:    "y",
		IndexName:   "indexA",
		PIndexNames: []string{"d"},
		QueryURL:    "baz",
		httpClient:  HttpClient,
	}

	a, err := GroupIndexClientsByHostPort([]*IndexClient{c0, c1, c2})
	if err != nil {
		t.Errorf("expect nil err")
	}
	if len(a) != 2 {
		t.Errorf("expect 2 hostPorts")
	}
	if a[0].HostPort != "x" ||
		len(a[0].PIndexNames) != 3 ||
		!reflect.DeepEqual(a[0].PIndexNames, []string{"a", "b", "c"}) {
		t.Errorf("expect x has 3 pindexes")
	}
	if a[0].QueryURL != "http://x/api/index/indexA/query" {
		t.Errorf("expect x query URL")
	}
	if a[1].HostPort != "y" ||
		len(a[1].PIndexNames) != 1 ||
		!reflect.DeepEqual(a[1].PIndexNames, []string{"d"}) {
		t.Errorf("expect y has 1 pindexes")
	}

	a, err = GroupIndexClientsByHostPort([]*IndexClient{})
	if err != nil {
		t.Errorf("expect nil err")
	}
	if len(a) != 0 {
		t.Errorf("expect 0 hostPorts")
	}

	a, err = GroupIndexClientsByHostPort(nil)
	if err != nil {
		t.Errorf("expect nil err")
	}
	if len(a) != 0 {
		t.Errorf("expect 0 hostPorts")
	}
}

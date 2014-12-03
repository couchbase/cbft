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
	"testing"
)

func TestNegativeBleveClient(t *testing.T) {
	bc := &BleveClient{}
	if bc.Index("", nil) != bleveClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.Delete("") != bleveClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.Batch(nil) != bleveClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	d, err := bc.Document("")
	if err != bleveClientUnimplementedErr || d != nil {
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
	if err != bleveClientUnimplementedErr || f != nil {
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
	if bc.Close() != bleveClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.Mapping() != nil {
		t.Errorf("expected nil")
	}
	if bc.Stats() != nil {
		t.Errorf("expected nil")
	}
	val, err := bc.GetInternal(nil)
	if err != bleveClientUnimplementedErr || val != nil {
		t.Errorf("expected unimplemented")
	}
	if bc.SetInternal(nil, nil) != bleveClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
	if bc.DeleteInternal(nil) != bleveClientUnimplementedErr {
		t.Errorf("expected unimplemented")
	}
}

func TestBadURLsBleveClient(t *testing.T) {
	bc := &BleveClient{
		CountURL: "bogus url",
		QueryURL: "fake url",
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

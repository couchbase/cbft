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
	"io/ioutil"
	"os"
	"testing"
)

func TestOpenPIndex(t *testing.T) {
	pindex, err := OpenPIndex(nil, "not-a-bleve-file")
	if pindex != nil || err == nil {
		t.Errorf("expected OpenPIndex to fail on a bad file")
	}
}

func TestNewPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := NewPIndex(nil, "fake", "uuid",
		"bleve", "indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", "sourceParams", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to work")
	}
	err = pindex.Close(true)
	if err != nil {
		t.Errorf("expected Close to work")
	}
}

func TestNewPIndexEmptyJSON(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := NewPIndex(nil, "fake", "uuid",
		"bleve", "indexName", "indexUUID", "{}",
		"sourceType", "sourceName", "sourceUUID", "sourceParams", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to fail with empty json map")
	}
}

func TestNewPIndexBadMapping(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := NewPIndex(nil, "fake", "uuid",
		"bleve", "indexName", "indexUUID", "} hey this isn't json :-(",
		"sourceType", "sourceName", "sourceUUID", "sourceParams", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex != nil || err == nil {
		t.Errorf("expected NewPIndex to fail with bad json")
	}
}

func TestNewPIndexImpl(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	restart := func() {
		t.Errorf("not expecting a restart")
	}

	indexParams := ""

	pindexImpl, dest, err := NewPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE",
		indexParams, emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on unknown impl type")
	}

	pindexImpl, dest, err = OpenPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE", emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on unknown impl type")
	}

	pindexImpl, dest, err = NewPIndexImpl("bleve", indexParams, emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on existing dir")
	}
}

func TestBlackholePIndexImpl(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	restart := func() {
		t.Errorf("not expecting a restart")
	}

	pindex, dest, err := OpenBlackHolePIndexImpl("blackhole", emptyDir, restart)
	if err == nil || pindex != nil || dest != nil {
		t.Errorf("expected OpenBlackHolePIndexImpl to error on emptyDir")
	}

	pindex, dest, err = NewBlackHolePIndexImpl("blackhole", "", emptyDir, restart)
	if err != nil || pindex == nil || dest == nil {
		t.Errorf("expected NewBlackHolePIndexImpl to work")
	}

	pindex, dest, err = OpenBlackHolePIndexImpl("blackhole", emptyDir, restart)
	if err != nil || pindex == nil || dest == nil {
		t.Errorf("expected OpenBlackHolePIndexImpl to work")
	}

	if dest.Close() != nil ||
		dest.OnDataUpdate("", nil, 0, nil) != nil ||
		dest.OnDataDelete("", nil, 0) != nil ||
		dest.OnSnapshotStart("", 0, 0) != nil ||
		dest.SetOpaque("", nil) != nil ||
		dest.Rollback("", 0) != nil ||
		dest.ConsistencyWait("", "", "", 0, nil) != nil ||
		dest.Query(nil, nil, nil, nil) != nil {
		t.Errorf("expected no errors from a blackhole pindex impl")
	}

	c, err := dest.Count(nil, nil)
	if err != nil || c != 0 {
		t.Errorf("expected 0, no err")
	}

	b := &bytes.Buffer{}
	err = dest.Stats(b)
	if err != nil {
		t.Errorf("expected 0, no err")
	}
	if string(b.Bytes()) != "null" {
		t.Errorf("expected null")
	}

	v, lastSeq, err := dest.GetOpaque("")
	if err != nil || v != nil || lastSeq != 0 {
		t.Errorf("expected nothing from blackhole.GetOpaque()")
	}

	bt := PIndexImplTypes["blackhole"]
	if bt == nil {
		t.Errorf("expected blackhole in PIndexImplTypes")
	}
	if bt.New == nil || bt.Open == nil {
		t.Errorf("blackhole should have open and new funcs")
	}
	count, err := bt.Count(nil, "", "")
	if err == nil || count != 0 {
		t.Errorf("expected blackhole count to err")
	}
	err = bt.Query(nil, "", "", nil, nil)
	if err == nil {
		t.Errorf("expected blackhole query to err")
	}
}

func TestErrorConsistencyWait(t *testing.T) {
	e := &ErrorConsistencyWait{}
	if e.Error() == "" {
		t.Errorf("expected err")
	}
}

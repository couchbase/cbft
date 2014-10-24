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

package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestOpenPIndex(t *testing.T) {
	pindex, err := OpenPIndex("not-a-bleve-file")
	if pindex != nil || err == nil {
		t.Errorf("expected OpenPIndex to fail on a bad file")
	}
}

func TestNewPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	// TODO: Should have a blackhole index implementation for testing.

	pindex, err := NewPIndex("fake", "uuid",
		"bleve", "indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to work")
	}
	close(pindex.Stream)

	pindex, err = NewPIndex("fake", "uuid",
		"bleve", "indexName", "indexUUID", "{}",
		"sourceType", "sourceName", "sourceUUID", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex != nil || err == nil {
		t.Errorf("expected NewPIndex to fail with empty json map")
	}

	pindex, err = NewPIndex("fake", "uuid",
		"bleve", "indexName", "indexUUID", "} hey this isn't json :-(",
		"sourceType", "sourceName", "sourceUUID", "sourcePartitions",
		PIndexPath(emptyDir, "fake"))
	if pindex != nil || err == nil {
		t.Errorf("expected NewPIndex to fail with bad json")
	}
}

func TestNewPIndexImpl(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindexImpl, err := NewPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE", "", emptyDir)
	if err == nil || pindexImpl != nil {
		t.Errorf("expected err on unknown impl type")
	}

	pindexImpl, err = OpenPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE", emptyDir)
	if err == nil || pindexImpl != nil {
		t.Errorf("expected err on unknown impl type")
	}
}

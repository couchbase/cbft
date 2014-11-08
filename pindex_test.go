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
	pindex, err := OpenPIndex(nil, "not-a-bleve-file")
	if pindex != nil || err == nil {
		t.Errorf("expected OpenPIndex to fail on a bad file")
	}
}

func TestNewPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	// TODO: Should have a blackhole index implementation for testing.

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

	indexSchema := ""

	pindexImpl, dest, err := NewPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE",
		indexSchema, emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on unknown impl type")
	}

	pindexImpl, dest, err = OpenPIndexImpl("AN UNKNOWN PINDEX IMPL TYPE", emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on unknown impl type")
	}

	pindexImpl, dest, err = NewPIndexImpl("bleve", indexSchema, emptyDir, restart)
	if err == nil || pindexImpl != nil || dest != nil {
		t.Errorf("expected err on existing dir")
	}
}

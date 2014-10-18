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
	pindex, err := OpenPIndex("fake", "not-a-bleve-file")
	if pindex != nil || err == nil {
		t.Errorf("expected OpenPIndex to fail on a bad file")
	}
}

func TestNewPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := NewPIndex("fake", PIndexPath(emptyDir, "fake"),
		[]byte{})
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to work")
	}
	close(pindex.Stream())

	pindex, err = NewPIndex("fake", PIndexPath(emptyDir, "fake"),
		[]byte("{}"))
	if pindex != nil || err == nil {
		t.Errorf("expected NewPIndex to fail with empty json map")
	}
}

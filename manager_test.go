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

func TestPIndexPath(t *testing.T) {
	m := NewManager("dir", "svr", nil)
	p := m.PIndexPath("x")
	expected := "dir" + string(os.PathSeparator) + "x.pindex"
	if p != expected {
		t.Errorf("wrong pindex path %s, %s", p, expected)
	}
	n, ok := m.ParsePIndexPath(p)
	if !ok || n != "x" {
		t.Errorf("parse pindex path not ok, %v, %v", n, ok)
	}
}

func TestManagerStart(t *testing.T) {
	m := NewManager("dir", "not-a-real-svr", nil)
	if m.Start() == nil {
		t.Errorf("expected NewManager() with bad svr should fail")
	}

	m = NewManager("not-a-real-dir", "", nil)
	if m.Start() == nil {
		t.Errorf("expected NewManager() with bad dir should fail")
	}
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	m = NewManager(emptyDir, "", nil)
	if err := m.Start(); err != nil {
		t.Errorf("expected NewManager() with empty dir to work, err: %v", err)
	}
}

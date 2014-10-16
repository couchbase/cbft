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
	"os"
	"testing"
)

func TestIndexPath(t *testing.T) {
	m := NewManager("dir", "svr")
	p := m.IndexPath("x")
	expected := "dir" + string(os.PathSeparator) + "x.cbft"
	if p != expected {
		t.Errorf("wrong index path %s, %s", p, expected)
	}
	n, ok := m.ParseIndexPath(p)
	if !ok || n != "x" {
		t.Errorf("parse index path not ok, %v, %v", n, ok)
	}
}

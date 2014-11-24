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

func TestNewManagerRESTRouter(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	ring, err := NewMsgRing(nil, 1)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, "", ":1000",
		emptyDir, "some-datasource", nil)
	r, err := NewManagerRESTRouter(mgr, emptyDir, ring)
	if r == nil || err != nil {
		t.Errorf("expected no errors")
	}

	mgr = NewManager(VERSION, cfg, NewUUID(), []string{"queryer", "anotherTag"},
		"", ":1000", emptyDir, "some-datasource", nil)
	r, err = NewManagerRESTRouter(mgr, emptyDir, ring)
	if r == nil || err != nil {
		t.Errorf("expected no errors")
	}
}

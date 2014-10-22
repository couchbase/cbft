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

func TestMainStart(t *testing.T) {
	router, err := MainStart(nil, NewUUID(), ":1000",
		"bad data dir", "./static", "", false)
	if router != nil || err == nil {
		t.Errorf("expected empty server string to fail mainStart()")
	}

	router, err = MainStart(nil, NewUUID(), ":1000",
		"bad data dir", "./static", "bad server", false)
	if router != nil || err == nil {
		t.Errorf("expected bad server string to fail mainStart()")
	}
}

func TestMainUUID(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	uuid, err := MainUUID(emptyDir)
	if err != nil || uuid == "" {
		t.Errorf("expected MainUUID() to work, err: %v", err)
	}

	uuid2, err := MainUUID(emptyDir)
	if err != nil || uuid2 != uuid {
		t.Errorf("expected MainUUID() reload to give same uuid, uuid: %s vs %s, err: %v",
			uuid, uuid2, err)
	}

	path := emptyDir + string(os.PathSeparator) + "cbft.uuid"
	os.Remove(path)
	ioutil.WriteFile(path, []byte{}, 0600)

	uuid3, err := MainUUID(emptyDir)
	if err == nil || uuid3 != "" {
		t.Errorf("expected MainUUID() to fail on empty file")
	}
}

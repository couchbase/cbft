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

	"github.com/couchbaselabs/cbft"
)

func TestMainStart(t *testing.T) {
	mr, err := cbft.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	router, err := MainStart(nil, cbft.NewUUID(), nil, "", 1, ":1000",
		"bad data dir", "./static", "etag", "", "", mr)
	if router != nil || err == nil {
		t.Errorf("expected empty server string to fail mainStart()")
	}

	router, err = MainStart(nil, cbft.NewUUID(), nil, "", 1, ":1000",
		"bad data dir", "./static", "etag", "bad server", "", mr)
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
		t.Errorf("expected MainUUID() reload to give same uuid,"+
			" uuid: %s vs %s, err: %v", uuid, uuid2, err)
	}

	path := emptyDir + string(os.PathSeparator) + "cbft.uuid"
	os.Remove(path)
	ioutil.WriteFile(path, []byte{}, 0600)

	uuid3, err := MainUUID(emptyDir)
	if err == nil || uuid3 != "" {
		t.Errorf("expected MainUUID() to fail on empty file")
	}
}

func TestMainCfg(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg, err := MainCfg("an unknown cfg provider", emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected MainCfg() to fail on unknown provider")
	}

	cfg, err = MainCfg("simple", emptyDir)
	if err != nil || cfg == nil {
		t.Errorf("expected MainCfg() to work on simple provider")
	}

	if _, err := cfg.Set("k", []byte("value"), 0); err != nil {
		t.Errorf("expected Set() to work")
	}

	cfg, err = MainCfg("simple", emptyDir)
	if err != nil || cfg == nil {
		t.Errorf("expected MainCfg() to work on simple provider when reload")
	}

	cfg, err = MainCfg("couchbase:http://", emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected err on bad url")
	}

	cfg, err = MainCfg("couchbase:http://user:pswd@127.0.0.1:666", emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected err on bad server")
	}
}

func TestMainWelcome(t *testing.T) {
	MainWelcome(flagAliases) // Don't crash.
}

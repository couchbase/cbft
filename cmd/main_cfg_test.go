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

package cmd

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestMainCfg(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	bindHttp := "10.1.1.20:8095"
	register := "wanted"

	cfg, err := MainCfg("cbft", "an unknown cfg provider",
		bindHttp, register, emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected MainCfg() to fail on unknown provider")
	}

	cfg, err = MainCfg("cbft", "simple", bindHttp, register, emptyDir)
	if err != nil || cfg == nil {
		t.Errorf("expected MainCfg() to work on simple provider")
	}

	if _, err := cfg.Set("k", []byte("value"), 0); err != nil {
		t.Errorf("expected Set() to work")
	}

	cfg, err = MainCfg("cbft", "simple", bindHttp, register, emptyDir)
	if err != nil || cfg == nil {
		t.Errorf("expected MainCfg() to work on simple provider when reload")
	}

	cfg, err = MainCfg("cbft", "couchbase:http://", bindHttp, register, emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected err on bad url")
	}

	cfg, err = MainCfg("cbft", "couchbase:http://user:pswd@127.0.0.1:666",
		bindHttp, register, emptyDir)
	if err == nil || cfg != nil {
		t.Errorf("expected err on bad server")
	}
}

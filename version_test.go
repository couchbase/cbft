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
	"testing"
)

func TestCheckVersion(t *testing.T) {
	ok, err := CheckVersion(nil, "1.1.0")
	if err != nil || ok {
		t.Errorf("expect nil err and not ok on nil cfg")
	}

	cfg := NewCfgMem()
	ok, err = CheckVersion(cfg, "1.0.0")
	if err != nil || !ok {
		t.Errorf("expected first version to win in brand new cfg")
	}
	v, _, err := cfg.Get(VERSION_KEY, 0)
	if err != nil || string(v) != "1.0.0" {
		t.Errorf("expected first version to persist in brand new cfg")
	}
	ok, err = CheckVersion(cfg, "1.1.0")
	if err != nil || !ok {
		t.Errorf("expected upgrade version to win")
	}
	v, _, err = cfg.Get(VERSION_KEY, 0)
	if err != nil || string(v) != "1.1.0" {
		t.Errorf("expected upgrade version to persist in brand new cfg")
	}
	ok, err = CheckVersion(cfg, "1.0.0")
	if err != nil || ok {
		t.Errorf("expected lower version to lose")
	}
	v, _, err = cfg.Get(VERSION_KEY, 0)
	if err != nil || string(v) != "1.1.0" {
		t.Errorf("expected version to remain stable on lower version check")
	}

	for i := 0; i < 3; i++ {
		cfg = NewCfgMem()
		eac := &ErrorAfterCfg{
			inner:    cfg,
			errAfter: i,
		}
		ok, err = CheckVersion(eac, "1.0.0")
		if err == nil || ok {
			t.Errorf("expected err when cfg errors on %d'th op", i)
		}
	}

	cfg = NewCfgMem()
	eac := &ErrorAfterCfg{
		inner:    cfg,
		errAfter: 3,
	}
	ok, err = CheckVersion(eac, "1.0.0")
	if err != nil || !ok {
		t.Errorf("expected ok when cfg doesn't error until 3rd op ")
	}

	cfg = NewCfgMem()
	eac = &ErrorAfterCfg{
		inner:    cfg,
		errAfter: 4,
	}
	ok, err = CheckVersion(eac, "1.0.0")
	if err != nil || !ok {
		t.Errorf("expected ok on first version init")
	}
	ok, err = CheckVersion(eac, "1.1.0")
	if err == nil || ok {
		t.Errorf("expected err when forcing cfg Set() error during verison upgrade")
	}
}

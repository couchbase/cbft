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

func TestCfgMem(t *testing.T) {
	testCfg(t, NewCfgMem())
}

func TestCfgSimple(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	testCfg(t, NewCfgSimple(emptyDir+string(os.PathSeparator)+"test.cfg"))
}

func testCfg(t *testing.T, c Cfg) {
	v, cas, err := c.Get("nope", 0)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to miss on brand new CfgMem")
	}
	v, cas, err = c.Get("nope", 100)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to miss on brand new CfgMem with wrong CAS")
	}
	cas, err = c.Set("a", []byte("A"), 100)
	if err == nil || cas != 0 {
		t.Errorf("expected creation Set() to fail when no entry and wrong CAS")
	}

	cas1, err := c.Set("a", []byte("A"), 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected creation Set() to ok CAS 0")
	}
	cas, err = c.Set("a", []byte("A"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected re-creation Set() to fail with CAS 0")
	}
	cas, err = c.Set("a", []byte("A"), 100)
	if err == nil || cas != 0 {
		t.Errorf("expected update Set() to fail when entry and wrong CAS")
	}
	v, cas, err = c.Get("a", 100)
	if err == nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to fail on wrong CAS")
	}
	v, cas, err = c.Get("a", 0)
	if err != nil || string(v) != "A" || cas != cas1 {
		t.Errorf("expected Get() to succeed on 0 CAS")
	}
	v, cas, err = c.Get("a", cas1)
	if err != nil || string(v) != "A" || cas != cas1 {
		t.Errorf("expected Get() to succeed on right CAS")
	}

	cas2, err := c.Set("a", []byte("AA"), cas1)
	if err != nil || cas2 != 2 {
		t.Errorf("expected update Set() to succeed when right CAS")
	}
	cas, err = c.Set("a", []byte("AA-should-fail"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected re-creation Set() to fail with CAS 0")
	}
	cas, err = c.Set("a", []byte("AA"), cas1)
	if err == nil || cas != 0 {
		t.Errorf("expected update Set() to fail when retried after success")
	}
	v, cas, err = c.Get("a", 100)
	if err == nil || v != nil || cas != 0 {
		t.Errorf("expected Get() to fail on wrong CAS")
	}
	v, cas, err = c.Get("a", 0)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on 0 CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on right CAS")
	}

	err = c.Del("nope", 0)
	if err != nil {
		t.Errorf("expected Del() to succeed on missing item when 0 CAS")
	}
	err = c.Del("nope", 100)
	if err == nil {
		t.Errorf("expected Del() to fail on missing item when non-zero CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on right CAS")
	}
	err = c.Del("a", 100)
	if err == nil {
		t.Errorf("expected Del() to fail when wrong CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || string(v) != "AA" || cas != cas2 {
		t.Errorf("expected Get() to succeed on right CAS")
	}
	err = c.Del("a", cas2)
	if err != nil {
		t.Errorf("expected Del() to succeed when right CAS")
	}
	v, cas, err = c.Get("a", cas2)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() with CAS to miss after Del(): "+
			" %v, %v, %v", err, v, cas)
	}
	v, cas, err = c.Get("a", 0)
	if err != nil || v != nil || cas != 0 {
		t.Errorf("expected Get() with 0 CAS to miss after Del(): "+
			" %v, %v, %v", err, v, cas)
	}
}

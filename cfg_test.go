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
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

type ErrorOnlyCfg struct{} // For testing.

func (c *ErrorOnlyCfg) Get(key string, cas uint64) (
	[]byte, uint64, error) {
	return nil, 0, fmt.Errorf("error only")
}

func (c *ErrorOnlyCfg) Set(key string, val []byte, cas uint64) (
	uint64, error) {
	return 0, fmt.Errorf("error only")
}

func (c *ErrorOnlyCfg) Del(key string, cas uint64) error {
	return fmt.Errorf("error only")
}

// ------------------------------------------------

func TestCfgMem(t *testing.T) {
	testCfg(t, NewCfgMem())
}

func TestCfgSimple(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgSimple(emptyDir + string(os.PathSeparator) + "test.cfg")
	testCfg(t, cfg)
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

func TestCfgCASError(t *testing.T) {
	err := &CfgCASError{}
	if err.Error() != "CAS mismatch" {
		t.Errorf("expected error string wasn't right")
	}
}

func TestCfgSimpleLoad(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	c := NewCfgSimple(emptyDir + string(os.PathSeparator) + "not-a-file.cfg")
	if err := c.Load(); err == nil {
		t.Errorf("expected Load() to fail on bogus file")
	}

	path := emptyDir + string(os.PathSeparator) + "test.cfg"

	c1 := NewCfgSimple(path)
	cas1, err := c1.Set("a", []byte("A"), 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected Set() on initial cfg simple")
	}

	c2 := NewCfgSimple(path)
	if err := c2.Load(); err != nil {
		t.Errorf("expected Load() to work")
	}
	v, cas, err := c2.Get("a", 0)
	if err != nil || v == nil || cas != cas1 {
		t.Errorf("expected Get() to succeed")
	}
	if string(v) != "A" {
		t.Errorf("exepcted to read what we wrote")
	}

	badPath := emptyDir + string(os.PathSeparator) + "bad.cfg"
	ioutil.WriteFile(badPath, []byte("}hey this is bad json :-{"), 0600)
	c3 := NewCfgSimple(badPath)
	if err = c3.Load(); err == nil {
		t.Errorf("expected Load() to fail on bad json file")
	}
}

func TestCfgSimpleSave(t *testing.T) {
	path := "totally/not/a/dir/test.cfg"
	c1 := NewCfgSimple(path)
	cas, err := c1.Set("a", []byte("A"), 0)
	if err == nil || cas != 0 {
		t.Errorf("expected Save() to bad dir to fail")
	}
}

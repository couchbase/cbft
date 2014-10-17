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

// Implements ManagerEventHandlers interface.
type TestMEH struct {
	lastPIndex *PIndex
	lastCall   string
}

func (meh *TestMEH) OnRegisterPIndex(pindex *PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnRegisterPIndex"
}

func (meh *TestMEH) OnUnregisterPIndex(pindex *PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnUnregisterPIndex"
}

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
	if m.DataDir() != "dir" {
		t.Errorf("wrong data dir")
	}

	m = NewManager("not-a-real-dir", "", nil)
	if m.Start() == nil {
		t.Errorf("expected NewManager() with bad dir should fail")
	}
	if m.DataDir() != "not-a-real-dir" {
		t.Errorf("wrong data dir")
	}

	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	m = NewManager(emptyDir, "", nil)
	if err := m.Start(); err != nil {
		t.Errorf("expected NewManager() with empty dir to work, err: %v", err)
	}
}

func TestManagerRegisterPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	p, err := NewPIndex("p0", m.PIndexPath("p0"), []byte{})
	if err != nil {
		t.Errorf("expected NewPIndex() to work")
	}
	defer close(p.Stream())
	px := m.UnregisterPIndex(p.Name())
	if px != nil {
		t.Errorf("expected UnregisterPIndex() on newborn manager to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err = m.RegisterPIndex(p)
	if err != nil {
		t.Errorf("expected first RegisterPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnRegisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	err = m.RegisterPIndex(p)
	if err == nil {
		t.Errorf("expected second RegisterPIndex() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	pc, ok := pindexes[p.Name()]
	if !ok || p != pc {
		t.Errorf("wrong pindex in current pindexes")
	}

	px = m.UnregisterPIndex(p.Name())
	if px == nil {
		t.Errorf("expected first UnregisterPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	px = m.UnregisterPIndex(p.Name())
	if px != nil {
		t.Errorf("expected second UnregisterPIndex() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
}

func TestManagerRegisterFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	f := &ErrorOnlyFeed{name: "f0"}
	fx := m.UnregisterFeed(f.Name())
	if fx != nil {
		t.Errorf("expected UnregisterFeed() on newborn manager to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err := m.RegisterFeed(f)
	if err != nil {
		t.Errorf("expected first RegisterFeed() to work")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err = m.RegisterFeed(f)
	if err == nil {
		t.Errorf("expected second RegisterFeed() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 1 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	fc, ok := feeds[f.Name()]
	if !ok || f != fc {
		t.Errorf("wrong feed in current feeds")
	}

	fx = m.UnregisterFeed(f.Name())
	if fx == nil {
		t.Errorf("expected first UnregisterFeed() to work")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	fx = m.UnregisterFeed(f.Name())
	if fx != nil {
		t.Errorf("expected second UnregisterFeed() to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
}

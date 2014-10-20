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
	"encoding/json"
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
	m := NewManager(VERSION, nil, "", "dir", "svr", nil)
	p := m.PIndexPath("x")
	expected := "dir" + string(os.PathSeparator) + "x" + pindexPathSuffix
	if p != expected {
		t.Errorf("wrong pindex path %s, %s", p, expected)
	}
	n, ok := m.ParsePIndexPath(p)
	if !ok || n != "x" {
		t.Errorf("parse pindex path not ok, %v, %v", n, ok)
	}
	n, ok = m.ParsePIndexPath("totally not a pindex path" + pindexPathSuffix)
	if ok {
		t.Errorf("expected not-ok on bad pindex path")
	}
	n, ok = m.ParsePIndexPath("dir" + string(os.PathSeparator) + "not-a-pindex")
	if ok {
		t.Errorf("expected not-ok on bad pindex path")
	}
}

func TestManagerStart(t *testing.T) {
	m := NewManager(VERSION, nil, "", "dir", "not-a-real-svr", nil)
	if m.Start(false) == nil {
		t.Errorf("expected NewManager() with bad svr should fail")
	}
	if m.DataDir() != "dir" {
		t.Errorf("wrong data dir")
	}

	m = NewManager(VERSION, nil, "", "not-a-real-dir", "", nil)
	if m.Start(false) == nil {
		t.Errorf("expected NewManager() with bad dir should fail")
	}
	if m.DataDir() != "not-a-real-dir" {
		t.Errorf("wrong data dir")
	}

	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m = NewManager(VERSION, nil, "", emptyDir, "", nil)
	if err := m.Start(false); err != nil {
		t.Errorf("expected NewManager() with empty dir to work, err: %v", err)
	}

	cfg := NewCfgMem()
	m = NewManager(VERSION, cfg, ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start(false); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas != 0 || nd != nil {
		t.Errorf("expected no node defs wanted")
	}

	cfg = NewCfgMem()
	m = NewManager(VERSION, cfg, ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs known, %v, %d, %v", nd, cas, err)
	}
	nd, cas, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil || cas == 0 || nd == nil {
		t.Errorf("expected node defs wanted")
	}
}

func TestManagerRegisterPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, "", emptyDir, "", meh)
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

	p, err := NewPIndex("p0", "uuid",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("expected NewPIndex() to work")
	}
	defer close(p.Stream)
	px := m.UnregisterPIndex(p.Name)
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
	pc, ok := pindexes[p.Name]
	if !ok || p != pc {
		t.Errorf("wrong pindex in current pindexes")
	}

	px = m.UnregisterPIndex(p.Name)
	if px == nil {
		t.Errorf("expected first UnregisterPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	px = m.UnregisterPIndex(p.Name)
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
	m := NewManager(VERSION, nil, "", emptyDir, "", meh)
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

func TestCheckVersion(t *testing.T) {
	cfg := NewCfgMem()
	ok, err := CheckVersion(cfg, "1.0.0")
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
}

func TestIndexDefs(t *testing.T) {
	d := NewIndexDefs("1.2.3")
	buf, _ := json.Marshal(d)
	d2, err := UnmarshalIndexDefs(buf)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalIndexDefs err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetIndexDefs(cfg)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetIndexDefs on new cfg should be nil")
	}
	cas, err = CfgSetIndexDefs(cfg, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetIndexDefs create on new cfg")
	}
	cas1, err := CfgSetIndexDefs(cfg, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetIndexDefs(cfg, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetIndexDefs recreate")
	}
	d4, cas, err := CfgGetIndexDefs(cfg)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestNodeDefs(t *testing.T) {
	d := NewNodeDefs("1.2.3")
	buf, _ := json.Marshal(d)
	d2, err := UnmarshalNodeDefs(buf)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalNodeDefs err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetNodeDefs on new cfg should be nil")
	}
	cas, err = CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetNodeDefs create on new cfg")
	}
	cas1, err := CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetNodeDefs(cfg, NODE_DEFS_KNOWN, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetNodeDefs recreate")
	}
	d4, cas, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestPlanPIndexes(t *testing.T) {
	d := NewPlanPIndexes("1.2.3")
	buf, _ := json.Marshal(d)
	d2, err := UnmarshalPlanPIndexes(buf)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("UnmarshalPlanPIndexes err or mismatch")
	}

	cfg := NewCfgMem()
	d3, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil || cas != 0 || d3 != nil {
		t.Errorf("CfgGetPlanPIndexes on new cfg should be nil")
	}
	cas, err = CfgSetPlanPIndexes(cfg, d, 100)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetPlanPIndexes create on new cfg")
	}
	cas1, err := CfgSetPlanPIndexes(cfg, d, 0)
	if err != nil || cas1 != 1 {
		t.Errorf("expected ok on first save")
	}
	cas, err = CfgSetPlanPIndexes(cfg, d, 0)
	if err == nil || cas != 0 {
		t.Errorf("expected error on CfgSetPlanPIndexes recreate")
	}
	d4, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil || cas != cas1 ||
		d.UUID != d4.UUID || d.ImplVersion != d4.ImplVersion {
		t.Errorf("expected get to match first save")
	}
}

func TestSamePlanPIndexes(t *testing.T) {
	a := NewPlanPIndexes("0.0.1")
	b := NewPlanPIndexes("0.0.1")
	c := NewPlanPIndexes("0.1.0")

	if !SamePlanPIndexes(a, b) {
		t.Errorf("expected same, a: %v, b: %v", a, b)
	}
	if !SamePlanPIndexes(a, b) {
		t.Errorf("expected same, a: %v, b: %v", a, b)
	}
	if !SamePlanPIndexes(a, c) {
		t.Errorf("expected same, a: %v, c: %v", a, c)
	}
	if !SamePlanPIndexes(c, a) {
		t.Errorf("expected same, a: %v, c: %v", a, c)
	}

	a.PlanPIndexes["foo"] = &PlanPIndex{
		Name: "foo",
	}
	if SamePlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(a, c) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(c, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
}

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
	"reflect"
	"testing"

	log "github.com/couchbaselabs/clog"
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
	m := NewManager(VERSION, nil, NewUUID(), nil, "", "dir", "svr", nil)
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
	m := NewManager(VERSION, nil, NewUUID(), nil, "", "dir", "not-a-real-svr", nil)
	if m.Start(false) == nil {
		t.Errorf("expected NewManager() with bad svr should fail")
	}
	if m.DataDir() != "dir" {
		t.Errorf("wrong data dir")
	}

	m = NewManager(VERSION, nil, NewUUID(), nil, "", "not-a-real-dir", "", nil)
	if m.Start(false) == nil {
		t.Errorf("expected NewManager() with bad dir should fail")
	}
	if m.DataDir() != "not-a-real-dir" {
		t.Errorf("wrong data dir")
	}

	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m = NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", nil)
	if err := m.Start(false); err != nil {
		t.Errorf("expected NewManager() with empty dir to work, err: %v", err)
	}

	cfg := NewCfgMem()
	m = NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
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
	m = NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
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

func TestManagerRestart(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	m := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if err := m.CreateIndex("couchbase", "default", "123",
		"bleve", "foo", ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	close(m.plannerCh)
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to be 1 feed and 1 pindex, got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	for _, pindex := range pindexes {
		pindex.Impl.Close()
	}

	m2 := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	m2.uuid = m.uuid
	if err := m2.Start(true); err != nil {
		t.Errorf("expected reload Manager.Start() to work, err: %v", err)
	}
	close(m2.plannerCh)
	feeds, pindexes = m2.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to load 1 feed and 1 pindex, got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
}

func TestManagerCreateDeleteIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	m := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if err := m.CreateIndex("couchbase", "default", "123",
		"bleve", "foo", ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("couchbase", "default", "123",
		"bleve", "foo", ""); err == nil {
		t.Errorf("expected re-CreateIndex() to fail")
	}
	if err := m.DeleteIndex("not-an-actual-index-name"); err == nil {
		t.Errorf("expected bad DeleteIndex() to fail")
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to be 1 feed and 1 pindex, got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}

	log.Printf("test will now deleting the index: foo")

	if err := m.DeleteIndex("foo"); err != nil {
		t.Errorf("expected DeleteIndex() to work on actual index, err: %v", err)
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes = m.CurrentMaps()
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("expected to be 0 feed and 0 pindex, got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
}

func TestManagerRegisterPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", meh)
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

	p, err := NewPIndex(m, "p0", "uuid", "bleve",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("expected NewPIndex() to work")
	}
	defer close(p.Stream)
	px := m.unregisterPIndex(p.Name)
	if px != nil {
		t.Errorf("expected unregisterPIndex() on newborn manager to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err = m.registerPIndex(p)
	if err != nil {
		t.Errorf("expected first registerPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnRegisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	err = m.registerPIndex(p)
	if err == nil {
		t.Errorf("expected second registerPIndex() to fail")
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

	px = m.unregisterPIndex(p.Name)
	if px == nil {
		t.Errorf("expected first unregisterPIndex() to work")
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("expected OnRegisterPIndex callback to meh")
	}
	meh.lastPIndex = nil
	meh.lastCall = ""

	px = m.unregisterPIndex(p.Name)
	if px != nil {
		t.Errorf("expected second unregisterPIndex() to fail")
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
	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", meh)
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
	fx := m.unregisterFeed(f.Name())
	if fx != nil {
		t.Errorf("expected unregisterFeed() on newborn manager to fail")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err := m.registerFeed(f)
	if err != nil {
		t.Errorf("expected first registerFeed() to work")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	err = m.registerFeed(f)
	if err == nil {
		t.Errorf("expected second registerFeed() to fail")
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

	fx = m.unregisterFeed(f.Name())
	if fx == nil {
		t.Errorf("expected first unregisterFeed() to work")
	}
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}

	fx = m.unregisterFeed(f.Name())
	if fx != nil {
		t.Errorf("expected second unregisterFeed() to fail")
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
	d2 := &IndexDefs{}
	err := json.Unmarshal(buf, d2)
	if err != nil || d.UUID != d2.UUID || d.ImplVersion != d2.ImplVersion {
		t.Errorf("Unmarshal IndexDefs err or mismatch")
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
	d2 := &NodeDefs{}
	err := json.Unmarshal(buf, d2)
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

	if SubsetPlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if !SubsetPlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}

	b.PlanPIndexes["foo"] = &PlanPIndex{
		Name:      "foo",
		IndexName: "differnet-than-foo-in-a",
	}

	if SamePlanPIndexes(a, b) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
	if SamePlanPIndexes(b, a) {
		t.Errorf("expected not same, a: %v, b: %v", a, b)
	}
}

func TestSamePlanPIndex(t *testing.T) {
	ppi0 := &PlanPIndex{
		Name:             "0",
		UUID:             "x",
		IndexName:        "x",
		IndexUUID:        "x",
		IndexMapping:     "x",
		SourceType:       "x",
		SourceName:       "x",
		SourceUUID:       "x",
		SourcePartitions: "x",
		NodeUUIDs:        make(map[string]string),
	}
	ppi1 := &PlanPIndex{
		Name:             "1",
		UUID:             "x",
		IndexName:        "x",
		IndexUUID:        "x",
		IndexMapping:     "x",
		SourceType:       "x",
		SourceName:       "x",
		SourceUUID:       "x",
		SourcePartitions: "x",
		NodeUUIDs:        make(map[string]string),
	}

	if !SamePlanPIndex(ppi0, ppi0) {
		t.Errorf("expected SamePlanPindex to be true")
	}
	if SamePlanPIndex(ppi0, ppi1) {
		t.Errorf("expected SamePlanPindex to be false")
	}
	if SamePlanPIndex(ppi1, ppi0) {
		t.Errorf("expected SamePlanPindex to be false")
	}
}

func TestCfgGetHelpers(t *testing.T) {
	errCfg := &ErrorOnlyCfg{}

	if _, err := CheckVersion(errCfg, "my-version"); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetIndexDefs(errCfg); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetNodeDefs(errCfg, NODE_DEFS_KNOWN); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
	if _, _, err := CfgGetPlanPIndexes(errCfg); err == nil {
		t.Errorf("expected to fail with errCfg")
	}
}

func TestManagerStartNILFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	err := mgr.startFeedByType("feedName", "indexName", "indexUUID", "nil",
		"sourceName", "sourceUUID", nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for nil sourceType")
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*NILFeed); !ok {
		t.Errorf("expected a NILFeed")
	}
}

func TestManagerTags(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, ":1000",
		emptyDir, "some-datasource", nil)
	tm := mgr.Tags()
	if tm != nil {
		t.Errorf("expected nil Tags()")
	}

	mgr = NewManager(VERSION, cfg, NewUUID(), []string{"a", "b"}, ":1000",
		emptyDir, "some-datasource", nil)
	tm = mgr.Tags()
	te := map[string]bool{}
	te["a"] = true
	te["b"] = true
	if tm == nil || !reflect.DeepEqual(tm, te) {
		t.Errorf("expected equal Tags()")
	}
}

func TestManagerClosePIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}
	m.Start(true)
	p, err := NewPIndex(m, "p0", "uuid", "bleve",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", "sourcePartitions",
		m.PIndexPath("p0"))
	m.registerPIndex(p)
	feeds, pindexes := m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 1 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	err = m.ClosePIndex(p)
	if err != nil {
		t.Errorf("expected ClosePIndex() to work")
	}
	feeds, pindexes = m.CurrentMaps()
	if feeds == nil || pindexes == nil {
		t.Errorf("expected current feeds & pindexes to be non-nil")
	}
	if len(feeds) != 0 || len(pindexes) != 0 {
		t.Errorf("wrong counts for current feeds (%d) & pindexes (%d)",
			len(feeds), len(pindexes))
	}
	if meh.lastPIndex != p || meh.lastCall != "OnUnregisterPIndex" {
		t.Errorf("meh callbacks were wrong")
	}
}

func TestManagerStrangeWorkReqs(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	meh := &TestMEH{}
	m := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", meh)
	if err := m.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if err := m.CreateIndex("simple", "sourceName", "sourceUUID",
		"bleve", "foo", ""); err != nil {
		t.Errorf("expected simple CreateIndex() to work")
	}
	if err := SyncWorkReq(m.plannerCh, "whoa-this-isn't-a-valid-op",
		"test", nil); err == nil {
		t.Errorf("expected error on weird work req to planner")
	}
	if err := SyncWorkReq(m.janitorCh, "whoa-this-isn't-a-valid-op",
		"test", nil); err == nil {
		t.Errorf("expected error on weird work req to janitor")
	}
}

func TestManagerCreateSimpleFeed(t *testing.T) {
	testManagerSimpleFeed(t, func(mgr *Manager, sf *SimpleFeed) {
		err := sf.Close()
		if err != nil {
			t.Errorf("expected simple feed close to work")
		}
	})
}

func TestManagerSimpleFeedCloseSource(t *testing.T) {
	testManagerSimpleFeed(t, func(mgr *Manager, sf *SimpleFeed) {
		close(sf.Source())
		err := sf.Close()
		if err != nil {
			t.Errorf("expected simple feed close after source close to work")
		}
	})
}

func testManagerSimpleFeed(t *testing.T, andThen func(*Manager, *SimpleFeed)) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	meh := &TestMEH{}
	m := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", meh)
	if err := m.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if err := m.CreateIndex("simple", "sourceName", "sourceUUID",
		"bleve", "foo", ""); err != nil {
		t.Errorf("expected simple CreateIndex() to work")
	}
	m.PlannerNOOP("test")
	m.JanitorNOOP("test")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to be 1 feed and 1 pindex, got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	if meh.lastPIndex == nil {
		t.Errorf("expected to be meh.lastPIndex")
	}
	feedName := FeedName(meh.lastPIndex)
	feed, exists := feeds[feedName]
	if !exists || feed == nil {
		t.Errorf("expected there to be feed: %s", feedName)
	}
	sf, ok := feed.(*SimpleFeed)
	if !ok || sf == nil {
		t.Errorf("expected feed to be simple")
	}
	if sf.Source() == nil {
		t.Errorf("expected simple feed source to be there")
	}
	if sf.Streams() == nil {
		t.Errorf("expected simple feed streams to be there")
	}
	andThen(m, sf)
}

func TestPIndexMatchesPlan(t *testing.T) {
	plan := &PlanPIndex{
		Name: "hi",
		UUID: "111",
	}
	px := &PIndex{
		Name: "hi",
		UUID: "222",
	}
	py := &PIndex{
		Name: "hello",
		UUID: "111",
	}
	if PIndexMatchesPlan(px, plan) == false {
		t.Errorf("expected pindex to match the plan")
	}
	if PIndexMatchesPlan(py, plan) == true {
		t.Errorf("expected pindex to not match the plan")
	}
}

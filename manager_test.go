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
	"runtime"
	"strings"
	"testing"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"
)

// Implements ManagerEventHandlers interface.
type TestMEH struct {
	lastPIndex *PIndex
	lastCall   string
	ch         chan bool
}

func (meh *TestMEH) OnRegisterPIndex(pindex *PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnRegisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func (meh *TestMEH) OnUnregisterPIndex(pindex *PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnUnregisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
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
	sourceParams := ""
	if err := m.CreateIndex("dest", "default", "123", sourceParams,
		"bleve", "foo", "", PlanParams{}); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	m.Kick("test0")
	m.PlannerNOOP("test0")
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
	m2.Kick("test2")
	m2.PlannerNOOP("test2")
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
	sourceParams := ""
	if err := m.CreateIndex("dest", "default", "123", sourceParams,
		"bleve", "foo", "", PlanParams{}); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("dest", "default", "123", sourceParams,
		"bleve", "foo", "", PlanParams{}); err == nil {
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

	log.Printf("test will now be deleting the index: foo")

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

	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "bleve",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", sourceParams, "sourcePartitions",
		m.PIndexPath("p0"))
	if err != nil {
		t.Errorf("expected NewPIndex() to work")
	}
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
	d2 := &PlanPIndexes{}
	err := json.Unmarshal(buf, d2)
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
		IndexSchema:      "x",
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
		IndexSchema:      "x",
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

func TestManagerStartDCPFeed(t *testing.T) {
	testManagerStartDCPFeed(t, "couchbase")
	testManagerStartDCPFeed(t, "couchbase-dcp")
}

func testManagerStartDCPFeed(t *testing.T, sourceType string) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	err := mgr.startFeedByType("feedName", "indexName", "indexUUID", sourceType,
		"sourceName", "sourceUUID", sourceParams, nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for sourceType: %s, err: %v",
			sourceType, err)
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*DCPFeed); !ok {
		t.Errorf("expected a DCPFeed")
	}
	err = mgr.startFeedByType("feedName", "indexName", "indexUUID", sourceType,
		"sourceName", "sourceUUID", sourceParams, nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
	err = mgr.startFeedByType("feedName2", "indexName2", "indexUUID2", sourceType,
		"sourceName2", "sourceUUID2", "NOT-VALID-JSON-sourceParams", nil)
	if err == nil {
		t.Errorf("expected startFeedByType to fail on non-json sourceParams")
	}
}

func TestManagerStartTAPFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	err := mgr.startFeedByType("feedName", "indexName", "indexUUID", "couchbase-tap",
		"sourceName", "sourceUUID", sourceParams, nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for simple sourceType")
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*TAPFeed); !ok {
		t.Errorf("expected a TAPFeed")
	}
	err = mgr.startFeedByType("feedName", "indexName", "indexUUID", "couchbase-tap",
		"sourceName", "sourceUUID", sourceParams, nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
	err = mgr.startFeedByType("feedName2", "indexName2", "indexUUID2", "couchbase-tap",
		"sourceName2", "sourceUUID2", "NOT-VALID-JSON-sourceParams", nil)
	if err == nil {
		t.Errorf("expected startFeedByType to fail on non-json sourceParams")
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
		"sourceName", "sourceUUID", "sourceParams", nil)
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
	err = mgr.startFeedByType("feedName", "indexName", "indexUUID", "nil",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
	}
}

func TestManagerStartSimpleFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", nil)
	if err := mgr.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	err := mgr.startFeedByType("feedName", "indexName", "indexUUID", "dest",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err != nil {
		t.Errorf("expected startFeedByType ok for simple sourceType")
	}
	currFeeds, _ := mgr.CurrentMaps()
	if len(currFeeds) != 1 {
		t.Errorf("len currFeeds != 1")
	}
	f, exists := currFeeds["feedName"]
	if !exists || f.Name() != "feedName" {
		t.Errorf("expected a feed")
	}
	if _, ok := f.(*DestFeed); !ok {
		t.Errorf("expected a SimpleFeed")
	}
	err = mgr.startFeedByType("feedName", "indexName", "indexUUID", "dest",
		"sourceName", "sourceUUID", "sourceParams", nil)
	if err == nil {
		t.Errorf("expected re-startFeedByType to fail")
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
	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "bleve",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", sourceParams, "sourcePartitions",
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

func TestManagerRemovePIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", meh)
	if meh.lastPIndex != nil || meh.lastCall != "" {
		t.Errorf("expected no callback events to meh")
	}
	m.Start(true)
	sourceParams := ""
	p, err := NewPIndex(m, "p0", "uuid", "bleve",
		"indexName", "indexUUID", "",
		"sourceType", "sourceName", "sourceUUID", sourceParams, "sourcePartitions",
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
	err = m.RemovePIndex(p)
	if err != nil {
		t.Errorf("expected RemovePIndex() to work")
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
	sourceParams := ""
	if err := m.CreateIndex("dest", "sourceName", "sourceUUID", sourceParams,
		"bleve", "foo", "", PlanParams{}); err != nil {
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

func TestManagerStartFeedByType(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", nil)
	err := m.startFeedByType("feedName", "indexName", "indexUUID",
		"sourceType-is-unknown", "sourceName", "sourceUUID", "sourceParams", nil)
	if err == nil {
		t.Errorf("expected err on unknown source type")
	}
}

func TestManagerStartPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", nil)
	err := m.startPIndex(&PlanPIndex{IndexType: "unknown-index-type"})
	if err == nil {
		t.Errorf("expected err on unknown index type")
	}
	err = m.startPIndex(&PlanPIndex{IndexType: "bleve", IndexName: "a"})
	if err != nil {
		t.Errorf("expected new bleve pindex to work, err: %v", err)
	}
}

func TestManagerReStartPIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	meh := &TestMEH{}
	m := NewManager(VERSION, nil, NewUUID(), nil, "", emptyDir, "", meh)

	err := m.startPIndex(&PlanPIndex{Name: "p", IndexType: "bleve", IndexName: "i"})
	if err != nil {
		t.Errorf("expected first start to work")
	}
	err = m.stopPIndex(meh.lastPIndex, true)
	if err != nil {
		t.Errorf("expected close pindex to work")
	}
	err = m.startPIndex(&PlanPIndex{Name: "p", IndexType: "bleve", IndexName: "i"})
	if err != nil {
		t.Errorf("expected close+restart pindex to work")
	}
}

func testManagerSimpleFeed(t *testing.T,
	sourceParams string, planParams PlanParams,
	andThen func(*Manager, *DestFeed, *TestMEH)) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)
	cfg := NewCfgMem()
	meh := &TestMEH{}
	m := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", meh)
	if err := m.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	if err := m.CreateIndex("dest", "sourceName", "sourceUUID", sourceParams,
		"bleve", "foo", "", planParams); err != nil {
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
	sf, ok := feed.(*DestFeed)
	if !ok || sf == nil {
		t.Errorf("expected feed to be simple")
	}
	if sf.Dests() == nil {
		t.Errorf("expected simple feed dests to be there")
	}
	andThen(m, sf, meh)
}

func TestManagerCreateSimpleFeed(t *testing.T) {
	sourceParams := ""
	testManagerSimpleFeed(t, sourceParams, PlanParams{},
		func(mgr *Manager, sf *DestFeed, meh *TestMEH) {
			err := sf.Close()
			if err != nil {
				t.Errorf("expected simple feed close to work")
			}
		})
}

func TestBasicStreamMutations(t *testing.T) {
	sourceParams := ""
	testManagerSimpleFeed(t, sourceParams, PlanParams{},
		func(mgr *Manager, sf *DestFeed, meh *TestMEH) {
			pindex := meh.lastPIndex
			bindex, ok := pindex.Impl.(bleve.Index)
			if !ok || bindex == nil {
				t.Errorf("expected bleve.Index")
			}
			if len(sf.Dests()) != 1 {
				t.Errorf("expected just 1 dest")
			}

			partition := ""
			key := []byte("hello")
			seq := uint64(0)
			val := []byte("{}")
			err := sf.OnDataUpdate(partition, key, seq, val)
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}

			key = []byte("goodbye")
			val = []byte("{}")
			err = sf.OnDataUpdate(partition, key, seq, val)
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}

			n, err := bindex.DocCount()
			if n != 2 {
				t.Errorf("expected 2 docs in bindex, got: %d", n)
			}

			key = []byte("goodbye")
			err = sf.OnDataDelete(partition, key, seq)
			if err != nil {
				t.Errorf("expected no error to DELETE, err: %v", err)
			}

			n, err = bindex.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex, got: %d", n)
			}

			mehCh := make(chan bool, 10)
			meh.ch = mehCh
			err = sf.Rollback(partition, seq)
			if err != nil {
				t.Errorf("expected no error to ROLLBACK, err: %v", err)
			}

			runtime.Gosched()
			<-mehCh
			mgr.Kick("after-rollback")
			mgr.PlannerNOOP("after-rollback")
			mgr.JanitorNOOP("after-rollback")
			runtime.Gosched()
			<-mehCh
			feeds, pindexes := mgr.CurrentMaps()
			if len(feeds) != 1 || len(pindexes) != 1 {
				t.Errorf("expected to be 1 feed and 1 pindex, got feeds: %+v, pindexes: %+v",
					feeds, pindexes)
			}
			var pindex2 *PIndex
			for _, p := range pindexes {
				pindex2 = p
			}
			if pindex == pindex2 {
				t.Errorf("expected new pindex to be re-built")
			}
			bindex2 := pindex2.Impl.(bleve.Index)
			if bindex == bindex2 {
				t.Errorf("expected new bindex to be re-built")
			}
			n, err = bindex2.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex2, got: %d", n)
			}
		})
}

func TestStreamGetSetMeta(t *testing.T) {
	sourceParams := ""
	testManagerSimpleFeed(t, sourceParams, PlanParams{},
		func(mgr *Manager, sf *DestFeed, meh *TestMEH) {
			pindex := meh.lastPIndex
			bindex, ok := pindex.Impl.(bleve.Index)
			if !ok || bindex == nil {
				t.Errorf("expected bleve.Index")
			}
			if len(sf.Dests()) != 1 {
				t.Errorf("expected just 1 dest")
			}

			partition := "0"
			v, lastSeq, err := sf.GetOpaque(partition)
			if err != nil {
				t.Errorf("expected no error to get, err: %v", err)
			}
			if v != nil {
				t.Errorf("expected []byte{nil} for get, got: %s", v)
			}
			n, err := bindex.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex, got: %d", n)
			}

			err = sf.SetOpaque(partition, []byte("dinner"))
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}
			n, err = bindex.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex after set, got: %d", n)
			}

			v, lastSeq, err = sf.GetOpaque(partition)
			if err != nil {
				t.Errorf("expected no error to get, err: %v", err)
			}
			if string(v) != "dinner" {
				t.Errorf("expected dinner")
			}
			if lastSeq != 0 {
				t.Errorf("expected 0 lastSeq")
			}
			n, err = bindex.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex after set, got: %d", n)
			}
		})
}

func TestMultiStreamGetSetMeta(t *testing.T) {
	sourceParams := "{\"numPartitions\":2}"
	testManagerSimpleFeed(t, sourceParams, PlanParams{},
		func(mgr *Manager, sf *DestFeed, meh *TestMEH) {
			pindex := meh.lastPIndex
			bindex, ok := pindex.Impl.(bleve.Index)
			if !ok || bindex == nil {
				t.Errorf("expected bleve.Index")
			}
			if len(sf.Dests()) != 2 {
				t.Errorf("expected 2 entries in dests")
			}

			partition := "0"
			v, lastSeq, err := sf.GetOpaque(partition)
			if err != nil {
				t.Errorf("expected no error to get, err: %v", err)
			}
			if v != nil {
				t.Errorf("expected nil []byte, got: %#v", v)
			}
			n, err := bindex.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex, got: %d", n)
			}

			err = sf.SetOpaque(partition, []byte("dinner"))
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}
			n, err = bindex.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex after set, got: %d", n)
			}

			v, lastSeq, err = sf.GetOpaque(partition)
			if err != nil {
				t.Errorf("expected no error to get, err: %v", err)
			}
			if string(v) != "dinner" {
				t.Errorf("expected dinner, got: %s", v)
			}
			if lastSeq != 0 {
				t.Errorf("expected 0 lastSeq")
			}
			n, err = bindex.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex after set, got: %d", n)
			}
		})
}

func testPartitioning(t *testing.T,
	sourceParams string,
	planParams PlanParams,
	expectedNumPIndexes int,
	expectedNumDests int,
	andThen func(mgr *Manager, sf *DestFeed, pindexes map[string]*PIndex)) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, ":1000", emptyDir, "some-datasource", meh)
	if err := mgr.Start(true); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}

	if err := mgr.CreateIndex("dest", "sourceName", "sourceUUID", sourceParams,
		"bleve", "foo", "", planParams); err != nil {
		t.Errorf("expected CreateIndex() to work")
	}

	mgr.PlannerNOOP("test")
	mgr.JanitorNOOP("test")
	feeds, pindexes := mgr.CurrentMaps()
	if len(feeds) != 1 {
		t.Errorf("expected to be 1 feed, got feeds: %+v", feeds)
	}
	if len(pindexes) != expectedNumPIndexes {
		t.Errorf("expected to be %d pindex, got pindexes: %+v",
			expectedNumPIndexes, pindexes)
	}
	var feed Feed
	for _, f := range feeds {
		feed = f
	}
	sf, ok := feed.(*DestFeed)
	if !ok || sf == nil {
		t.Errorf("expected feed to be simple")
	}
	if len(sf.Dests()) != expectedNumDests {
		t.Errorf("expected %d dests", expectedNumDests)
	}

	if andThen != nil {
		andThen(mgr, sf, pindexes)
	}
}

func TestPartitioning(t *testing.T) {
	sourceParams := "{\"numPartitions\":2}"
	planParams := PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	expectedNumPIndexes := 2
	expectedNumStreams := 2
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams, nil)

	sourceParams = "{\"numPartitions\":10}"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	expectedNumPIndexes = 10
	expectedNumStreams = 10
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams, nil)

	sourceParams = "{\"numPartitions\":5}"
	planParams = PlanParams{
		MaxPartitionsPerPIndex: 2,
	}
	expectedNumPIndexes = 3
	expectedNumStreams = 5
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams, nil)
}

func TestPartitioningMutations(t *testing.T) {
	sourceParams := "{\"numPartitions\":2}"
	planParams := PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	expectedNumPIndexes := 2
	expectedNumStreams := 2
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams,
		func(mgr *Manager, sf *DestFeed, pindexes map[string]*PIndex) {
			var pindex0 *PIndex
			var pindex1 *PIndex
			for _, pindex := range pindexes {
				if pindex.SourcePartitions == "0" {
					pindex0 = pindex
				}
				if pindex.SourcePartitions == "1" {
					pindex1 = pindex
				}
			}
			if pindex0 == nil {
				t.Errorf("expected pindex0")
			}
			if pindex1 == nil {
				t.Errorf("expected pindex1")
			}
			bindex0, ok := pindex0.Impl.(bleve.Index)
			if !ok || bindex0 == nil {
				t.Errorf("expected bleve.Index")
			}
			bindex1, ok := pindex1.Impl.(bleve.Index)
			if !ok || bindex1 == nil {
				t.Errorf("expected bleve.Index")
			}
			n, err := bindex0.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex1, got: %d", n)
			}

			partition := "0"
			key := []byte("hello")
			seq := uint64(0)
			val := []byte("{}")
			err = sf.OnDataUpdate(partition, key, seq, val)
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}
			n, err = bindex0.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex1, got: %d", n)
			}
		})
}

func TestFanInPartitioningMutations(t *testing.T) {
	sourceParams := "{\"numPartitions\":3}"
	planParams := PlanParams{
		MaxPartitionsPerPIndex: 2,
	}
	expectedNumPIndexes := 2
	expectedNumStreamsEntries := 3
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreamsEntries,
		func(mgr *Manager, sf *DestFeed, pindexes map[string]*PIndex) {
			var pindex0_0 *PIndex
			var pindex0_1 *PIndex
			var pindex1 *PIndex
			for _, pindex := range pindexes {
				if strings.Contains(pindex.SourcePartitions, "0") {
					pindex0_0 = pindex
				}
				if strings.Contains(pindex.SourcePartitions, "1") {
					pindex0_1 = pindex
				}
				if pindex.SourcePartitions == "2" {
					pindex1 = pindex
				}
			}
			if pindex0_0 == nil || pindex0_1 == nil {
				t.Errorf("expected pindex0_0/1")
			}
			if pindex0_0 != pindex0_1 {
				t.Errorf("expected pindex0 equality")
			}
			if pindex1 == nil {
				t.Errorf("expected pindex1")
			}
			bindex0, ok := pindex0_0.Impl.(bleve.Index)
			if !ok || bindex0 == nil {
				t.Errorf("expected bleve.Index")
			}
			bindex1, ok := pindex1.Impl.(bleve.Index)
			if !ok || bindex1 == nil {
				t.Errorf("expected bleve.Index")
			}
			n, err := bindex0.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex1, got: %d", n)
			}

			partition := "0"
			key := []byte("hello")
			seq := uint64(0)
			val := []byte("{}")
			err = sf.OnDataUpdate(partition, key, seq, val)
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}
			n, err = bindex0.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex1, got: %d", n)
			}

			partition = "2"
			key = []byte("hi")
			val = []byte("{}")
			err = sf.OnDataUpdate(partition, key, seq, val)
			if err != nil {
				t.Errorf("expected no error to update, err: %v", err)
			}
			n, err = bindex0.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex1, got: %d", n)
			}

			err = sf.Rollback("1", 0)
			if err != nil {
				t.Errorf("expected no error to rollback, err: %v", err)
			}
			runtime.Gosched()
			mgr.PlannerNOOP("after-rollback")
			mgr.JanitorNOOP("after-rollback")
			runtime.Gosched()
			mgr.PlannerNOOP("after-rollback")
			mgr.JanitorNOOP("after-rollback")
			feeds, pindexes := mgr.CurrentMaps()
			if len(feeds) != 1 {
				t.Errorf("expected to be 1 feed, got feeds: %+v", feeds)
			}
			if len(pindexes) != 2 {
				t.Errorf("expected to be %d pindex, got pindexes: %+v",
					2, pindexes)
			}
			for _, pindex := range pindexes {
				if strings.Contains(pindex.SourcePartitions, "0") {
					pindex0_0 = pindex
				}
				if strings.Contains(pindex.SourcePartitions, "1") {
					pindex0_1 = pindex
				}
				if pindex.SourcePartitions == "2" {
					pindex1 = pindex
				}
			}
			if pindex0_0 == nil || pindex0_1 == nil {
				t.Errorf("expected pindex0_0/1")
			}
			if pindex0_0 != pindex0_1 {
				t.Errorf("expected pindex0 equality")
			}
			if pindex1 == nil {
				t.Errorf("expected pindex1")
			}
			bindex0, ok = pindex0_0.Impl.(bleve.Index)
			if !ok || bindex0 == nil {
				t.Errorf("expected bleve.Index")
			}
			bindex1, ok = pindex1.Impl.(bleve.Index)
			if !ok || bindex1 == nil {
				t.Errorf("expected bleve.Index")
			}
			n, err = bindex0.DocCount()
			if n != 0 {
				t.Errorf("expected 0 docs in bindex0 after rollback, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex1 after rollback, got: %d", n)
			}
		})
}

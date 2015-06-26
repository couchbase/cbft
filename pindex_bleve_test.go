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
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/blevesearch/bleve"

	"github.com/couchbaselabs/cbgt"
)

func TestManagerRestart(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	m := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"bleve", "foo", "", cbgt.PlanParams{},
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err" +
			" on attempted create-with-prevIndexUUID")
	}
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"bleve", "foo", "", cbgt.PlanParams{},
		""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"bleve", "foo", "", cbgt.PlanParams{},
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err on update" +
			" with wrong prevIndexUUID")
	}
	m.Kick("test0")
	m.PlannerNOOP("test0")
	feeds, pindexes := m.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to be 1 feed and 1 pindex,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
	for _, pindex := range pindexes {
		pindex.Dest.Close()
		if m.GetPIndex(pindex.Name) != pindex {
			t.Errorf("expected GetPIndex() to match")
		}
	}

	m2 := cbgt.NewManager(cbgt.VERSION, cfg, m.UUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m2.Start("wanted"); err != nil {
		t.Errorf("expected reload Manager.Start() to work, err: %v", err)
	}
	m2.Kick("test2")
	m2.PlannerNOOP("test2")
	feeds, pindexes = m2.CurrentMaps()
	if len(feeds) != 1 || len(pindexes) != 1 {
		t.Errorf("expected to load 1 feed and 1 pindex,"+
			" got feeds: %+v, pindexes: %+v",
			feeds, pindexes)
	}
}

func testPartitioning(t *testing.T,
	sourceParams string,
	planParams cbgt.PlanParams,
	expectedNumPIndexes int,
	expectedNumDests int,
	andThen func(mgr *cbgt.Manager,
		sf *cbgt.PrimaryFeed, pindexes map[string]*cbgt.PIndex)) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	if err := mgr.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}

	if err := mgr.CreateIndex("primary",
		"sourceName", "sourceUUID", sourceParams,
		"bleve", "foo", "", planParams, ""); err != nil {
		t.Errorf("expected CreateIndex() to work")
	}

	mgr.Kick("test")
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
	var feed cbgt.Feed
	for _, f := range feeds {
		feed = f
	}
	sf, ok := feed.(*cbgt.PrimaryFeed)
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
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	expectedNumPIndexes := 2
	expectedNumStreams := 2
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams, nil)

	sourceParams = "{\"numPartitions\":10}"
	planParams = cbgt.PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	expectedNumPIndexes = 10
	expectedNumStreams = 10
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams, nil)

	sourceParams = "{\"numPartitions\":5}"
	planParams = cbgt.PlanParams{
		MaxPartitionsPerPIndex: 2,
	}
	expectedNumPIndexes = 3
	expectedNumStreams = 5
	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams, nil)
}

func TestPartitioningMutations(t *testing.T) {
	sourceParams := "{\"numPartitions\":2}"
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 1,
	}
	expectedNumPIndexes := 2
	expectedNumStreams := 2

	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreams,
		func(mgr *cbgt.Manager, sf *cbgt.PrimaryFeed,
			pindexes map[string]*cbgt.PIndex) {
			var pindex0 *cbgt.PIndex
			var pindex1 *cbgt.PIndex
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
			err = sf.DataUpdate(partition, key, seq, val,
				0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 2,
	}
	expectedNumPIndexes := 2
	expectedNumStreamsEntries := 3

	testPartitioning(t, sourceParams, planParams,
		expectedNumPIndexes, expectedNumStreamsEntries,
		func(mgr *cbgt.Manager, sf *cbgt.PrimaryFeed,
			pindexes map[string]*cbgt.PIndex) {
			var pindex0_0 *cbgt.PIndex
			var pindex0_1 *cbgt.PIndex
			var pindex1 *cbgt.PIndex
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
			err = sf.DataUpdate(partition, key, seq, val,
				0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
			err = sf.DataUpdate(partition, key, seq, val,
				0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
			mgr.Kick("after-rollback")
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
				t.Errorf("expected 0 docs in bindex0 after rollback,"+
					" got: %d", n)
			}
			n, err = bindex1.DocCount()
			if n != 1 {
				t.Errorf("expected 1 docs in bindex1 after rollback,"+
					" got: %d", n)
			}
		})
}

func TestManagerIndexControl(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	m := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"bleve", "foo", "", cbgt.PlanParams{}, ""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	m.Kick("test0")
	m.PlannerNOOP("test0")

	err := m.IndexControl("foo", "wrong-uuid", "", "", "")
	if err == nil {
		t.Errorf("expected err on wrong UUID")
	}

	indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
	npp := indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp != nil {
		t.Errorf("expected nil npp")
	}

	err = m.IndexControl("foo", "", "", "", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] != nil {
		t.Errorf("expected nil npp.sub")
	}

	err = m.IndexControl("foo", "", "disallow", "", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] == nil {
		t.Errorf("expected npp.sub")
	}
	if npp[""].CanRead {
		t.Errorf("expected CanRead false")
	}
	if !npp[""].CanWrite {
		t.Errorf("expected CanWrite")
	}

	err = m.IndexControl("foo", "", "", "", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] == nil {
		t.Errorf("expected npp.sub")
	}
	if npp[""].CanRead {
		t.Errorf("expected CanRead false")
	}
	if !npp[""].CanWrite {
		t.Errorf("expected CanWrite")
	}

	err = m.IndexControl("foo", "", "", "pause", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] == nil {
		t.Errorf("expected npp.sub")
	}
	if npp[""].CanRead {
		t.Errorf("expected CanRead false")
	}
	if npp[""].CanWrite {
		t.Errorf("expected CanWrite false")
	}

	err = m.IndexControl("foo", "", "", "", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] == nil {
		t.Errorf("expected npp.sub")
	}
	if npp[""].CanRead {
		t.Errorf("expected CanRead false")
	}
	if npp[""].CanWrite {
		t.Errorf("expected CanWrite false")
	}

	err = m.IndexControl("foo", "", "", "resume", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] == nil {
		t.Errorf("expected npp.sub")
	}
	if npp[""].CanRead {
		t.Errorf("expected CanRead false")
	}
	if !npp[""].CanWrite {
		t.Errorf("expected CanWrite")
	}

	err = m.IndexControl("foo", "", "allow", "resume", "")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	npp = indexDefs.IndexDefs["foo"].PlanParams.NodePlanParams[""]
	if npp == nil {
		t.Errorf("expected npp")
	}
	if npp[""] != nil {
		t.Errorf("expected nil npp.sub")
	}

	if indexDefs.IndexDefs["foo"].PlanParams.PlanFrozen {
		t.Errorf("expected not yet frozen")
	}
	err = m.IndexControl("foo", "", "", "", "freeze")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	if !indexDefs.IndexDefs["foo"].PlanParams.PlanFrozen {
		t.Errorf("expected frozen")
	}

	err = m.IndexControl("foo", "", "", "", "unfreeze")
	if err != nil {
		t.Errorf("expected ok")
	}
	indexDefs, _, _ = cbgt.CfgGetIndexDefs(cfg)
	if indexDefs.IndexDefs["foo"].PlanParams.PlanFrozen {
		t.Errorf("expected not frozen")
	}
}

func TestNewPIndexEmptyBleveJSON(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := cbgt.NewPIndex(nil, "fake", "uuid",
		"bleve", "indexName", "indexUUID", "{}",
		"sourceType", "sourceName", "sourceUUID",
		"sourceParams", "sourcePartitions",
		cbgt.PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to fail with empty json map")
	}
}

func TestNewPIndexBleveBadMapping(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := cbgt.NewPIndex(nil, "fake", "uuid",
		"bleve", "indexName", "indexUUID", "} hey this isn't json :-(",
		"sourceType", "sourceName", "sourceUUID",
		"sourceParams", "sourcePartitions",
		cbgt.PIndexPath(emptyDir, "fake"))
	if pindex != nil || err == nil {
		t.Errorf("expected NewPIndex to fail with bad json")
	}
}

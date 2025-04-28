//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/couchbase/cbgt"
)

func WaitForPersistence(pindex *cbgt.PIndex, docCount float64) error {
	for i := 0; i < 100; i++ {
		stats := map[string]interface{}{
			"doc_count":           float64(0),
			"num_recs_to_persist": float64(0),
		}
		err := addPIndexStats(pindex, stats, nil, true)
		if err != nil {
			return fmt.Errorf("expected nil addPIndexStats err, got: %v", err)
		}
		dv, ok := stats["doc_count"]
		if ok {
			dc, ok := dv.(float64)
			if ok && dc == docCount {
				v, ok1 := stats["num_recs_to_persist"]
				if ok1 {
					nrtp, ok1 := v.(float64)
					if ok1 && nrtp <= 0.0 {
						return nil
					}
				}
			}

		}

		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("persistence took too long!")
}

func TestManagerRestart(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	cbgt.DataSourceUUID = func(sourceType, sourceName, sourceParams, server string,
		options map[string]string) (string, error) {
		return "123", nil
	}

	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer func() {
		cbgt.DataSourceUUID = prevDataSourceUUID
		os.RemoveAll(emptyDir)
	}()

	cfg := cbgt.NewCfgMem()
	m := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"fulltext-index", "foo", "", cbgt.PlanParams{},
		"bad-prevIndexUUID"); err == nil {
		t.Errorf("expected CreateIndex() err" +
			" on attempted create-with-prevIndexUUID")
	}
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"fulltext-index", "foo", "", cbgt.PlanParams{},
		""); err != nil {
		t.Errorf("expected CreateIndex() to work, err: %v", err)
	}
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"fulltext-index", "foo", "", cbgt.PlanParams{},
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
		pindex.Dest.Close(false)
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
	attempts := 0
	for {
		feeds, pindexes = m2.CurrentMaps()
		if len(feeds) == 1 && len(pindexes) == 1 {
			break
		}
		attempts++
		if attempts > 10 {
			t.Errorf("expected to load 1 feed and 1 pindex,"+
				" got feeds: %+v, pindexes: %+v",
				feeds, pindexes)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func testPartitioning(t *testing.T,
	sourceParams string,
	planParams cbgt.PlanParams,
	expectedNumPIndexes int,
	expectedNumDests int,
	andThen func(mgr *cbgt.Manager,
		sf *cbgt.PrimaryFeed, pindexes map[string]*cbgt.PIndex)) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	cbgt.DataSourceUUID = func(sourceType, sourceName, sourceParams, server string,
		options map[string]string) (string, error) {
		return "sourceUUID", nil
	}

	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer func() {
		cbgt.DataSourceUUID = prevDataSourceUUID
		os.RemoveAll(emptyDir)
	}()

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	if err := mgr.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}

	if err := mgr.CreateIndex("primary",
		"sourceName", "sourceUUID", sourceParams,
		"fulltext-index", "foo", "", planParams, ""); err != nil {
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
		t.Fatalf("expected to be %d pindex, got pindexes: %+v",
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
				t.Fatalf("expected pindex0")
			}
			if pindex1 == nil {
				t.Fatalf("expected pindex1")
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
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
			if n != 0 {
				t.Errorf("expected 0 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
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
			WaitForPersistence(pindex0, float64(1))
			n, err = bindex0.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
			if n != 1 {
				t.Errorf("expected 1 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
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
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
			if n != 0 {
				t.Errorf("expected 0 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
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
			WaitForPersistence(pindex0_0, float64(1))
			n, err = bindex0.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
			if n != 1 {
				t.Errorf("expected 1 docs in bindex0, got: %d", n)
			}
			n, err = bindex1.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
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
			WaitForPersistence(pindex0_0, float64(1))
			n, err = bindex0.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
			if n != 1 {
				t.Errorf("expected 1 docs in bindex0, got: %d", n)
			}
			WaitForPersistence(pindex1, float64(1))
			n, err = bindex1.DocCount()
			if err != nil {
				t.Errorf("error getting doc count: %v", err)
			}
			if n != 1 {
				t.Errorf("expected 1 docs in bindex1, got: %d", n)
			}

			err = sf.Rollback("1", 0)
			if err != nil {
				t.Errorf("expected no error to rollback, err: %v", err)
			}
			runtime.Gosched()

			// wait till the feed is created after rollback
			feedAllotment := mgr.GetOption(cbgt.FeedAllotmentOption)
			pindex0_0_Feed := cbgt.FeedNameForPIndex(pindex0_0, feedAllotment)

			// Adding a sleep since the feed is created async
			// Should be a sufficienty large sleep to ensure the feed is created
			time.Sleep(2 * time.Second)

			foundFeed := false
			for i := 0; i < 1000; i++ {
				feeds, _ := mgr.CurrentMaps()

				if _, exists := feeds[pindex0_0_Feed]; exists {
					foundFeed = true
					break
				}
			}
			if !foundFeed {
				t.Errorf("rollback taking too long")
			}

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
			if destForwarder, ok := pindex0_0.Dest.(*cbgt.DestForwarder); ok {
				if bp, ok := destForwarder.DestProvider.(*BleveDest); ok {
					bindex0, ok := bp.bindex.(bleve.Index)
					if !ok || bindex0 == nil {
						t.Errorf("expected bleve.Index")
					}
					n, err := bindex0.DocCount()
					if err != nil {
						t.Errorf("error getting doc count: %v", err)
					}
					if n != 0 {
						t.Errorf("expected 0 docs in bindex0 after rollback,"+
							" got: %d", n)
					}
				}
			}

			if destForwarder, ok := pindex1.Dest.(*cbgt.DestForwarder); ok {
				if bp, ok := destForwarder.DestProvider.(*BleveDest); ok {
					bindex1, ok := bp.bindex.(bleve.Index)
					if !ok || bindex1 == nil {
						t.Errorf("expected bleve.Index")
					}
					n, err := bindex1.DocCount()
					if err != nil {
						t.Errorf("error getting doc count: %v", err)
					}
					if n != 1 {
						t.Errorf("expected 1 docs in bindex1 after rollback,"+
							" got: %d", n)
					}
				}
			}
		})
}

func TestManagerIndexControl(t *testing.T) {
	prevDataSourceUUID := cbgt.DataSourceUUID
	cbgt.DataSourceUUID = func(sourceType, sourceName, sourceParams, server string,
		options map[string]string) (string, error) {
		return "123", nil
	}

	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer func() {
		cbgt.DataSourceUUID = prevDataSourceUUID
		os.RemoveAll(emptyDir)
	}()

	cfg := cbgt.NewCfgMem()
	m := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", nil)
	if err := m.Start("wanted"); err != nil {
		t.Errorf("expected Manager.Start() to work, err: %v", err)
	}
	sourceParams := ""
	if err := m.CreateIndex("primary", "default", "123", sourceParams,
		"fulltext-index", "foo", "", cbgt.PlanParams{}, ""); err != nil {
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
	if npp != nil {
		t.Errorf("expected nil, as we're back to default settings")
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
	if npp != nil {
		t.Errorf("expected nil, as we're back to default settings")
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
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := cbgt.NewPIndex(nil, "fake", "uuid",
		"fulltext-index", "indexName", "indexUUID", "{}",
		"sourceType", "sourceName", "sourceUUID",
		"sourceParams", "sourcePartitions",
		cbgt.PIndexPath(emptyDir, "fake"))
	if pindex == nil || err != nil {
		t.Errorf("expected NewPIndex to fail with empty json map")
	}
}

func TestNewPIndexBleveBadMapping(t *testing.T) {
	emptyDir, _ := os.MkdirTemp("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	pindex, err := cbgt.NewPIndex(nil, "fake", "uuid",
		"fulltext-index", "indexName", "indexUUID", "} hey this isn't json :-(",
		"sourceType", "sourceName", "sourceUUID",
		"sourceParams", "sourcePartitions",
		cbgt.PIndexPath(emptyDir, "fake"))
	if pindex != nil || err == nil {
		t.Errorf("expected NewPIndex to fail with bad json")
	}
}

func TestSearchRequestExt(t *testing.T) {
	reqs := [][]byte{
		[]byte(`{"query": {"query": "california"}, "size": 4, "from": 5}`),
		[]byte(`{"query": {"query": "california"}, "limit": 4, "offset": 5}`),
		[]byte(`{"query": {"query": "california"}, "limit": 10, "offset":8, "size": 4, "from": 5}`),
		[]byte(`{"query": {"query": "california"}, "size": 4, "offset": 5}`),
		[]byte(`{"query": {"query": "california"}, "limit": 4, "from": 5}`),
	}

	expectSize := 4
	expectFrom := 5

	for i, req := range reqs {
		var sr *SearchRequest
		err := json.Unmarshal(req, &sr)
		if err != nil {
			t.Fatal(err)
		}
		bsr, err := sr.ConvertToBleveSearchRequest()
		if err != nil {
			t.Fatal(err)
		}
		if bsr.Size != expectSize {
			t.Fatalf("(%d) Expected size: %v, got size: %v", i+1, expectSize, bsr.Size)
		}
		if bsr.From != expectFrom {
			t.Fatalf("(%d) Expected from: %v, got from: %v", i+1, expectFrom, bsr.From)
		}
	}
}

func getTestCache() *collMetaFieldCache {
	cache := make(map[string]string)
	cache["ftsIndexA$colA"] = "_$suid_$cuidA"
	cache["ftsIndexB$colA"] = "_$suid_$cuidA"
	cache["ftsIndexB$colB"] = "_$suid_$cuidB"
	cache["ftsIndexB$colC"] = "_$suid_$cuidC"

	indexCache := make(map[string]*sourceDetails)
	sdm := &sourceDetails{scopeName: "test",
		collUIDNameMap: make(map[uint32]string)}
	sdm.collUIDNameMap[1] = "colA"
	sdm.collUIDNameMap[2] = "colB"
	indexCache["ftsIndexA"] = sdm
	sdm = &sourceDetails{scopeName: "test",
		collUIDNameMap: make(map[uint32]string)}
	sdm.collUIDNameMap[1] = "colA"
	sdm.collUIDNameMap[2] = "colB"
	sdm.collUIDNameMap[3] = "colC"
	indexCache["ftsIndexB"] = sdm

	testCache := &collMetaFieldCache{}
	testCache.cache = cache
	testCache.sourceDetailsMap = indexCache
	return testCache
}

func TestCollectionSearchRequest(t *testing.T) {
	testCache := getTestCache()
	tests := []struct {
		indexName     string
		collections   []string
		qNumDisjuncts int
		qField        string
		qTerm         []string
	}{
		{
			indexName:     "ftsIndexA",
			collections:   []string{"colA"},
			qField:        "_$scope_$collection",
			qTerm:         []string{"_$suid_$cuidA"},
			qNumDisjuncts: 1,
		},
		{
			indexName:     "ftsIndexB",
			collections:   []string{"colA", "colB", "colC"},
			qField:        "_$scope_$collection",
			qTerm:         []string{"_$suid_$cuidA", "_$suid_$cuidB", "_$suid_$cuidC"},
			qNumDisjuncts: 3,
		},
	}

	var sr *SearchRequest
	err := json.Unmarshal([]byte(`{"query": {"query": "california"}, "size": 4, "from": 5}`), &sr)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		sr.Collections = test.collections
		bsr, err := sr.ConvertToBleveSearchRequest()
		_, bsr.Query = sr.decorateQuery(test.indexName, bsr.Query, testCache)
		if err != nil {
			t.Fatal(err)
		}
		switch qq := bsr.Query.(type) {
		case *query.ConjunctionQuery:
			if len(qq.Conjuncts) != 2 {
				t.Fatalf("Exception in conjunction query, number of conjunct clauses: %v",
					len(qq.Conjuncts))
			}
			dqs := qq.Conjuncts[1].(*query.DisjunctionQuery)
			if len(dqs.Disjuncts) != test.qNumDisjuncts {
				t.Fatalf("Exception in disjunction query, number of disjunct clauses: %v",
					len(dqs.Disjuncts))
			}

			for i, dq := range dqs.Disjuncts {
				mq := dq.(*query.MatchQuery)
				if mq.Match != test.qTerm[i] || mq.FieldVal != test.qField {
					t.Fatalf("Exception in disjunction should query: %v, %v",
						mq.Match, mq.FieldVal)
				}
			}
		default:
			t.Fatalf("No conjunction query found, query: %+v", bsr.Query)
		}
	}
}

func TestCollectionSearchRequestDocIDQuery(t *testing.T) {
	testCache := getTestCache()
	tests := []struct {
		indexName        string
		collections      []string
		targetDocIDCount int
		queryParams      string
		docIDs           []string
	}{
		{
			indexName:        "ftsIndexA",
			collections:      []string{"colA"},
			targetDocIDCount: 1,
			queryParams:      "{\"ids\": [\"beer-100\"]}",
			docIDs:           []string{"beer-100"},
		},
		{
			indexName:        "ftsIndexA",
			collections:      nil,
			targetDocIDCount: 2,
			queryParams:      "{\"ids\": [\"beer-100\"]}",
			docIDs:           []string{"beer-100"},
		},
		{
			indexName:        "ftsIndexB",
			collections:      nil,
			targetDocIDCount: 3,
			queryParams:      "{\"ids\": [\"beer-100\"]}",
			docIDs:           []string{"beer-100"},
		},
		{
			indexName:        "ftsIndexB",
			collections:      nil,
			targetDocIDCount: 6,
			queryParams:      "{\"ids\": [\"beer-100\", \"beer-101\"]}",
			docIDs:           []string{"beer-100", "beer-101"},
		},
		{
			indexName:        "ftsIndexB",
			collections:      []string{"colA"},
			targetDocIDCount: 2,
			queryParams:      "{\"ids\": [\"beer-100\", \"beer-101\"]}",
			docIDs:           []string{"beer-100", "beer-101"},
		},
	}

	equal := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i, val := range a {
			if val != b[i] {
				return false
			}
		}
		return true
	}

	var origQuery query.Query
	var decoratedQuery *query.DocIDQuery
	var ok bool

	for _, test := range tests {
		var sr *SearchRequest
		queryStr := fmt.Sprintf("{\"query\": %s, \"size\": 4, \"from\": 5}", test.queryParams)
		err := json.Unmarshal([]byte(queryStr), &sr)
		if err != nil {
			t.Fatal(err)
		}
		sr.Collections = test.collections
		bsr, err := sr.ConvertToBleveSearchRequest()
		origQuery, bsr.Query = sr.decorateQuery(test.indexName, bsr.Query, testCache)
		if err != nil {
			t.Fatal(err)
		}
		// verify that the original query remains intact
		if decoratedQuery, ok = origQuery.(*query.DocIDQuery); !ok {
			t.Errorf("Original query should remain intact, but got: %+v", origQuery)
		} else if !equal(decoratedQuery.IDs, test.docIDs) {
			t.Errorf("Expected docIDs in query %+v, but got: %+v", test.docIDs, decoratedQuery.IDs)
		}

		if decoratedQuery, ok = bsr.Query.(*query.DocIDQuery); !ok {
			t.Errorf("Collection decorated query expected, but got: %+v", bsr.Query)
		}
		// docID query gets targetted for all the source collections
		// in the index unless it is a collection targetted query.
		if len(decoratedQuery.IDs) != test.targetDocIDCount {
			t.Errorf("Expected %d docIDs after decoration, but got: %+v", test.targetDocIDCount, bsr.Query)
		}

	}
}

func TestHasXAttrs(t *testing.T) {

	tests := []struct {
		bleveParams     *BleveParams
		indexMapping    *mapping.IndexMappingImpl
		fields          map[string]interface{}
		typeMappingName string
		xattrs          bool
	}{
		{
			bleveParams:  NewBleveParams(),
			indexMapping: bleve.NewIndexMapping(),
			fields: map[string]interface{}{
				"key": map[string]interface{}{
					"value": struct{}{},
				},
			},
			typeMappingName: "",
			xattrs:          false,
		},
		{
			bleveParams:  NewBleveParams(),
			indexMapping: bleve.NewIndexMapping(),
			fields: map[string]interface{}{
				xattrsMappingName: map[string]interface{}{
					"value": struct{}{},
				},
			},
			typeMappingName: "",
			xattrs:          true,
		},
		{
			bleveParams:  NewBleveParams(),
			indexMapping: bleve.NewIndexMapping(),
			fields: map[string]interface{}{
				"key": map[string]interface{}{
					xattrsMappingName: struct{}{},
				},
			},
			typeMappingName: "type",
			xattrs:          false,
		},
		{
			bleveParams:  NewBleveParams(),
			indexMapping: bleve.NewIndexMapping(),
			fields: map[string]interface{}{
				"key": map[string]interface{}{
					"value": struct{}{},
				},
			},
			typeMappingName: xattrsMappingName,
			xattrs:          false,
		},
	}

	var res bool
	for _, test := range tests {

		docMapping := bleve.NewDocumentMapping()
		for outerName, val := range test.fields {

			docMapping.AddFieldMappingsAt(outerName, bleve.NewTextFieldMapping())
			if inner, ok := val.(map[string]interface{}); ok {
				for innerName, _ := range inner {
					docMapping.Properties[outerName].AddFieldMappingsAt(
						innerName, bleve.NewTextFieldMapping(),
					)
				}
			}
		}

		if test.typeMappingName == "" {
			test.indexMapping.DefaultMapping = docMapping
		} else {
			test.indexMapping.TypeMapping[test.typeMappingName] = docMapping
		}
		test.bleveParams.Mapping = test.indexMapping
		res = mappingContainsXAttrs(test.bleveParams)
		if res != test.xattrs {
			t.Errorf("Expected %v as output of hasXAttrs, but got %v. Fields - %+v",
				test.xattrs, res, test.fields)
		}

	}
}

func TestVectorPictureFromIndexMapping(t *testing.T) {
	tests := []struct {
		idxMapping     *mapping.IndexMappingImpl
		expectFields   int
		expectDims     int
		expectDimsFlag string
		expectCosine   bool
	}{
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "text",
						},
					},
				},
			},
			expectFields:   noVectorFields,
			expectDimsFlag: "",
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "vector",
							Dims: 3072,
						},
					},
				},
			},
			expectFields:   vectorFields,
			expectDims:     3072,
			expectDimsFlag: featuresVectorBase64Dims4096,
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "vector_base64",
						},
					},
				},
			},
			expectFields:   vectorAndBase64Fields,
			expectDimsFlag: "",
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"Type1": {
						Enabled:    true,
						Properties: map[string]*mapping.DocumentMapping{},
						Fields: []*mapping.FieldMapping{
							{
								Type: "vector_base64",
								Dims: 3072,
							},
						},
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "vector",
						},
					},
				},
			},
			expectFields:   vectorAndBase64Fields,
			expectDims:     3072,
			expectDimsFlag: featuresVectorBase64Dims4096,
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"Type1": {
						Enabled:    true,
						Properties: map[string]*mapping.DocumentMapping{},
						Fields: []*mapping.FieldMapping{
							{
								Type: "vector",
								Dims: 3072,
							},
						},
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "vector_base64",
							Dims: 4096,
						},
					},
				},
			},
			expectFields:   vectorAndBase64Fields,
			expectDims:     4096,
			expectDimsFlag: featuresVectorBase64Dims4096,
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"Type1": {
						Enabled:    true,
						Properties: map[string]*mapping.DocumentMapping{},
						Fields: []*mapping.FieldMapping{
							{
								Type: "vector",
							},
						},
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "text",
						},
					},
				},
			},
			expectFields:   vectorFields,
			expectDimsFlag: "",
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"Type1": {
						Enabled:    true,
						Properties: map[string]*mapping.DocumentMapping{},
						Fields: []*mapping.FieldMapping{
							{
								Type: "vector_base64",
							},
						},
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled:    true,
					Properties: map[string]*mapping.DocumentMapping{},
					Fields: []*mapping.FieldMapping{
						{
							Type: "text",
						},
					},
				},
			},
			expectFields: vectorAndBase64Fields,
		},
		{
			idxMapping: &mapping.IndexMappingImpl{
				TypeMapping: map[string]*mapping.DocumentMapping{
					"Type1": {
						Enabled:    true,
						Properties: map[string]*mapping.DocumentMapping{},
						Fields: []*mapping.FieldMapping{
							{
								Type:       "vector",
								Dims:       3072,
								Similarity: "cosine",
							},
						},
					},
				},
				DefaultMapping: &mapping.DocumentMapping{
					Enabled: false,
				},
			},
			expectFields:   vectorFields,
			expectDims:     3072,
			expectDimsFlag: featuresVectorBase64Dims4096,
			expectCosine:   true,
		},
	}

	for testi, test := range tests {
		res := vectorPictureFromIndexMapping(test.idxMapping)

		if res.fields != test.expectFields {
			t.Errorf("[%d] Expected fields:%v as output, but got %v. Index Mapping - %+v",
				testi+1, test.expectFields, res.fields, test.idxMapping)
		}

		if res.maxDims != test.expectDims {
			t.Errorf("[%d] Expected dims:%v as output, but got %v. Index Mapping - %+v",
				testi+1, test.expectDims, res.maxDims, test.idxMapping)
		}

		if featureFlagForDims(res.maxDims) != test.expectDimsFlag {
			t.Errorf("[%d] Unexpected flag for dims: %v", testi+1, res.maxDims)
		}

		if res.cosine != test.expectCosine {
			t.Errorf("[%d] Expected cosine:%v as output, but got %v. Index Mapping - %+v",
				testi+1, test.expectCosine, res.cosine, test.idxMapping)
		}
	}
}

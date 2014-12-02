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
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/gorilla/mux"
)

func TestNewManagerRESTRouter(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	ring, err := NewMsgRing(nil, 1)

	cfg := NewCfgMem()
	mgr := NewManager(VERSION, cfg, NewUUID(), nil, "", 1, ":1000",
		emptyDir, "some-datasource", nil)
	r, err := NewManagerRESTRouter(mgr, emptyDir, ring)
	if r == nil || err != nil {
		t.Errorf("expected no errors")
	}

	mgr = NewManager(VERSION, cfg, NewUUID(), []string{"queryer", "anotherTag"},
		"", 1, ":1000", emptyDir, "some-datasource", nil)
	r, err = NewManagerRESTRouter(mgr, emptyDir, ring)
	if r == nil || err != nil {
		t.Errorf("expected no errors")
	}
}

type RESTHandlerTest struct {
	Desc          string
	Path          string
	Method        string
	Params        url.Values
	Body          []byte
	Status        int
	ResponseBody  []byte
	ResponseMatch map[string]bool

	Before func()
	After  func()
}

func (test *RESTHandlerTest) check(t *testing.T, record *httptest.ResponseRecorder) {
	if got, want := record.Code, test.Status; got != want {
		t.Errorf("%s: response code = %d, want %d", test.Desc, got, want)
		t.Errorf("%s: response body = %s", test.Desc, record.Body)
	}
	got := bytes.TrimRight(record.Body.Bytes(), "\n")
	if test.ResponseBody != nil {
		if !reflect.DeepEqual(got, test.ResponseBody) {
			t.Errorf("%s: expected: '%s', got: '%s'",
				test.Desc, test.ResponseBody, got)
		}
	}
	for pattern, shouldMatch := range test.ResponseMatch {
		didMatch := bytes.Contains(got, []byte(pattern))
		if didMatch != shouldMatch {
			t.Errorf("%s: expected match %t for pattern %s, got %t",
				test.Desc, shouldMatch, pattern, didMatch)
			t.Errorf("%s: response body was: %s", test.Desc, got)
		}
	}
}

func testRESTHandlers(t *testing.T, tests []*RESTHandlerTest, router *mux.Router) {
	for _, test := range tests {
		if test.Before != nil {
			test.Before()
		}
		if test.Method != "NOOP" {
			req := &http.Request{
				Method: test.Method,
				URL:    &url.URL{Path: test.Path},
				Form:   test.Params,
				Body:   ioutil.NopCloser(bytes.NewBuffer(test.Body)),
			}
			record := httptest.NewRecorder()
			router.ServeHTTP(record, req)
			test.check(t, record)
		}
		if test.After != nil {
			test.After()
		}
	}
}

func TestHandlersForEmptyManager(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	router, err := NewManagerRESTRouter(mgr, "static", mr)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:         "log on empty msg ring",
			Path:         "/api/log",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"messages":["hello","world"]}`),
		},
		{
			Desc:   "cfg on empty manaager",
			Path:   "/api/cfg",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok"`:       true,
				`"indexDefs":null`:    true,
				`"nodeDefsKnown":{`:   true,
				`"nodeDefsWanted":{`:  true,
				`"planPIndexes":null`: true,
			},
		},
		{
			Desc:   "cfg refresh on empty, unchanged manager",
			Path:   "/api/cfgRefresh",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "manager kick on empty, unchanged manager",
			Path:   "/api/managerKick",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "manager meta",
			Path:   "/api/managerMeta",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok"`:    true,
				`"startSamples":{`: true,
			},
		},
		{
			Desc:   "feed stats when no feeds",
			Path:   "/api/feedStats",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`[`: true,
				`]`: true,
			},
		},
		{
			Desc:         "list empty indexes",
			Path:         "/api/index",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"status":"ok","indexDefs":null}`),
		},
		{
			Desc:         "try to get a nonexistent index",
			Path:         "/api/index/NOT-AN-INDEX",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       400,
			ResponseBody: []byte(`not an index`),
		},
		{
			Desc:   "try to create a default index with bad server",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`failed to connect`: true,
			},
		},
		{
			Desc:   "try to delete a nonexistent index when no indexes",
			Path:   "/api/index/NOT-AN-INDEX",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`indexes do not exist`: true,
			},
		},
		{
			Desc:   "try to count a nonexistent index when no indexes",
			Path:   "/api/index/NOT-AN-INDEX/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`could not get indexDefs`: true,
			},
		},
		{
			Desc:   "try to query a nonexistent index when no indexes",
			Path:   "/api/index/NOT-AN-INDEX/query",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`could not get indexDefs`: true,
			},
		},
		{
			Desc:   "create an index with bogus indexType",
			Path:   "/api/index/idxBogusIndexType",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"not-a-real-index-type"},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error`: true,
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersForOneIndexWithNILFeed(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "shelf/rack/row", 1, ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := NewMsgRing(os.Stderr, 1000)

	router, err := NewManagerRESTRouter(mgr, "static", mr)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "create an index with nil feed",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"bleve"},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "cfg on a 1 index manaager",
			Path:   "/api/cfg",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok"`:       true,
				`"indexDefs":null`:    false,
				`"nodeDefsKnown":{`:   true,
				`"nodeDefsWanted":{`:  true,
				`"planPIndexes":null`: false,
			},
		},
		{
			Desc:   "cfg refresh on a 1 index manager",
			Path:   "/api/cfgRefresh",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "manager kick on a 1 index manager",
			Path:   "/api/managerKick",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "manager meta on a 1 index manager",
			Path:   "/api/managerMeta",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok"`:    true,
				`"startSamples":{`: true,
			},
		},
		{
			Desc:   "feed stats on a 1 index manager",
			Path:   "/api/feedStats",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`[`:                  true,
				`{"feedName":"idx0_`: true,
				`]`:                  true,
			},
		},
		{
			Desc:   "list on a 1 index manager",
			Path:   "/api/index",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","indexDefs":{"uuid":`:                                                                                                                                true,
				`"indexDefs":{"idx0":{"type":"bleve","name":"idx0","uuid":"`:                                                                                                         true,
				`"params":"","sourceType":"nil","sourceName":"","sourceUUID":"","sourceParams":"","planParams":{"maxPartitionsPerPIndex":0,"numReplicas":0,"hierarchyRules":null}}}`: true,
			},
		},
		{
			Desc:         "try to get a nonexistent index on a 1 index manager",
			Path:         "/api/index/NOT-AN-INDEX",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       400,
			ResponseBody: []byte(`not an index`),
		},
		{
			Desc:   "try to delete a nonexistent index on a 1 index manager",
			Path:   "/api/index/NOT-AN-INDEX",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`index to delete does not exist`: true,
			},
		},
		{
			Desc:   "try to count a nonexistent index on a 1 index manager",
			Path:   "/api/index/NOT-AN-INDEX/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err: no indexDef, indexName: NOT-AN-INDEX`: true,
			},
		},
		{
			Desc:   "try to query a nonexistent index on a 1 index manager",
			Path:   "/api/index/NOT-AN-INDEX/query",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err: no indexDef, indexName: NOT-AN-INDEX`: true,
			},
		},
		{
			Desc:   "get idx0 on a 1 index manager",
			Path:   "/api/index/idx0",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"status":"ok","indexDef":{"type":"bleve","name":"idx0","uuid":`: true,
			},
		},
		{
			Desc:   "count empty idx0 on a 1 index manager",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "query empty idx0 on a 1 index manager with missing args",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`unexpected end of JSON input`: true,
			},
		},
		{
			Desc:   "no-hit query of empty idx0 on a 1 index manager",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"query":{"query":"foo"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
		{
			Desc:   "delete idx0 on a 1 index manager",
			Path:   "/api/index/idx0",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "list indexes after delete one & only index",
			Path:   "/api/index",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok",`: true,
				`"indexDefs":{}`:  true,
			},
		},
		{
			Desc:         "try to get a deleted index",
			Path:         "/api/index/idx0",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       400,
			ResponseBody: []byte(`not an index`),
		},
		{
			Desc:   "try to delete a deleted index",
			Path:   "/api/index/idx0",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`index to delete does not exist, indexName: idx0`: true,
			},
		},
		{
			Desc:   "try to count a deleted index",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`no indexDef, indexName: idx0`: true,
			},
		},
		{
			Desc:   "try to query a deleted index",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`no indexDef, indexName: idx0`: true,
			},
		},
		{
			Desc:   "re-create a deleted index with nil feed",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"bleve"},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "create an index bad inputParams",
			Path:   "/api/index/idxBadInputParams",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"bleve"},
				"indexParams": []string{"}}totally n0t json{{"},
				"sourceType":  []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error creating index`: true,
			},
		},
		{
			Desc:   "create an index bad planParams",
			Path:   "/api/index/idxBadPlanParams",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"blackhole"},
				"indexParams": []string{},
				"sourceType":  []string{"nil"},
				"planParams":  []string{">>>this isn't json<<<"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error parsing planParams`: true,
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersWithOnePartitionDestFeedIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := NewMsgRing(os.Stderr, 1000)

	router, err := NewManagerRESTRouter(mgr, "static", mr)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	var doneCh chan *httptest.ResponseRecorder

	var feed *DestFeed

	tests := []*RESTHandlerTest{
		{
			Desc:   "create an index with dest feed with bad sourceParams",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"bleve"},
				"sourceType":   []string{"dest"},
				"sourceParams": []string{"-}totally n0t json{-"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`could not parse sourceParams`: true,
			},
		},
		{
			Desc:   "create an index with dest feed with 1 partition",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"bleve"},
				"sourceType":   []string{"dest"},
				"sourceParams": []string{`{"numPartitions":1}`},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
			After: func() {
				feeds, pindexes := mgr.CurrentMaps()
				if len(feeds) != 1 {
					t.Errorf("expected to be 1 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed, ok = f.(*DestFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a DestFeed")
					}
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(1)
				snapEnd := uint64(10)
				err = feed.OnSnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
			},
			Desc:   "count idx0 should be 0 when snapshot just started",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("hello")
				seq := uint64(1)
				val := []byte(`{"foo":"bar","yow":"wow"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0 when got an update (doc created)" +
				" but snapshot not ended",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("world")
				seq := uint64(2)
				val := []byte(`{"foo":"bing","yow":"wow"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0 when got 2nd update (another doc created)" +
				" but snapshot not ended",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("hello")
				seq := uint64(3)
				val := []byte(`{"foo":"baz","yow":"wow"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0 when got 3rd update" +
				" (mutation, so only 2 keys) but snapshot not ended",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(11)
				snapEnd := uint64(20)
				err = feed.OnSnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
			},
			Desc:   "count idx0 should be 2 when 1st snapshot ended",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":2}`: true,
			},
		},
		{
			Desc:   "query with bad args",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`>>>not json<<<`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err: QueryBlevePIndexImpl parsing bleveQueryParams`: true,
			},
		},
		{
			Desc:   "query for 0 hit",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"bar"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
		{
			Desc:   "query for 1 hit",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"baz"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"id":"hello"`:   true,
				`"total_hits":1`: true,
			},
		},
		{
			Desc:   "query for 2 hits",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"wow"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"id":"hello"`:   true,
				`"id":"world"`:   true,
				`"total_hits":2`: true,
			},
		},
		{
			Desc:   "direct pindex query on bogus pindex",
			Path:   "/api/pindex/not-a-pindex/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"wow"}}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`no pindex`: true,
			},
		},
		{
			Desc:   "direct pindex query on mismatched pindex UUID",
			Method: "NOOP",
			After: func() {
				var pindex *PIndex
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				body := []byte(`{"query":{"size":10,"query":{"query":"wow"}}}`)
				req := &http.Request{
					Method: "POST",
					URL:    &url.URL{Path: "/api/pindex/" + pindex.Name + "/query"},
					Form: url.Values{
						"pindexUUID": []string{"not the right pindex UUID"},
					},
					Body: ioutil.NopCloser(bytes.NewBuffer(body)),
				}
				record := httptest.NewRecorder()
				router.ServeHTTP(record, req)
				test := &RESTHandlerTest{
					Desc:   "direct pindex query on mismatched pindex UUID check",
					Status: 400,
					ResponseMatch: map[string]bool{
						`wrong pindexUUID`: true,
					},
				}
				test.check(t, record)
			},
		},
		{
			Desc:   "direct pindex query with clipped requestBody",
			Method: "NOOP",
			After: func() {
				var pindex *PIndex
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				body := []byte(`{"query":{"size":10,"query":{"query":"wow"`)
				req := &http.Request{
					Method: "POST",
					URL:    &url.URL{Path: "/api/pindex/" + pindex.Name + "/query"},
					Form:   url.Values{},
					Body:   ioutil.NopCloser(bytes.NewBuffer(body)),
				}
				record := httptest.NewRecorder()
				router.ServeHTTP(record, req)
				test := &RESTHandlerTest{
					Desc:   "direct pindex query with clipped requestBody check",
					Status: 400,
					ResponseMatch: map[string]bool{
						`unexpected end of JSON input`: true,
					},
				}
				test.check(t, record)
			},
		},
		{
			Desc:   "direct pindex query",
			Method: "NOOP",
			After: func() {
				var pindex *PIndex
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				body := []byte(`{"query":{"size":10,"query":{"query":"wow"}}}`)
				req := &http.Request{
					Method: "POST",
					URL:    &url.URL{Path: "/api/pindex/" + pindex.Name + "/query"},
					Form:   url.Values(nil),
					Body:   ioutil.NopCloser(bytes.NewBuffer(body)),
				}
				record := httptest.NewRecorder()
				router.ServeHTTP(record, req)
				test := &RESTHandlerTest{
					Desc:   "direct pindex query check",
					Status: http.StatusOK,
					ResponseMatch: map[string]bool{
						`"id":"hello"`:   true,
						`"id":"world"`:   true,
						`"total_hits":2`: true,
					},
				}
				test.check(t, record)
			},
		},

		// ------------------------------------------------------
		// Now let's test a consistency wait.
		{
			Desc:   "query with consistency params in the past",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"wow"}},"consistency":{"level":"at_plus","vectors":{"idx0":{"0":1}}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"id":"hello"`:   true,
				`"id":"world"`:   true,
				`"total_hits":2`: true,
			},
		},
		{
			Desc:   "query with bogus consistency params level",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"wow"}},"consistency":{"level":"this is not your level","vectors":{"idx0":{"0":1}}}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err: consistency wait unsupported level: this is not your level`: true,
			},
		},
		{
			Desc:   "query with bogus consistency params vectors",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"wow"}},"consistency":{"level":"at_plus","vectors":["array","not","legal"]}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err: json: cannot unmarshal array into Go value of type map[string]main.ConsistencyVector`: true,
			},
		},
		{
			Desc:   "query with consistency params in the future",
			Method: "NOOP",
			Before: func() {
				doneCh = make(chan *httptest.ResponseRecorder)
				go func() {
					body := []byte(`{"query":{"size":10,"query":{"query":"boof"}},"consistency":{"level":"at_plus","vectors":{"idx0":{"0":11}}}}`)
					req := &http.Request{
						Method: "POST",
						URL:    &url.URL{Path: "/api/index/idx0/query"},
						Form:   url.Values(nil),
						Body:   ioutil.NopCloser(bytes.NewBuffer(body)),
					}
					record := httptest.NewRecorder()
					router.ServeHTTP(record, req)
					doneCh <- record
				}()
			},
			After: func() {
				runtime.Gosched()
				select {
				case <-doneCh:
					t.Errorf("expected query to block waiting for more mutations")
				default:
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("whee")
				seq := uint64(11)
				val := []byte(`{"foo":"boof","yow":"wXw"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 2 since snapshot 2 is still open" +
				" (mutation, so only 2 keys) but snapshot not ended",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":2}`: true,
			},
			After: func() {
				runtime.Gosched()
				select {
				case <-doneCh:
					t.Errorf("expected query still blocked with snapshot still open")
				default:
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(21)
				snapEnd := uint64(30)
				err = feed.OnSnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
			},
			Desc:   "count idx0 should be 3 when 2st snapshot ended",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":3}`: true,
			},
			After: func() {
				runtime.Gosched()
				record, ok := <-doneCh
				if record == nil || !ok {
					t.Errorf("expected a record: %#v, ok: %v", record, ok)
				}
				test := &RESTHandlerTest{
					Desc:   "test consistency wait got right result",
					Status: http.StatusOK,
					ResponseMatch: map[string]bool{
						`"id":"whee"`:    true,
						`"total_hits":1`: true,
					},
				}
				test.check(t, record)
			},
		},

		// ------------------------------------------------------
		// Now let's test a 1-to-1 index alias.
		{
			Desc:   "create an index alias with bad indexParams",
			Path:   "/api/index/aa0",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"alias"},
				"indexParams": []string{`}--<not json>--{`},
				"sourceType":  []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error creating index`: true,
			},
		},
		{
			Desc:   "create an index alias with 1 target",
			Path:   "/api/index/aa0",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"alias"},
				"indexParams": []string{`{"targets":{"idx0":{}}}`},
				"sourceType":  []string{"nil"},
			},
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`"status":"ok"`: true,
			},
		},
		{
			Desc:   "count aa0 should be 2 when 1st snapshot ended",
			Path:   "/api/index/aa0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":3}`: true,
			},
		},
		{
			Desc:   "query with bogus args",
			Path:   "/api/index/aa0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`>>>not json<<<`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err: QueryAlias parsing bleveQueryParams`: true,
			},
		},
		{
			Desc:   "query for 0 hit",
			Path:   "/api/index/aa0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"bar"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
		{
			Desc:   "query for 1 hit",
			Path:   "/api/index/aa0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"baz"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"id":"hello"`:   true,
				`"total_hits":1`: true,
			},
		},
		{
			Desc:   "query for 2 hits",
			Path:   "/api/index/aa0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"wow"}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"id":"hello"`:   true,
				`"id":"world"`:   true,
				`"total_hits":2`: true,
			},
		},

		// ------------------------------------------------------
		// Now let's test a 1-to-1 index alias to a bogus target.
		{
			Desc:   "create an index alias with 1 target",
			Path:   "/api/index/aaBadTarget",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"alias"},
				"indexParams": []string{`{"targets":{"idxNotReal":{}}}`},
				"sourceType":  []string{"nil"},
			},
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`"status":"ok"`: true,
			},
		},
		{
			Desc:   "count aaBadTarget should be error",
			Path:   "/api/index/aaBadTarget/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: 500,
			ResponseMatch: map[string]bool{
				`error`: true,
			},
		},
		{
			Desc:   "query for 0 hit",
			Path:   "/api/index/aaBadTarget/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"bar"}}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`error`: true,
			},
		},

		// ------------------------------------------------------
		// Now let's delete the index.
		{
			Desc:   "delete idx0 with dest feed",
			Path:   "/api/index/idx0",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
			After: func() {
				feeds, pindexes := mgr.CurrentMaps()
				if len(feeds) != 0 {
					t.Errorf("expected to be 0 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 0 {
					t.Errorf("expected to be 0 pindex, got pindexes: %+v", pindexes)
				}
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersWithOnePartitionDestFeedRollback(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := NewMsgRing(os.Stderr, 1000)

	router, err := NewManagerRESTRouter(mgr, "static", mr)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	var doneCh chan *httptest.ResponseRecorder

	var feed *DestFeed

	tests := []*RESTHandlerTest{
		{
			Desc:   "create an index with dest feed with 1 partition for rollback",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"bleve"},
				"sourceType":   []string{"dest"},
				"sourceParams": []string{`{"numPartitions":1}`},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
			After: func() {
				feeds, pindexes := mgr.CurrentMaps()
				if len(feeds) != 1 {
					t.Errorf("expected to be 1 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed, ok = f.(*DestFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a DestFeed")
					}
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(1)
				snapEnd := uint64(10)
				err = feed.OnSnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
			},
			Desc:   "count idx0 should be 0 when snapshot just started, pre-rollback",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("hello")
				seq := uint64(1)
				val := []byte(`{"foo":"bar","yow":"wow"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0 when got an update (doc created)" +
				" but snapshot not ended, pre-rollback",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("world")
				seq := uint64(2)
				val := []byte(`{"foo":"bing","yow":"wow"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0 when got 2nd update (another doc created)" +
				" but snapshot not ended, pre-rollback",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				key := []byte("hello")
				seq := uint64(3)
				val := []byte(`{"foo":"baz","yow":"wow"}`)
				err = feed.OnDataUpdate(partition, key, seq, val)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0 when got 3rd update" +
				" (mutation, so only 2 keys) but snapshot not ended, pre-rollback",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(11)
				snapEnd := uint64(20)
				err = feed.OnSnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
			},
			Desc:   "count idx0 should be 2 when 1st snapshot ended, pre-rollback",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":2}`: true,
			},
		},
		{
			Desc:   "query with consistency params in the future, pre-rollback",
			Method: "NOOP",
			Before: func() {
				doneCh = make(chan *httptest.ResponseRecorder)
				go func() {
					body := []byte(`{"query":{"size":10,"query":{"query":"boof"}},"consistency":{"level":"at_plus","vectors":{"idx0":{"0":11}}}}`)
					req := &http.Request{
						Method: "POST",
						URL:    &url.URL{Path: "/api/index/idx0/query"},
						Form:   url.Values(nil),
						Body:   ioutil.NopCloser(bytes.NewBuffer(body)),
					}
					record := httptest.NewRecorder()
					router.ServeHTTP(record, req)
					doneCh <- record
				}()
			},
			After: func() {
				runtime.Gosched()
				select {
				case <-doneCh:
					t.Errorf("expected query to block waiting for more mutations")
				default:
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				seq := uint64(0)
				err = feed.Rollback(partition, seq)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
				// TODO: We should test right after rollback but before
				// we get a kick, but unfortunately results will be race-y.
				mgr.Kick("after-rollback")
			},
			Desc:   "count idx0 should be 0 since we rolled back to 0",
			Path:   "/api/index/idx0/count",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
			After: func() {
				runtime.Gosched()
				record, ok := <-doneCh
				if record == nil || !ok {
					t.Errorf("expected a record: %#v, ok: %v", record, ok)
				}
				test := &RESTHandlerTest{
					Desc:   "test consistency wait got right result",
					Status: 400,
					ResponseMatch: map[string]bool{
						`err: bleveIndexAlias consistency wait`: true,
						`err: consistency wait closed`:          true,
					},
				}
				test.check(t, record)
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestCreateIndexTwoNodes(t *testing.T) {
	cfg := NewCfgMem()

	emptyDir0, _ := ioutil.TempDir("./tmp", "test")
	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir0)
	defer os.RemoveAll(emptyDir1)

	meh0 := &TestMEH{}
	meh1 := &TestMEH{}
	mgr0 := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "localhost:1000", emptyDir0, "some-datasource", meh0)
	mgr1 := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, "localhost:2000", emptyDir1, "some-datasource", meh1)

	mgr0.Start("wanted")
	mgr1.Start("wanted")
	mgr0.Kick("test-start-kick")
	mgr1.Kick("test-start-kick")

	mr0, _ := NewMsgRing(os.Stderr, 1000)
	mr1, _ := NewMsgRing(os.Stderr, 1000)

	router0, err := NewManagerRESTRouter(mgr0, "static", mr0)
	if err != nil || router0 == nil {
		t.Errorf("no mux router")
	}
	router1, err := NewManagerRESTRouter(mgr1, "static", mr1)
	if err != nil || router1 == nil {
		t.Errorf("no mux router")
	}

	nd, _, err := CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs wanted")
	}

	if cfg.Refresh() != nil {
		t.Errorf("expected cfg refresh to work")
	}

	mgr0.Kick("test-start-kick-again")
	mgr1.Kick("test-start-kick-again")

	var feed0 *DestFeed
	var feed1 *DestFeed

	httpGetPrev := httpGet
	defer func() { httpGet = httpGetPrev }()

	httpGet = func(urlStr string) (
		resp *http.Response, err error) {
		u, _ := url.Parse(urlStr)
		req := &http.Request{
			Method: "GET",
			URL:    u,
			Body:   ioutil.NopCloser(bytes.NewBuffer([]byte{})),
		}
		record := httptest.NewRecorder()
		router1.ServeHTTP(record, req)
		return &http.Response{
			StatusCode: record.Code,
			Body:       ioutil.NopCloser(record.Body),
		}, nil
	}

	httpPostPrev := httpPost
	defer func() { httpPost = httpPostPrev }()

	httpPost = func(urlStr string, bodyType string, body io.Reader) (
		resp *http.Response, err error) {
		u, _ := url.Parse(urlStr)
		req := &http.Request{
			Method: "POST",
			URL:    u,
			Body:   ioutil.NopCloser(body),
		}
		record := httptest.NewRecorder()
		router1.ServeHTTP(record, req)
		return &http.Response{
			StatusCode: record.Code,
			Body:       ioutil.NopCloser(record.Body),
		}, nil
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "create an index with dest feed with 2 partitions, 2 nodes",
			Path:   "/api/index/myIdx",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"bleve"},
				"sourceType":   []string{"dest"},
				"sourceParams": []string{`{"numPartitions":2}`},
				"planParams":   []string{`{"maxPartitionsPerPIndex":1}`},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
			After: func() {
				if cfg.Refresh() != nil {
					t.Errorf("expected cfg refresh to work")
				}

				mgr0.Kick("kick after index create")
				mgr1.Kick("kick after index create")

				feeds, pindexes := mgr0.CurrentMaps()
				if len(feeds) != 1 {
					t.Errorf("expected to be 1 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed0, ok = f.(*DestFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a DestFeed")
					}
				}
				for _, p := range pindexes {
					if p.IndexName != "myIdx" {
						t.Errorf("expected p.IndexName to match on mgr0")
					}
				}

				feeds, pindexes = mgr1.CurrentMaps()
				if len(feeds) != 1 {
					t.Errorf("expected to be 1 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed1, ok = f.(*DestFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a DestFeed")
					}
				}
				for _, p := range pindexes {
					if p.IndexName != "myIdx" {
						t.Errorf("expected p.IndexName to match on mgr1")
					}
				}

				indexDefs, _, _ := CfgGetIndexDefs(cfg)
				if len(indexDefs.IndexDefs) != 1 {
					t.Errorf("expected 1 indexDef")
				}
				for _, indexDef := range indexDefs.IndexDefs {
					if indexDef.Name != "myIdx" {
						t.Errorf("expected 1 indexDef named myIdx")
					}
				}
			},
		},
		{
			Desc:   "count myIdx should be 0, 2 nodes",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "count myIdx should be 0, 2 nodes, with consistency params",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Body:   []byte(`{"consistency":{"level":"at_plus","vectors":{"idx0":{"0":0}}}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "query myIdx should have 0 hits, 2 nodes",
			Path:   "/api/index/myIdx/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"no-hits"}}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},

		// ----------------------------------
		// Now test some aliases.
		{
			Desc:   "create 2nd index with 2 partitions, 2 nodes",
			Path:   "/api/index/yourIdx",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"bleve"},
				"sourceType":   []string{"dest"},
				"sourceParams": []string{`{"numPartitions":8}`},
				"planParams":   []string{`{"maxPartitionsPerPIndex":1}`},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "create an alias, 2 nodes",
			Path:   "/api/index/myAlias",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"alias"},
				"indexParams": []string{`{"targets":{"myIdx":{},"yourIdx":{}}}`},
				"sourceType":  []string{"nil"},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "count myAlias should be 0, 2 nodes",
			Path:   "/api/index/myAlias/count",
			Method: "GET",
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "count myAlias should be 0, 2 nodes, with consistency params",
			Path:   "/api/index/myAlias/count",
			Method: "GET",
			Body:   []byte(`{"consistency":{"level":"at_plus","vectors":{"myAlias":{"0":0},"myIdx":{"0":0}}}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "query myAlias should have 0 hits, 2 nodes",
			Path:   "/api/index/myAlias/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"query":{"size":10,"query":{"query":"no-hits"}}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
	}

	testRESTHandlers(t, tests, router0)
}

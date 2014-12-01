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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
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

func testRESTHandlers(t *testing.T, tests []*RESTHandlerTest, router *mux.Router) {
	for _, test := range tests {
		if test.Before != nil {
			test.Before()
		}
		record := httptest.NewRecorder()
		req := &http.Request{
			Method: test.Method,
			URL:    &url.URL{Path: test.Path},
			Form:   test.Params,
			Body:   ioutil.NopCloser(bytes.NewBuffer(test.Body)),
		}
		router.ServeHTTP(record, req)
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
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersForOneIndexWithNILFeed(t *testing.T) {
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
			Desc:   "create an index with dest feed with bad sourceParams",
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
				`{"status":"ok","count":2}`: true,
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
			Desc:   "count aaBadTarget should be 2 when 1st snapshot ended",
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
	}

	testRESTHandlers(t, tests, router)
}

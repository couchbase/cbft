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
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"testing"

	"github.com/gorilla/mux"

	"github.com/couchbase/cbgt"

	"github.com/couchbase/moss"

	"fmt"

	"github.com/blevesearch/bleve"
	bleveMoss "github.com/blevesearch/bleve/index/store/moss"
)

// Implements ManagerEventHandlers interface.
type TestMEH struct {
	lastPIndex *cbgt.PIndex
	lastCall   string
	ch         chan bool
}

func (meh *TestMEH) OnRegisterPIndex(pindex *cbgt.PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnRegisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func (meh *TestMEH) OnUnregisterPIndex(pindex *cbgt.PIndex) {
	meh.lastPIndex = pindex
	meh.lastCall = "OnUnregisterPIndex"
	if meh.ch != nil {
		meh.ch <- true
	}
}

func (meh *TestMEH) OnFeedError(srcType string, r cbgt.Feed,
	err error) {
}

func TestNewRESTRouter(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	ring, err := cbgt.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		t.Errorf("expected no ring errors")
	}

	cfg := cbgt.NewCfgMem()
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000",
		emptyDir, "some-datasource", nil)
	r, meta, err := NewRESTRouter("v0", mgr, emptyDir, "", ring, nil)
	if r == nil || meta == nil || err != nil {
		t.Errorf("expected no errors")
	}

	mgr = cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		[]string{"queryer", "anotherTag"},
		"", 1, "", ":1000", emptyDir, "some-datasource", nil)
	r, meta, err = NewRESTRouter("v0", mgr, emptyDir, "", ring, nil)
	if r == nil || meta == nil || err != nil {
		t.Errorf("expected no errors")
	}
}

type RESTHandlerTest struct {
	Desc           string
	Path           string
	Method         string
	Params         url.Values
	Body           []byte
	Status         int
	ResponseBody   []byte
	ResponseMatch  map[string]bool
	Header         map[string]string
	ResponseHeader map[string]string

	Before func()
	After  func()
}

func (test *RESTHandlerTest) check(t *testing.T,
	record *httptest.ResponseRecorder) {
	desc := test.Desc
	if desc == "" {
		desc = test.Path + " " + test.Method
	}

	if got, want := record.Code, test.Status; got != want {
		t.Errorf("%s: response code = %d, want %d", desc, got, want)
		t.Errorf("%s: response body = %s", desc, record.Body)
	}
	got := bytes.TrimRight(record.Body.Bytes(), "\n")
	if test.ResponseBody != nil {
		if !bytes.Contains(got, test.ResponseBody) {
			t.Errorf("%s: expected: '%s', got: '%s'",
				desc, test.ResponseBody, got)
		}
	}
	for key, val := range test.ResponseHeader {
		v := record.Header().Get(key)
		if v != val {
			t.Errorf("%s: response header expected %s got %s", desc, val, v)
		}
	}
	for pattern, shouldMatch := range test.ResponseMatch {
		didMatch := bytes.Contains(got, []byte(pattern))
		if didMatch != shouldMatch {
			t.Errorf("%s: expected match %t for pattern %s, got %t",
				desc, shouldMatch, pattern, didMatch)
			t.Errorf("%s: response body was: %s", desc, got)
		}
	}
}

func testRESTHandlers(t *testing.T,
	tests []*RESTHandlerTest, router *mux.Router) {
	for _, test := range tests {
		if test.Before != nil {
			test.Before()
		}
		if test.Method != "NOOP" {
			req := &http.Request{
				Method: test.Method,
				URL:    &url.URL{Path: test.Path},
				Form:   test.Params,
				Header: make(http.Header),
				Body:   ioutil.NopCloser(bytes.NewBuffer(test.Body)),
			}
			for key, val := range test.Header {
				req.Header.Set(key, val)
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

func TestHandlersForRuntimeOps(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Path:   "/api/runtime",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				"arch":       true,
				"go":         true,
				"GOMAXPROCS": true,
				"GOROOT":     true,
			},
		},
		{
			Path:          "/api/runtime/args",
			Method:        "GET",
			Params:        nil,
			Body:          nil,
			Status:        http.StatusOK,
			ResponseMatch: map[string]bool{
			// Actual production args are different from "go test" context.
			},
		},
		{
			Path:         "/api/runtime/gc",
			Method:       "POST",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(nil),
		},
		{
			Path:   "/api/runtime/statsMem",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				"Alloc":      true,
				"TotalAlloc": true,
			},
		},
		{
			Path:   "/api/runtime/profile/cpu",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				"incorrect or missing secs parameter": true,
			},
		},
		{
			Path:   "/api/runtime/trace",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				"incorrect or missing secs parameter": true,
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersForEmptyManager(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	mgr.AddEvent([]byte(`"fizz"`))
	mgr.AddEvent([]byte(`"buzz"`))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
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
			ResponseBody: []byte(`{"messages":["hello","world"],"events":["fizz","buzz"]}`),
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
			Desc:         "Sets the given node definitions in Cfg ",
			Path:         "/api/cfgNodeDefs",
			Method:       "PUT",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusBadRequest,
			ResponseBody: []byte(`rest_manage: no request body found`),
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
			Path:   "/api/stats",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{`: true,
				`}`: true,
			},
		},
		{
			Desc:   "diag on empty manaager",
			Path:   "/api/diag",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`/api/cfg`:         true,
				`/api/stats`:       true,
				`/api/log`:         true,
				`/api/managerMeta`: true,
				`/api/runtime`:     true,
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
			ResponseBody: []byte(`index not found`),
		},
		{
			Desc:   "try to create a default index with no params",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`rest_create_index: indexType is required`: true,
			},
		},
		{
			Desc:   "try to create a default index with no sourceType",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: url.Values{
				"indexType": []string{"fulltext-index"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`rest_create_index: sourceType is required`: true,
			},
		},
		{
			Desc:   "try to create a default index with bad server",
			Path:   "/api/index/index-on-a-bad-server",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"fulltext-index"},
				"sourceType": []string{"couchbase"},
			},
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
				`no indexes`: true,
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

func TestHandlersForOneBleveIndexWithNILFeed(t *testing.T) {
	testHandlersForOneBleveTypeIndexWithNILFeed(t, "fulltext-index")
}

func testHandlersForOneBleveTypeIndexWithNILFeed(t *testing.T,
	indexType string) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "shelf/rack/row", 1, "", ":1000",
		emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "create an index with bad indexName",
			Path:   "/api/index/%#^badIndexName",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{indexType},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error creating index`: true,
				`indexName is invalid`: true,
			},
		},
		{
			Desc:   "create an index with whitespace in indexName",
			Path:   "/api/index/another bad index name",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{indexType},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`error creating index`: true,
				`indexName is invalid`: true,
			},
		},
		{
			Desc:   "create an index with nil feed",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{indexType},
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
			Path:   "/api/stats",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{`:      true,
				`"idx0_`: true,
				`}`:      true,
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
				`{"status":"ok","indexDefs":{"uuid":`:          true,
				`"indexDefs":{"idx0":{"type":"fulltext-index"`: true,
				`"params":null`:                                true,
				`"sourceType":"nil"`:                           true,
				`"sourceName":`:                                false,
				`"sourceUUID":`:                                false,
				`"sourceParams":null`:                          true,
				`"planParams":{`:                               true,
				`"maxPartitionsPerPIndex":20`:                  true,
				`"numReplicas":`:                               false,
				`"hierarchyRules"`:                             false,
				`"nodePlanParams":`:                            false,
				`"pindexWeights":`:                             false,
				`"planFrozen":`:                                false,
			},
		},
		{
			Desc:         "try to get a nonexistent index on a 1 index manager",
			Path:         "/api/index/NOT-AN-INDEX",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       400,
			ResponseBody: []byte(`index not found`),
		},
		{
			Desc:   "try to delete a nonexistent index on a 1 index manager",
			Path:   "/api/index/NOT-AN-INDEX",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
				`index to delete missing`: true,
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
				`err`: true,
				`no indexDef, indexName: NOT-AN-INDEX`: true,
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
				`err`: true,
				`no indexDef, indexName: NOT-AN-INDEX`: true,
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
				`"status":"ok","indexDef":{"type":"fulltext-index"`: true,
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
			Body:   []byte(`{"query":{"query":"foo"}}`),
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
			ResponseBody: []byte(`index not found`),
		},
		{
			Desc:   "try to delete a deleted index",
			Path:   "/api/index/idx0",
			Method: "DELETE",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`index to delete missing, indexName: idx0`: true,
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
				"indexType":  []string{indexType},
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
				"indexType":   []string{indexType},
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

func TestHandlersWithOnePartitionPrimaryFeedIndex(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}
	var pindex *cbgt.PIndex

	var doneCh chan *httptest.ResponseRecorder

	var feed *cbgt.PrimaryFeed

	tests := []*RESTHandlerTest{
		{
			Desc:   "create an index with dest feed with bad sourceParams",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
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
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
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
					t.Errorf("expected to be 1 pindex,"+
						" got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed, ok = f.(*cbgt.PrimaryFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a PrimaryFeed")
					}
				}
				for _, p := range pindexes {
					pindex = p
					break
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(1)
				snapEnd := uint64(10)
				err = feed.SnapshotStart(partition, snapStart, snapEnd)
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 should be 0" +
				" when got 2nd update (another doc created)" +
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
				err = feed.SnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}

				WaitForPersistence(pindex, float64(2))

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
				`err`: true,
			},
		},
		{
			Desc:   "query for 0 hit",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"bar"}}`),
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
			Body:   []byte(`{"size":10,"query":{"query":"baz"}}`),
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
			Body:   []byte(`{"size":10,"query":{"query":"wow"}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"id":"hello"`:   true,
				`"id":"world"`:   true,
				`"total_hits":2`: true,
			},
		},
		{
			Desc:   "list pindex",
			Path:   "/api/pindex",
			Method: "GET",
			Status: 200,
			ResponseMatch: map[string]bool{
				`"pindexes"`:    true,
				`"status":"ok"`: true,
			},
		},
		{
			Desc:   "direct pindex query on bogus pindex",
			Path:   "/api/pindex/not-a-pindex/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"wow"}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`no pindex`: true,
			},
		},
		{
			Desc:   "direct pindex count on bogus pindex",
			Path:   "/api/pindex/not-a-pindex/count",
			Method: "GET",
			Status: 400,
			ResponseMatch: map[string]bool{
				`no pindex`: true,
			},
		},
		{
			Desc:   "direct pindex get on bogus pindex",
			Path:   "/api/pindex/not-a-pindex",
			Method: "GET",
			Status: 400,
			ResponseMatch: map[string]bool{
				`no pindex`: true,
			},
		},
		{
			Desc:   "direct pindex get",
			Method: "NOOP",
			After: func() {
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex,"+
						" got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				req := &http.Request{
					Method: "GET",
					URL:    &url.URL{Path: "/api/pindex/" + pindex.Name},
				}
				record := httptest.NewRecorder()
				router.ServeHTTP(record, req)
				test := &RESTHandlerTest{
					Desc:   "direct pindex get",
					Status: 200,
					ResponseMatch: map[string]bool{
						`pindex`:    true,
						`indexName`: true,
					},
				}
				test.check(t, record)
			},
		},
		{
			Desc:   "direct pindex query on mismatched pindex UUID",
			Method: "NOOP",
			After: func() {
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex,"+
						" got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				body := []byte(`{"size":10,"query":{"query":"wow"}}`)
				req := &http.Request{
					Method: "POST",
					URL: &url.URL{
						Path: "/api/pindex/" + pindex.Name + "/query",
					},
					Form: url.Values{
						"pindexUUID": []string{"not the right pindex UUID"},
					},
					Body: ioutil.NopCloser(bytes.NewBuffer(body)),
				}
				record := httptest.NewRecorder()
				router.ServeHTTP(record, req)
				test := &RESTHandlerTest{
					Desc:   "direct pindex query on mismatched pindex UUID",
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
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex,"+
						" got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				body := []byte(`{"size":10,"query":{"query":"wow"`)
				req := &http.Request{
					Method: "POST",
					URL: &url.URL{
						Path: "/api/pindex/" + pindex.Name + "/query",
					},
					Form: url.Values{},
					Body: ioutil.NopCloser(bytes.NewBuffer(body)),
				}
				record := httptest.NewRecorder()
				router.ServeHTTP(record, req)
				test := &RESTHandlerTest{
					Desc:   "direct pindex query with clipped requestBody",
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
				_, pindexes := mgr.CurrentMaps()
				if len(pindexes) != 1 {
					t.Errorf("expected to be 1 pindex,"+
						" got pindexes: %+v", pindexes)
				}
				for _, p := range pindexes {
					pindex = p
				}
				if pindex == nil {
					t.Errorf("expected to be a pindex")
				}
				body := []byte(`{"size":10,"query":{"query":"wow"}}`)
				req := &http.Request{
					Method: "POST",
					URL: &url.URL{
						Path: "/api/pindex/" + pindex.Name + "/query",
					},
					Form: url.Values(nil),
					Body: ioutil.NopCloser(bytes.NewBuffer(body)),
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
			Body:   []byte(`{"size":10,"query":{"query":"wow"},"ctl":{"consistency":{"level":"at_plus","vectors":{"idx0":{"0":1}}}}}`),
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
			Body:   []byte(`{"size":10,"query":{"query":"wow"},"ctl":{"consistency":{"level":"this is not your level","vectors":{"idx0":{"0":1}}}}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "query with bogus consistency params vectors",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"wow"},"ctl":{"consistency":{"level":"at_plus","vectors":["array","not","legal"]}}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "query with bogus consistency vectors bad partitionUUID",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"wow"},"ctl":{"consistency":{"level":"at_plus","vectors":{"idx0":{"0/badPartitionUUID":20}}}}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "query with empty consistency level (same as stale=ok)",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"wow"},"ctl":{"consistency":{"level":"","vectors":{"idx0":{"0":1000}}}}}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`hello`: true,
			},
		},
		{
			Desc:   "query with consistency params in the future",
			Method: "NOOP",
			Before: func() {
				doneCh = make(chan *httptest.ResponseRecorder)
				go func() {
					body := []byte(`{"size":10,"query":{"query":"boof"},"ctl":{"consistency":{"level":"at_plus","vectors":{"idx0":{"0":11}}}}}`)
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
					t.Errorf("expected block waiting for more mutations")
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
					t.Errorf("expected  still blocked with snapshot open")
				default:
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(21)
				snapEnd := uint64(30)
				err = feed.SnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
				WaitForPersistence(pindex, float64(3))
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
				"indexType":   []string{"fulltext-alias"},
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
				"indexType":   []string{"fulltext-alias"},
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
				`err`: true,
			},
		},
		{
			Desc:   "query for 0 hit aa0 query",
			Path:   "/api/index/aa0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"bar"}}`),
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
			Body:   []byte(`{"size":10,"query":{"query":"baz"}}`),
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
			Body:   []byte(`{"size":10,"query":{"query":"wow"}}`),
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
				"indexType":   []string{"fulltext-alias"},
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
			Desc:   "query for 0 hit aaBadTargetQuery",
			Path:   "/api/index/aaBadTarget/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"bar"}}`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
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
					t.Errorf("expected 0 pindex, got pindexes: %+v", pindexes)
				}
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestHandlersWithOnePartitionPrimaryFeedRollbackMoss(t *testing.T) {
	BlevePIndexAllowMossPrev := BlevePIndexAllowMoss
	BlevePIndexAllowMoss = true

	bleveConfigDefaultKVStorePrev := bleve.Config.DefaultKVStore
	bleve.Config.DefaultKVStore = "mossStore"

	bleveMossRegistryCollectionOptionsFTSPrev := bleveMoss.RegistryCollectionOptions["fts"]
	bleveMoss.RegistryCollectionOptions["fts"] = moss.CollectionOptions{}

	defer func() {
		BlevePIndexAllowMoss = BlevePIndexAllowMossPrev
		bleve.Config.DefaultKVStore = bleveConfigDefaultKVStorePrev
		bleveMoss.RegistryCollectionOptions["fts"] = bleveMossRegistryCollectionOptionsFTSPrev
	}()

	testHandlersWithOnePartitionPrimaryFeedRollback(t)
}

func TestHandlersWithOnePartitionPrimaryFeedRollbackDefault(t *testing.T) {
	testHandlersWithOnePartitionPrimaryFeedRollback(t)
}

func testHandlersWithOnePartitionPrimaryFeedRollback(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	var doneCh chan *httptest.ResponseRecorder

	var feed *cbgt.PrimaryFeed

	var pindex *cbgt.PIndex

	tests := []*RESTHandlerTest{
		{
			Desc:   "create dest feed with 1 partition for rollback",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
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
					t.Errorf("expected to be 1 pindex,"+
						" got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed, ok = f.(*cbgt.PrimaryFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a PrimaryFeed")
					}
				}

				for _, p := range pindexes {
					pindex = p
					break
				}
			},
		},
		{
			Before: func() {
				partition := "0"
				snapStart := uint64(1)
				snapEnd := uint64(10)
				err = feed.SnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
			},
			Desc:   "count idx0 0 when snapshot just started, pre-rollback",
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 0 when got 2nd update (another doc created)" +
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
				err = feed.DataUpdate(partition, key, seq, val,
					0, cbgt.DEST_EXTRAS_TYPE_NIL, nil)
				if err != nil {
					t.Errorf("expected no err on data-udpate")
				}
			},
			Desc: "count idx0 0 when got 3rd update, pre-rollback" +
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
				err = feed.SnapshotStart(partition, snapStart, snapEnd)
				if err != nil {
					t.Errorf("expected no err on snapshot-start")
				}
				WaitForPersistence(pindex, float64(2))
			},
			Desc:   "count idx0 2 when 1st snapshot ended, pre-rollback",
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
			Desc:   "query with consistency params in future, pre-rollback",
			Method: "NOOP",
			Before: func() {
				doneCh = make(chan *httptest.ResponseRecorder)
				go func() {
					body := []byte(`{"size":10,"query":{"query":"boof"},"ctl":{"consistency":{"level":"at_plus","vectors":{"idx0":{"0":11}}}}}`)
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
					t.Errorf("expected block waiting for more mutations")
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
				// NOTE: We might check right after rollback but before
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
					Desc:   "test consistency wait had right result",
					Status: 400,
					ResponseMatch: map[string]bool{
						`err`: true,
					},
				}
				test.check(t, record)
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestCreateIndexTwoNodes(t *testing.T) {
	cfg := cbgt.NewCfgMem()

	emptyDir0, _ := ioutil.TempDir("./tmp", "test")
	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir0)
	defer os.RemoveAll(emptyDir1)

	meh0 := &TestMEH{}
	meh1 := &TestMEH{}
	mgr0 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:1000", emptyDir0, "some-datasource", meh0)
	mgr1 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:2000", emptyDir1, "some-datasource", meh1)

	mgr0.Start("wanted")
	mgr1.Start("wanted")
	mgr0.Kick("test-start-kick")
	mgr1.Kick("test-start-kick")

	mr0, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr1, _ := cbgt.NewMsgRing(os.Stderr, 1000)

	router0, _, err := NewRESTRouter("v0", mgr0, "static", "", mr0, nil)
	if err != nil || router0 == nil {
		t.Errorf("no mux router")
	}
	router1, _, err := NewRESTRouter("v0", mgr1, "static", "", mr1, nil)
	if err != nil || router1 == nil {
		t.Errorf("no mux router")
	}

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
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

	var feed0 *cbgt.PrimaryFeed
	var feed1 *cbgt.PrimaryFeed

	httpGetPrev := HttpGet
	defer func() { HttpGet = httpGetPrev }()

	HttpGet = func(client *http.Client, urlStr string) (
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

	httpPostPrev := HttpPost
	defer func() { HttpPost = httpPostPrev }()

	HttpPost = func(client *http.Client,
		urlStr string, bodyType string, body io.Reader) (
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
			Desc:   "create index with dest feed 2 partitions, 2 nodes",
			Path:   "/api/index/myIdx",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
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

				runtime.Gosched()

				mgr0.Kick("kick after index create")
				mgr1.Kick("kick after index create")

				feeds, pindexes := mgr0.CurrentMaps()
				if len(feeds) != 1 {
					t.Errorf("expected 1 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 1 {
					t.Errorf("expected 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed0, ok = f.(*cbgt.PrimaryFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a PrimaryFeed")
					}
				}
				for _, p := range pindexes {
					if p.IndexName != "myIdx" {
						t.Errorf("expected p.IndexName to match on mgr0")
					}
				}

				feeds, pindexes = mgr1.CurrentMaps()
				if len(feeds) != 1 {
					t.Errorf("expected 1 feed, got feeds: %+v", feeds)
				}
				if len(pindexes) != 1 {
					t.Errorf("expected 1 pindex, got pindexes: %+v", pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed1, ok = f.(*cbgt.PrimaryFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a PrimaryFeed")
					}
				}
				for _, p := range pindexes {
					if p.IndexName != "myIdx" {
						t.Errorf("expected p.IndexName to match on mgr1")
					}
				}

				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
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
			Body:   []byte(`{"size":10,"query":{"query":"no-hits"}}`),
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
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
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
				"indexType":   []string{"fulltext-alias"},
				"indexParams": []string{`{"targets":{"myIdx":{},"yourIdx":{}}}`},
				"sourceType":  []string{"nil"},
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

				runtime.Gosched()

				mgr0.Kick("kick after alias create")
				mgr1.Kick("kick after alias create")
			},
		},
		{
			Desc:   "create an alias without target",
			Path:   "/api/index/emptyAlias",
			Method: "PUT",
			Params: url.Values{
				"indexType":   []string{"fulltext-alias"},
				"indexParams": []string{`{"targets":{}}`},
				"sourceType":  []string{"nil"},
			},
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`no index targets were specified`: true,
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
			Desc:   "count myAlias 0, 2 nodes, with consistency params",
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
			Body:   []byte(`{"size":10,"query":{"query":"no-hits"}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
	}

	testRESTHandlers(t, tests, router0)
}

func testCreateIndex1Node(t *testing.T, planParams []string,
	expNumPIndexes, expNumFeeds int) (
	cbgt.Cfg, *cbgt.Manager, *mux.Router) {
	cfg := cbgt.NewCfgMem()

	emptyDir0, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir0)

	meh0 := &TestMEH{}
	options0 := map[string]string{"maxReplicasAllowed": "1"}
	mgr0 := cbgt.NewManagerEx(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:1000", emptyDir0, "some-datasource",
		meh0, options0)
	mgr0.Start("wanted")
	mgr0.Kick("test-start-kick")
	mr0, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	router0, _, err := NewRESTRouter("v0", mgr0, "static", "", mr0, nil)
	if err != nil || router0 == nil {
		t.Errorf("no mux router")
	}

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 1 {
		t.Errorf("expected 1 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 1 {
		t.Errorf("expected 1 node defs wanted")
	}

	mgr0.Kick("test-start-kick-again")

	var feed0 *cbgt.PrimaryFeed

	httpGetPrev := HttpGet
	defer func() { HttpGet = httpGetPrev }()

	HttpGet = func(client *http.Client, urlStr string) (
		resp *http.Response, err error) {
		u, _ := url.Parse(urlStr)
		req := &http.Request{
			Method: "GET",
			URL:    u,
			Body:   ioutil.NopCloser(bytes.NewBuffer([]byte{})),
		}
		record := httptest.NewRecorder()
		router0.ServeHTTP(record, req)
		return &http.Response{
			StatusCode: record.Code,
			Body:       ioutil.NopCloser(record.Body),
		}, nil
	}

	httpPostPrev := HttpPost
	defer func() { HttpPost = httpPostPrev }()

	HttpPost = func(client *http.Client,
		urlStr string, bodyType string, body io.Reader) (
		resp *http.Response, err error) {
		u, _ := url.Parse(urlStr)
		req := &http.Request{
			Method: "POST",
			URL:    u,
			Body:   ioutil.NopCloser(body),
		}
		record := httptest.NewRecorder()
		router0.ServeHTTP(record, req)
		return &http.Response{
			StatusCode: record.Code,
			Body:       ioutil.NopCloser(record.Body),
		}, nil
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "create index with dest feedm 1 partitions, 1 nodes",
			Path:   "/api/index/myIdx",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"indexParams":  []string{"{}"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":2}`},
				"planParams":   planParams,
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

				runtime.Gosched()

				mgr0.Kick("kick after index create")

				feeds, pindexes := mgr0.CurrentMaps()
				if len(feeds) != expNumFeeds {
					t.Errorf("expected to be %d feed, got feeds: %+v",
						expNumFeeds, feeds)
				}
				if len(pindexes) != expNumPIndexes {
					t.Errorf("expected to be %d pindex, got pindexes: %+v",
						expNumPIndexes, pindexes)
				}
				for _, f := range feeds {
					var ok bool
					feed0, ok = f.(*cbgt.PrimaryFeed)
					if !ok {
						t.Errorf("expected the 1 feed to be a PrimaryFeed")
					}
				}
				for _, p := range pindexes {
					if p.IndexName != "myIdx" {
						t.Errorf("expected p.IndexName to match on mgr0")
					}
				}

				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
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
	}

	testRESTHandlers(t, tests, router0)

	return cfg, mgr0, router0
}

func TestCreateIndexAddNode(t *testing.T) {
	cfg, mgr0, router0 := testCreateIndex1Node(t,
		[]string{`{"maxPartitionsPerPIndex":1}`}, 2, 1)

	tests := []*RESTHandlerTest{
		{
			Desc:   "count myIdx should be 0, 1 nodes",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "query myIdx should have 0 hits, 1 nodes",
			Path:   "/api/index/myIdx/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"no-hits"}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
	}
	testRESTHandlers(t, tests, router0)

	planPIndexesPrev, casPrev, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}

	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir1)

	meh1 := &TestMEH{}
	mgr1 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:2000",
		emptyDir1, "some-datasource", meh1)
	mgr1.Start("wanted")
	mgr1.Kick("test-start-kick")

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs wanted")
	}

	if cfg.Refresh() != nil {
		t.Errorf("expected cfg refresh to work")
	}

	runtime.Gosched()

	mgr0.Kick("test-kick-after-new-node")

	planPIndexesCurr, casCurr, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}
	if casPrev >= casCurr {
		t.Errorf("expected diff casPrev: %d, casCurr: %d",
			casPrev, casCurr)
	}
	if cbgt.SamePlanPIndexes(planPIndexesPrev, planPIndexesCurr) {
		planPIndexesPrevJS, _ := json.Marshal(planPIndexesPrev)
		planPIndexesCurrJS, _ := json.Marshal(planPIndexesCurr)
		t.Errorf("expected diff plans,"+
			" planPIndexesPrev: %s, planPIndexesCurr: %s",
			planPIndexesPrevJS, planPIndexesCurrJS)
	}
}

func TestCreateIndex1PIndexAddNode(t *testing.T) {
	cfg, mgr0, router0 := testCreateIndex1Node(t,
		[]string{`{"maxPartitionsPerPIndex":100}`}, 1, 1)

	tests := []*RESTHandlerTest{
		{
			Desc:   "count myIdx should be 0, 1 nodes",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
		{
			Desc:   "query myIdx should have 0 hits, 1 nodes",
			Path:   "/api/index/myIdx/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{"size":10,"query":{"query":"no-hits"}}`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`"hits":[],"total_hits":0`: true,
			},
		},
	}
	testRESTHandlers(t, tests, router0)

	planPIndexesPrev, casPrev, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}

	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir1)

	meh1 := &TestMEH{}
	mgr1 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:2000",
		emptyDir1, "some-datasource", meh1)
	mgr1.Start("wanted")
	mgr1.Kick("test-start-kick")

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs wanted")
	}

	if cfg.Refresh() != nil {
		t.Errorf("expected cfg refresh to work")
	}

	runtime.Gosched()

	mgr0.Kick("test-kick-after-new-node")

	planPIndexesCurr, casCurr, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}
	if casPrev != casCurr {
		t.Errorf("expected same casPrev: %d, casCurr: %d", casPrev, casCurr)
	}
	if !cbgt.SamePlanPIndexes(planPIndexesPrev, planPIndexesCurr) {
		planPIndexesPrevJS, _ := json.Marshal(planPIndexesPrev)
		planPIndexesCurrJS, _ := json.Marshal(planPIndexesCurr)
		t.Errorf("expected same plans,"+
			" planPIndexesPrev: %s, planPIndexesCurr: %s",
			planPIndexesPrevJS, planPIndexesCurrJS)
	}

	n := 0
	for _, pindex := range planPIndexesCurr.PlanPIndexes {
		for nodeUUID, planPIndexNode := range pindex.Nodes {
			n++
			if !planPIndexNode.CanRead || !planPIndexNode.CanWrite {
				t.Errorf("expected readable, writable,"+
					" nodeUUID: %s", nodeUUID)
			}
		}
	}
	if n <= 0 {
		t.Errorf("expected some pindexes")
	}
}

func TestCreateIndexPlanFrozenAddNode(t *testing.T) {
	cfg, mgr0, router0 := testCreateIndex1Node(t,
		[]string{`{"maxPartitionsPerPIndex":1,"planFrozen":true}`}, 0, 0)

	tests := []*RESTHandlerTest{
		{
			Desc:   "count myIdx should be 0, 1 nodes",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Status: 500,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
	}
	testRESTHandlers(t, tests, router0)

	planPIndexesPrev, casPrev, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}

	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir1)

	meh1 := &TestMEH{}
	mgr1 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:2000", emptyDir1, "some-datasource", meh1)
	mgr1.Start("wanted")
	mgr1.Kick("test-start-kick")

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs wanted")
	}

	if cfg.Refresh() != nil {
		t.Errorf("expected cfg refresh to work")
	}

	runtime.Gosched()

	mgr0.Kick("test-kick-after-new-node")

	planPIndexesCurr, casCurr, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}
	if casPrev != casCurr {
		t.Errorf("expected same casPrev: %d, casCurr: %d", casPrev, casCurr)
	}
	if !cbgt.SamePlanPIndexes(planPIndexesPrev, planPIndexesCurr) {
		planPIndexesPrevJS, _ := json.Marshal(planPIndexesPrev)
		planPIndexesCurrJS, _ := json.Marshal(planPIndexesCurr)
		t.Errorf("expected same plans,"+
			" planPIndexesPrev: %s, planPIndexesCurr: %s",
			planPIndexesPrevJS, planPIndexesCurrJS)
	}
}

func TestCreateIndexThenFreezePlanThenAddNode(t *testing.T) {
	cfg, mgr0, router0 := testCreateIndex1Node(t,
		[]string{`{"maxPartitionsPerPIndex":1}`}, 2, 1)

	tests := []*RESTHandlerTest{
		{
			Desc:   "count myIdx should be 0, 1 nodes",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
	}
	testRESTHandlers(t, tests, router0)

	indexDefs, indexDefsCas, err := cbgt.CfgGetIndexDefs(cfg)
	if err != nil {
		t.Errorf("error CfgGetIndexDefs: %v", err)
	}
	indexDefs.IndexDefs["myIdx"].PlanParams.PlanFrozen = true
	_, err = cbgt.CfgSetIndexDefs(cfg, indexDefs, indexDefsCas)
	if err != nil {
		t.Errorf("expected CfgSetIndexDefs for plan freeze to work")
	}

	planPIndexesPrev, casPrev, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}

	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir1)

	meh1 := &TestMEH{}
	mgr1 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:2000", emptyDir1,
		"some-datasource", meh1)
	mgr1.Start("wanted")
	mgr1.Kick("test-start-kick")

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs wanted")
	}

	if cfg.Refresh() != nil {
		t.Errorf("expected cfg refresh to work")
	}

	runtime.Gosched()

	mgr0.Kick("test-kick-after-new-node")

	planPIndexesCurr, casCurr, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}
	if casPrev != casCurr {
		t.Errorf("expected same casPrev: %d, casCurr: %d",
			casPrev, casCurr)
	}
	if !cbgt.SamePlanPIndexes(planPIndexesPrev, planPIndexesCurr) {
		planPIndexesPrevJS, _ := json.Marshal(planPIndexesPrev)
		planPIndexesCurrJS, _ := json.Marshal(planPIndexesCurr)
		t.Errorf("expected same plans, planPIndexesPrev: %s, planPIndexesCurr: %s",
			planPIndexesPrevJS, planPIndexesCurrJS)
	}
}

func TestNodePlanParams(t *testing.T) {
	cfg, mgr0, router0 := testCreateIndex1Node(t,
		[]string{`{"maxPartitionsPerPIndex":1,"numReplicas":1,"nodePlanParams":{"":{"":{"canRead":false,"canWrite":false}}}}`},
		2, 0)

	tests := []*RESTHandlerTest{
		{
			Desc:   "count myIdx should be 0, 1 nodes",
			Path:   "/api/index/myIdx/count",
			Method: "GET",
			Status: 200,
			ResponseMatch: map[string]bool{
				`{"status":"ok","count":0}`: true,
			},
		},
	}
	testRESTHandlers(t, tests, router0)

	planPIndexesPrev, casPrev, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}

	emptyDir1, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir1)

	meh1 := &TestMEH{}
	mgr1 := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", "localhost:2000", emptyDir1,
		"some-datasource", meh1)
	mgr1.Start("wanted")
	mgr1.Kick("test-start-kick")

	nd, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		t.Errorf("expected node defs known")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs unknown")
	}

	nd, _, err = cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_WANTED)
	if err != nil {
		t.Errorf("expected node defs wanted")
	}
	if len(nd.NodeDefs) != 2 {
		t.Errorf("expected 2 node defs wanted")
	}

	if cfg.Refresh() != nil {
		t.Errorf("expected cfg refresh to work")
	}

	runtime.Gosched()

	mgr0.Kick("test-kick-after-new-node")

	planPIndexesCurr, casCurr, err := cbgt.CfgGetPlanPIndexes(cfg)
	if err != nil {
		t.Errorf("expected no err")
	}
	if casPrev >= casCurr {
		t.Errorf("expected diff casPrev: %d, casCurr: %d",
			casPrev, casCurr)
	}
	if cbgt.SamePlanPIndexes(planPIndexesPrev, planPIndexesCurr) {
		planPIndexesPrevJS, _ := json.Marshal(planPIndexesPrev)
		planPIndexesCurrJS, _ := json.Marshal(planPIndexesCurr)
		t.Errorf("expected diff plans,"+
			" planPIndexesPrev: %s, planPIndexesCurr: %s",
			planPIndexesPrevJS, planPIndexesCurrJS)
	}

	n := 0
	for _, pindex := range planPIndexesCurr.PlanPIndexes {
		for nodeUUID, planPIndexNode := range pindex.Nodes {
			n++
			if planPIndexNode.CanRead || planPIndexNode.CanWrite {
				t.Errorf("expected not readable, not writable,"+
					" nodeUUID: %s", nodeUUID)
			}
		}
	}
	if n <= 0 {
		t.Errorf("expected some pindexes")
	}
}

func TestHandlersForIndexControl(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "ingestControl on not-an-index",
			Path:   "/api/index/not-an-index/ingestControl/pause",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "queryControl on not-an-index",
			Path:   "/api/index/not-an-index/queryControl/disallow",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "create an index with nil feed",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":  []string{"fulltext-index"},
				"sourceType": []string{"nil"},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "ingestControl on not-an-index",
			Path:   "/api/index/not-an-index/ingestControl/pause",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Before: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams != nil {
					t.Errorf("expected nil plan params" +
						" before accessing index control")
				}
			},
			Desc:   "ingestControl real index, bad op",
			Path:   "/api/index/idx0/ingestControl/not-an-op",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`:         true,
				`unsupported`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams != nil {
					t.Errorf("expected nil plan params" +
						" after rejected index control")
				}
			},
		},
		{
			Desc:   "ingestControl real index, pause",
			Path:   "/api/index/idx0/ingestControl/pause",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`ok`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams == nil {
					t.Errorf("expected non-nil plan params")
				}
				if !indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams[""][""].CanRead {
					t.Errorf("expected readable")
				}
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams[""][""].CanWrite {
					t.Errorf("expected non-write")
				}
			},
		},
		{
			Desc:   "ingestControl real index, resume",
			Path:   "/api/index/idx0/ingestControl/resume",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`ok`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams == nil {
					t.Errorf("expected non-nil plan params")
				}
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams[""][""] != nil {
					t.Errorf("expected nil sub plan params after resume")
				}
			},
		},
		{
			Desc:   "queryControl on not-an-index",
			Path:   "/api/index/not-an-index/queryControl/disallow",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "queryControl real index, bad op",
			Path:   "/api/index/idx0/queryControl/not-an-op",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`:         true,
				`unsupported`: true,
			},
		},
		{
			Desc:   "queryControl real index, disallow",
			Path:   "/api/index/idx0/queryControl/disallow",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`ok`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams == nil {
					t.Errorf("expected non-nil plan params")
				}
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams[""][""].CanRead {
					t.Errorf("expected non-readable")
				}
				if !indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams[""][""].CanWrite {
					t.Errorf("expected write")
				}
			},
		},
		{
			Desc:   "queryControl real index, allow",
			Path:   "/api/index/idx0/queryControl/allow",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`ok`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams == nil {
					t.Errorf("expected non-nil plan params")
				}
				if indexDefs.IndexDefs["idx0"].PlanParams.NodePlanParams[""][""] != nil {
					t.Errorf("expected nil sub plan params after allow")
				}
			},
		},
		{
			Desc:   "planFreezeControl real index, freeze",
			Path:   "/api/index/idx0/planFreezeControl/freeze",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`ok`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if !indexDefs.IndexDefs["idx0"].PlanParams.PlanFrozen {
					t.Errorf("expected frozen")
				}
			},
		},
		{
			Desc:   "planFreezeControl real index, unfreeze",
			Path:   "/api/index/idx0/planFreezeControl/unfreeze",
			Method: "POST",
			Params: nil,
			Body:   nil,
			Status: 200,
			ResponseMatch: map[string]bool{
				`ok`: true,
			},
			After: func() {
				indexDefs, _, _ := cbgt.CfgGetIndexDefs(cfg)
				if indexDefs.IndexDefs["idx0"].PlanParams.PlanFrozen {
					t.Errorf("expected thawed")
				}
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestMultiFeedStats(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "create another index with primary feed",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "create another index with primary feed",
			Path:   "/api/index/idx1",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "feed stats on a multi-index manager",
			Path:   "/api/stats",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{`:      true,
				`"idx0_`: true,
				`"idx1_`: true,
				`}`:      true,
			},
		},
	}

	testRESTHandlers(t, tests, router)
}

func TestIndexDefWithJSON(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := cbgt.NewCfgMem()
	meh := &TestMEH{}
	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		nil, "", 1, "", ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := cbgt.NewMsgRing(os.Stderr, 1000)
	mr.Write([]byte("hello"))
	mr.Write([]byte("world"))

	router, _, err := NewRESTRouter("v0", mgr, "static", "", mr, nil)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "try create index with bad JSON",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body:   []byte(`BAAAAD json! :-{`),
			Status: 400,
			ResponseMatch: map[string]bool{
				`err`: true,
			},
		},
		{
			Desc:   "create index with primary feed",
			Path:   "/api/index/idx0",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body: []byte(`{ "indexType": "fulltext-index",
                            "indexParams": "{}",
                            "sourceType": "primary",
                            "sourceUUID": "beefbeef",
                            "sourceParams": "{\"numPartitions\":10}"
                          }`),
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "create index with primary feed",
			Path:   "/api/index/idx00",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body: []byte(`{ "indexType": "fulltext-index",
                            "indexParams": "{}",
                            "sourceType": "primary",
                            "sourceUUID": "beefbeef",
                            "sourceParams": "{\"numPartitions\":10}"
                          }`),
			ResponseHeader: map[string]string{"Content-type": "application/json;version=" + API_MAX_VERSION},
			Status:         http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok"}`: true,
			},
		},
		{
			Desc:   "create index with primary feed1",
			Path:   "/api/index/idx01",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body: []byte(`{ "indexType": "fulltext-index",
                            "indexParams": "{}",
                            "sourceType": "primary",
                            "sourceUUID": "beefbeef",
                            "sourceParams": "{\"numPartitions\":10}"
                          }`),
			Header:       map[string]string{"Accept": "version=3.0.0"},
			ResponseBody: []byte(fmt.Sprintf(`["application/json;version=%s","application/json;version=0.0.0"]`, API_MAX_VERSION)),
			Status:       406,
		},
		{
			Desc:   "create index with primary feed1",
			Path:   "/api/index/idx02",
			Method: "PUT",
			Params: url.Values{
				"indexType":    []string{"fulltext-index"},
				"sourceType":   []string{"primary"},
				"sourceParams": []string{`{"numPartitions":10}`},
			},
			Body: []byte(`{ "indexType": "fulltext-index",
                            "indexParams": "{}",
                            "sourceType": "primary",
                            "sourceUUID": "beefbeef",
                            "sourceParams": "{\"numPartitions\":10}"
                          }`),
			ResponseHeader: map[string]string{"Content-type": "application/json;version=1.0.0"},
			Header:         map[string]string{"Accept": "version=1.0.0"},
			Status:         http.StatusOK,
		},
	}

	testRESTHandlers(t, tests, router)
}

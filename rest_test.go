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

	router, err := NewManagerRESTRouter(mgr, "static", mr)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []struct {
		Desc          string
		Path          string
		Method        string
		Params        url.Values
		Body          []byte
		Status        int
		ResponseBody  []byte
		ResponseMatch map[string]bool
	}{
		{
			Desc:         "log on empty msg ring",
			Path:         "/api/log",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"messages":[]}`),
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
			Status: 500,
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

	for _, test := range tests {
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
	}
}

func TestHandlersForOneIndex(t *testing.T) {
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

	tests := []struct {
		Desc          string
		Path          string
		Method        string
		Params        url.Values
		Body          []byte
		Status        int
		ResponseBody  []byte
		ResponseMatch map[string]bool
	}{
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
	}

	for _, test := range tests {
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
	}
}

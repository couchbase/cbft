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

// +build go1.4,vlite

package cbft

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"testing"
)

func TestHandlersForOneVLiteIndexWithNILFeed(t *testing.T) {
	if PIndexImplTypes["vlite"] != nil {
		testHandlersForOneVLiteTypeIndexWithNILFeed(t, "vlite")
	}
}

func TestHandlersForOneVLiteMemIndexWithNILFeed(t *testing.T) {
	if PIndexImplTypes["vlite-mem"] != nil {
		testHandlersForOneVLiteTypeIndexWithNILFeed(t, "vlite-mem")
	}
}

func testHandlersForOneVLiteTypeIndexWithNILFeed(t *testing.T, indexType string) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "shelf/rack/row", 1, ":1000", emptyDir, "some-datasource", meh)
	mgr.Start("wanted")
	mgr.Kick("test-start-kick")

	mr, _ := NewMsgRing(os.Stderr, 1000)

	router, _, err := NewManagerRESTRouter("v0", mgr, "static", "", mr)
	if err != nil || router == nil {
		t.Errorf("no mux router")
	}

	tests := []*RESTHandlerTest{
		{
			Desc:   "create a vlite type index with nil feed",
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
			Desc:   "feed stats on a 1 vlite index manager",
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
			Desc:   "list on a 1 vlite index manager",
			Path:   "/api/index",
			Method: "GET",
			Params: nil,
			Body:   nil,
			Status: http.StatusOK,
			ResponseMatch: map[string]bool{
				`{"status":"ok","indexDefs":{"uuid":`: true,
				`"indexDefs":{"idx0":{"type":"vlite`:  true,
			},
		},
		{
			Desc:   "count empty idx0 on a 1 vlite index manager",
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
			Desc:   "query empty idx0 on a 1 vlite index manager with missing args",
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
			Desc:   "query empty idx0 on a 1 vlite index manager with missing args",
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
			Desc:   "no-hit query of empty idx0 on a 1 vlite index manager",
			Path:   "/api/index/idx0/query",
			Method: "POST",
			Params: nil,
			Body:   []byte(`{}`),
			Status: 200,
			ResponseMatch: map[string]bool{
				`"results":[]`: true,
			},
		},
		{
			Desc:   "delete idx0 on a 1 vlite index manager",
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
			Desc:   "list indexes after delete one & only vlite index",
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
	}

	testRESTHandlers(t, tests, router)
}

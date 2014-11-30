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

func TestHandlers(t *testing.T) {
	emptyDir, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(emptyDir)

	cfg := NewCfgMem()
	meh := &TestMEH{}
	mgr := NewManager(VERSION, cfg, NewUUID(),
		nil, "", 1, ":1000", emptyDir, "some-datasource", meh)
	mr, _ := NewMsgRing(os.Stderr, 1000)

	tests := []struct {
		Desc          string
		Handler       http.Handler
		Path          string
		Method        string
		Params        url.Values
		Body          []byte
		Status        int
		ResponseBody  []byte
		ResponseMatch map[string]bool
	}{
		{
			Desc:         "log",
			Handler:      NewGetLogHandler(mr),
			Path:         "/api/log",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"messages":[]}`),
		},
		{
			Desc:         "list empty indexes",
			Handler:      NewListIndexHandler(mgr),
			Path:         "/api/index",
			Method:       "GET",
			Params:       nil,
			Body:         nil,
			Status:       http.StatusOK,
			ResponseBody: []byte(`{"status":"ok","indexDefs":null}`),
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
		test.Handler.ServeHTTP(record, req)
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

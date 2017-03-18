//  Copyright (c) 2016 Couchbase, Inc.
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
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

var testIndexDefsByName = map[string]*cbgt.IndexDef{
	"i1": &cbgt.IndexDef{
		Type:       "fulltext-index",
		SourceName: "s1",
	},
	"i2": &cbgt.IndexDef{
		Type:       "fulltext-index",
		SourceName: "s2",
	},
	"a1": &cbgt.IndexDef{
		Type:   "fulltext-alias",
		Params: `{"targets":{"i1":{}}}`,
	},
	"a2": &cbgt.IndexDef{
		Type:   "fulltext-alias",
		Params: `{"targets":{"i1":{},"i2":{}}}`,
	},
	"a3": &cbgt.IndexDef{
		Type:   "fulltext-alias",
		Params: `{"targets":{"a1":{},"i2":{}}}`,
	},
	"a4": &cbgt.IndexDef{
		Type:   "fulltext-alias",
		Params: `{"targets":{"a4":{},"i2":{}}}`,
	},
}

var testPIndexesByName = map[string]*cbgt.PIndex{
	"p1": &cbgt.PIndex{
		SourceName: "s3",
	},
}

func TestSourceNamesForAlias(t *testing.T) {
	tests := []struct {
		alias   string
		sources []string
		err     error
	}{
		// no such definition exists
		{
			alias:   "x",
			sources: []string(nil),
		},
		// not an alias
		{
			alias:   "i1",
			sources: []string(nil),
		},
		// alias to 1
		{
			alias:   "a1",
			sources: []string{"s1"},
		},
		// alias to multiple
		{
			alias:   "a2",
			sources: []string{"s1", "s2"},
		},
		// alias to another alias and index
		{
			alias:   "a3",
			sources: []string{"s1", "s2"},
		},
		// alias with loop
		{
			alias: "a4",
			err:   errAliasExpansionTooDeep,
		},
	}

	for i, test := range tests {
		actualNames, err := sourceNamesForAlias(test.alias, testIndexDefsByName, 0)
		if err != test.err {
			t.Errorf("test %d, expected err %v, got err %v", i, test.err, err)
		}
		sort.Strings(actualNames)
		if !reflect.DeepEqual(actualNames, test.sources) {
			t.Errorf("test %d, expected %#v, got %#v", i, test.sources, actualNames)
		}
	}
}

type stubDefinitionLookuper struct {
	pindexes map[string]*cbgt.PIndex
	defs     *cbgt.IndexDefs
}

func (s *stubDefinitionLookuper) GetPIndex(pindexName string) *cbgt.PIndex {
	return s.pindexes[pindexName]
}

func (s *stubDefinitionLookuper) GetIndexDefs(refresh bool) (
	*cbgt.IndexDefs, map[string]*cbgt.IndexDef, error) {
	return s.defs, s.defs.IndexDefs, nil
}

func TestSourceNamesFromReq(t *testing.T) {
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
	router.KeepContext = true // so we can see the mux vars

	s := &stubDefinitionLookuper{
		pindexes: testPIndexesByName,
		defs: &cbgt.IndexDefs{
			IndexDefs: testIndexDefsByName,
		},
	}

	tests := []struct {
		method  string
		uri     string
		path    string
		vars    map[string]string
		sources []string
		err     error
	}{
		// case with valid index name
		{
			method:  http.MethodGet,
			uri:     "/api/index/i1",
			path:    "/api/index/{indexName}",
			vars:    map[string]string{"indexName": "i1"},
			sources: []string{"s1"},
		},
		// case with invalid index name
		{
			method: http.MethodGet,
			uri:    "/api/index/x1",
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "x1"},
			err:    errIndexNotFound,
		},
		// case with invalid index name (actuall pindex name)
		{
			method: http.MethodGet,
			uri:    "/api/index/p1",
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "p1"},
			err:    errIndexNotFound,
		},
		// case with valid pindex name
		{
			method:  http.MethodGet,
			uri:     "/api/pindex/p1",
			path:    "/api/pindex/{pindexName}",
			vars:    map[string]string{"pindexName": "p1"},
			sources: []string{"s3"},
		},
		// case with invalid pindex name
		{
			method: http.MethodGet,
			uri:    "/api/pindex/y1",
			path:   "/api/pindex/{pindexName}",
			vars:   map[string]string{"pindexName": "y1"},
			err:    errPIndexNotFound,
		},
		// case with invalid pindex name (actually index name)
		{
			method: http.MethodGet,
			uri:    "/api/pindex/i1",
			path:   "/api/pindex/{pindexName}",
			vars:   map[string]string{"pindexName": "i1"},
			err:    errPIndexNotFound,
		},
		// case with valid alias, with operation that expands alias
		{
			method:  http.MethodGet,
			uri:     "/api/index/a1",
			path:    "/api/index/{indexName}",
			vars:    map[string]string{"indexName": "a1"},
			sources: []string{"s1"},
		},
		// case with valid alias, and this operation DOES expand alias
		{
			method:  http.MethodGet,
			uri:     "/api/index/a1/count",
			path:    "/api/index/{indexName}/count",
			vars:    map[string]string{"indexName": "a1"},
			sources: []string{"s1"},
		},
		// case with valid alias (multi), and this operation DOES expand alias
		{
			method:  http.MethodGet,
			uri:     "/api/index/a2/count",
			path:    "/api/index/{indexName}/count",
			vars:    map[string]string{"indexName": "a2"},
			sources: []string{"s1", "s2"},
		},
		// case with valid alias (containing another alias),
		// and this operation DOES expand alias
		{
			method:  http.MethodGet,
			uri:     "/api/index/a3/count",
			path:    "/api/index/{indexName}/count",
			vars:    map[string]string{"indexName": "a3"},
			sources: []string{"s1", "s2"},
		},
	}

	requestVariableLookupOrig := rest.RequestVariableLookup
	defer func() {
		rest.RequestVariableLookup = requestVariableLookupOrig
	}()

	for i, test := range tests {
		rest.RequestVariableLookup = func(req *http.Request, name string) string {
			if test.vars == nil {
				return ""
			}
			return test.vars[name]
		}

		req, err := http.NewRequest(test.method, test.uri, nil)
		if err != nil {
			t.Fatal(err)
		}

		// this actually executes things, which will usually fail
		// which is unrelated to what we're testing
		// but its the best i could do
		record := httptest.NewRecorder()
		router.ServeHTTP(record, req)

		actualNames, err := sourceNamesFromReq(s, req, test.method, test.path)
		if err != test.err {
			t.Errorf("test %d, expected err %v, got %v", i, test.err, err)
		}
		sort.Strings(actualNames)
		if !reflect.DeepEqual(actualNames, test.sources) {
			t.Errorf("test %d, expected %v, got %v", i, test.sources, actualNames)
		}
	}
}

func TestPreparePerms(t *testing.T) {
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
	router.KeepContext = true // so we can see the mux vars

	s := &stubDefinitionLookuper{
		pindexes: testPIndexesByName,
		defs: &cbgt.IndexDefs{
			IndexDefs: testIndexDefsByName,
		},
	}

	tests := []struct {
		method string
		uri    string
		body   []byte
		path   string
		vars   map[string]string
		perms  []string
		err    error
	}{
		// case with valid perm not containing source
		{
			method: http.MethodGet,
			uri:    "/api/index",
			path:   "/api/index",
			perms:  []string{"cluster.bucket.fts!read"},
		},
		// case with valid index name
		{
			method: http.MethodGet,
			uri:    "/api/index/i1",
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "i1"},
			perms:  []string{"cluster.bucket[s1].fts!read"},
		},
		// case with invalid index name
		{
			method: http.MethodGet,
			uri:    "/api/index/x1",
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "x1"},
			err:    errIndexNotFound,
		},
		// case with invalid index name (actuall pindex name)
		{
			method: http.MethodGet,
			uri:    "/api/index/p1",
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "p1"},
			err:    errIndexNotFound,
		},
		// case with valid pindex name
		{
			method: http.MethodGet,
			uri:    "/api/pindex/p1",
			path:   "/api/pindex/{pindexName}",
			vars:   map[string]string{"pindexName": "p1"},
			perms:  []string{"cluster.bucket[s3].fts!read"},
		},
		// case with invalid pindex name
		{
			method: http.MethodGet,
			uri:    "/api/pindex/y1",
			path:   "/api/pindex/{pindexName}",
			vars:   map[string]string{"pindexName": "y1"},
			err:    errPIndexNotFound,
		},
		// case with invalid pindex name (actually index name)
		{
			method: http.MethodGet,
			uri:    "/api/pindex/i1",
			path:   "/api/pindex/{pindexName}",
			vars:   map[string]string{"pindexName": "i1"},
			err:    errPIndexNotFound,
		},
		// case with valid alias, with operation that expands alias
		{
			method: http.MethodGet,
			uri:    "/api/index/a1",
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "a1"},
			perms:  []string{"cluster.bucket[s1].fts!read"},
		},
		// case with valid alias, and this operation DOES expand alias
		{
			method: http.MethodGet,
			uri:    "/api/index/a1/count",
			path:   "/api/index/{indexName}/count",
			vars:   map[string]string{"indexName": "a1"},
			perms:  []string{"cluster.bucket[s1].fts!read"},
		},
		// case with valid alias (multi), and this operation DOES expand alias
		{
			method: http.MethodGet,
			uri:    "/api/index/a2/count",
			path:   "/api/index/{indexName}/count",
			vars:   map[string]string{"indexName": "a2"},
			perms:  []string{"cluster.bucket[s1].fts!read", "cluster.bucket[s2].fts!read"},
		},
		// case with valid alias (containing another alias),
		// and this operation DOES expand alias
		{
			method: http.MethodGet,
			uri:    "/api/index/a3/count",
			path:   "/api/index/{indexName}/count",
			vars:   map[string]string{"indexName": "a3"},
			perms:  []string{"cluster.bucket[s1].fts!read", "cluster.bucket[s2].fts!read"},
		},
		// test special case for creating new index
		{
			method: http.MethodPut,
			uri:    "/api/index/anewone",
			body:   []byte(`{"type":"fulltext-index","sourceType":"couchbase","sourceName":"abucket"}`),
			path:   "/api/index/{indexName}",
			vars:   map[string]string{"indexName": "anewone"},
			perms:  []string{"cluster.bucket[abucket].fts!write"},
		},
	}

	requestVariableLookupOrig := rest.RequestVariableLookup
	defer func() {
		rest.RequestVariableLookup = requestVariableLookupOrig
	}()

	for i, test := range tests {
		rest.RequestVariableLookup = func(req *http.Request, name string) string {
			if test.vars == nil {
				return ""
			}
			return test.vars[name]
		}

		var r io.Reader
		if test.body != nil {
			r = bytes.NewBuffer(test.body)
		}
		req, err := http.NewRequest(test.method, test.uri, r)
		if err != nil {
			t.Fatal(err)
		}

		// this actually executes things, which will usually fail
		// which is unrelated to what we're testing
		// but its the best i could do
		record := httptest.NewRecorder()
		router.ServeHTTP(record, req)

		// set the request body again, as its been consumed :(
		if test.body != nil {
			req.Body = ioutil.NopCloser(bytes.NewBuffer(test.body))
		}

		actualPerms, err := preparePerms(s, req, test.method, test.path)
		if err != test.err {
			t.Errorf("test %d, expected err %v, got %v", i, test.err, err)
		}
		sort.Strings(actualPerms)
		if !reflect.DeepEqual(actualPerms, test.perms) {
			t.Errorf("test %d, expected %v, got %v", i, test.perms, actualPerms)
		}
	}
}

func TestPingAuth(t *testing.T) {
	path := "/api/ping"

	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		t.Fatal(err)
	}
	actualPerms, err := preparePerms(nil, req, "GET", path)

	if actualPerms != nil {
		t.Errorf("Invalid perms for ping %v, was not expecting any", actualPerms)
	}

	ok := CheckAPIAuth(nil, nil, req, path)
	if ok != true {
		t.Errorf("Not expecting auth failure for ping")
	}
}

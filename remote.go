//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"golang.org/x/net/context"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"

	"github.com/couchbase/cbgt"
)

var httpPost = http.Post // Overridable for unit-testability.
var httpGet = http.Get   // Overridable for unit-testability.

var indexClientUnimplementedErr = errors.New("unimplemented")

// IndexClient implements the Search() and DocCount() subset of the
// bleve.Index interface by accessing a remote cbft server via REST
// protocol.  This allows callers to add a IndexClient as a target of
// a bleve.IndexAlias, and implements cbft protocol features like
// query consistency and auth.
//
// TODO: Implement propagating auth info in IndexClient.
type IndexClient struct {
	mgr         *cbgt.Manager
	name        string
	QueryURL    string
	CountURL    string
	Consistency *cbgt.ConsistencyParams
}

func (r *IndexClient) Name() string {
	return r.name
}

func (r *IndexClient) SetName(name string) {
	r.name = name
}

func (r *IndexClient) Index(id string, data interface{}) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Delete(id string) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Batch(b *bleve.Batch) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Document(id string) (*document.Document, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) DocCount() (uint64, error) {
	if r.CountURL == "" {
		return 0, fmt.Errorf("remote: no CountURL provided")
	}
	u, err := UrlWithAuth(r.AuthType(), r.CountURL)
	if err != nil {
		return 0, fmt.Errorf("remote: auth for count,"+
			" docCountURL: %s, authType: %s, err: %v",
			r.CountURL, r.AuthType(), err)
	}
	resp, err := httpGet(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("remote: count got status code: %d,"+
			" docCountURL: %s, resp: %#v", resp.StatusCode, r.CountURL, resp)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("remote: count error reading resp.Body,"+
			" docCountURL: %s, resp: %#v, err: %v", r.CountURL, resp, err)
	}
	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{}
	err = json.Unmarshal(respBuf, &rv)
	if err != nil {
		return 0, fmt.Errorf("remote: count error parsing respBuf: %s,"+
			" docCountURL: %s, resp: %#v, err: %v",
			respBuf, r.CountURL, resp, err)
	}
	return rv.Count, nil
}

func (r *IndexClient) Search(req *bleve.SearchRequest) (
	*bleve.SearchResult, error) {
	return r.SearchInContext(context.Background(), req)
}

func (r *IndexClient) SearchInContext(ctx context.Context,
	req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("remote: no req provided")
	}

	if r.QueryURL == "" {
		return nil, fmt.Errorf("remote: no QueryURL provided")
	}

	queryCtlParams := &cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Consistency: r.Consistency,
		},
	}

	buf, err := json.Marshal(struct {
		*cbgt.QueryCtlParams
		*bleve.SearchRequest
	}{
		queryCtlParams,
		req,
	})
	if err != nil {
		return nil, err
	}

	respBuf, err := r.Query(buf)
	if err != nil {
		return nil, err
	}

	rv := &bleve.SearchResult{}
	err = json.Unmarshal(respBuf, rv)
	if err != nil {
		return nil, fmt.Errorf("remote: search error parsing respBuf: %s,"+
			" queryURL: %s, err: %v", respBuf, r.QueryURL, err)
	}
	return rv, nil
}

func (r *IndexClient) Fields() ([]string, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) FieldDict(field string) (index.FieldDict, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) FieldDictRange(field string,
	startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) DumpAll() chan interface{} {
	return nil
}

func (r *IndexClient) DumpDoc(id string) chan interface{} {
	return nil
}

func (r *IndexClient) DumpFields() chan interface{} {
	return nil
}

func (r *IndexClient) Close() error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Mapping() *bleve.IndexMapping {
	return nil
}

func (r *IndexClient) NewBatch() *bleve.Batch {
	return nil
}

func (r *IndexClient) Stats() *bleve.IndexStat {
	return nil
}

func (r *IndexClient) StatsMap() map[string]interface{} {
	return nil
}

func (r *IndexClient) GetInternal(key []byte) ([]byte, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) SetInternal(key, val []byte) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) DeleteInternal(key []byte) error {
	return indexClientUnimplementedErr
}

// -----------------------------------------------------

func (r *IndexClient) Count() (uint64, error) {
	return r.DocCount()
}

func (r *IndexClient) Query(buf []byte) ([]byte, error) {
	u, err := UrlWithAuth(r.AuthType(), r.QueryURL)
	if err != nil {
		return nil, fmt.Errorf("remote: auth for query,"+
			" queryURL: %s, authType: %s, err: %v",
			r.QueryURL, r.AuthType(), err)
	}
	resp, err :=
		httpPost(u, "application/json", bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("remote: query got status code: %d,"+
			" queryURL: %s, buf: %s, resp: %#v, err: %v",
			resp.StatusCode, r.QueryURL, buf, resp, err)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("remote: query error reading resp.Body,"+
			" queryURL: %s, buf: %s, resp: %#v, err: %v",
			r.QueryURL, buf, resp, err)
	}
	return respBuf, err
}

func (r *IndexClient) Advanced() (index.Index, store.KVStore, error) {
	return nil, nil, indexClientUnimplementedErr
}

// -----------------------------------------------------

func (r *IndexClient) AuthType() string {
	if r.mgr != nil {
		return r.mgr.Options()["authType"]
	}
	return ""
}

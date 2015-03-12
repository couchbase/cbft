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

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

var httpPost = http.Post
var httpGet = http.Get

var indexClientUnimplementedErr = errors.New("unimplemented")

// IndexClient implements the Search() and DocCount() subset of the
// bleve.Index interface by accessing a remote cbft server via REST
// protocol.  This allows callers to add a IndexClient as a target of
// a bleve.IndexAlias, and implements cbft protocol features like
// query consistency and auth.
//
// TODO: Implement propagating auth info in IndexClient.
type IndexClient struct {
	QueryURL    string
	CountURL    string
	Consistency *ConsistencyParams
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
	resp, err := httpGet(r.CountURL)
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
			" docCountURL: %s, resp: %#v", r.CountURL, resp)
	}
	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{}
	err = json.Unmarshal(respBuf, &rv)
	if err != nil {
		return 0, fmt.Errorf("remote: count error parsing respBuf: %s,"+
			" docCountURL: %s, resp: %#v", respBuf, r.CountURL, resp)
	}
	return rv.Count, nil
}

func (r *IndexClient) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if r.QueryURL == "" {
		return nil, fmt.Errorf("remote: no QueryURL provided")
	}

	bleveQueryParams := &BleveQueryParams{
		Query:       req,
		Consistency: r.Consistency,
	}

	buf, err := json.Marshal(bleveQueryParams)
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
			" queryURL: %s", respBuf, r.QueryURL)
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
	resp, err := httpPost(r.QueryURL, "application/json", bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("remote: query got status code: %d,"+
			" queryURL: %s, buf: %s, resp: %#v",
			resp.StatusCode, r.QueryURL, buf, resp)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("remote: query error reading resp.Body,"+
			" queryURL: %s, buf: %s, resp: %#v",
			r.QueryURL, buf, resp)
	}
	return respBuf, err
}

func (r *IndexClient) Advanced() (index.Index, store.KVStore, error) {
	return nil, nil, indexClientUnimplementedErr
}

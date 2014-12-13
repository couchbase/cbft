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
)

var httpPost = http.Post
var httpGet = http.Get

var pindexClientUnimplementedErr = errors.New("unimplemented")

// PIndexClient implements the Search() and DocCount() subset of the
// bleve.Index interface by accessing a remote cbft server via REST
// protocol.  This allows callers to add a PIndexClient as a target of
// a bleve.IndexAlias, and implements cbft protocol features like
// query consistency and auth.
//
// TODO: Implement propagating auth info in PIndexClient.
type PIndexClient struct {
	QueryURL    string
	CountURL    string
	Consistency *ConsistencyParams
}

func (r *PIndexClient) Index(id string, data interface{}) error {
	return pindexClientUnimplementedErr
}

func (r *PIndexClient) Delete(id string) error {
	return pindexClientUnimplementedErr
}

func (r *PIndexClient) Batch(b *bleve.Batch) error {
	return pindexClientUnimplementedErr
}

func (r *PIndexClient) Document(id string) (*document.Document, error) {
	return nil, pindexClientUnimplementedErr
}

func (r *PIndexClient) DocCount() (uint64, error) {
	if r.CountURL == "" {
		return 0, fmt.Errorf("no CountURL provided")
	}
	resp, err := httpGet(r.CountURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("pindexClient.DocCount got status code: %d,"+
			" docCountURL: %s, resp: %#v", resp.StatusCode, r.CountURL, resp)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("pindexClient.DocCount error reading resp.Body,"+
			" docCountURL: %s, resp: %#v", r.CountURL, resp)
	}
	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{}
	err = json.Unmarshal(respBuf, &rv)
	if err != nil {
		return 0, fmt.Errorf("pindexClient.DocCount error parsing respBuf: %s,"+
			" docCountURL: %s, resp: %#v", respBuf, r.CountURL, resp)
	}
	return rv.Count, nil
}

func (r *PIndexClient) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if r.QueryURL == "" {
		return nil, fmt.Errorf("no QueryURL provided")
	}

	bleveQueryParams := &BleveQueryParams{
		Query:       req,
		Consistency: r.Consistency,
	}

	buf, err := json.Marshal(bleveQueryParams)
	if err != nil {
		return nil, err
	}
	resp, err := httpPost(r.QueryURL, "application/json", bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("pindexClient.Search got status code: %d,"+
			" searchURL: %s, req: %#v, resp: %#v",
			resp.StatusCode, r.QueryURL, req, resp)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("pindexClient.Search error reading resp.Body,"+
			" searchURL: %s, req: %#v, resp: %#v",
			r.QueryURL, req, resp)
	}
	rv := &bleve.SearchResult{}
	err = json.Unmarshal(respBuf, rv)
	if err != nil {
		return nil, fmt.Errorf("pindexClient.Search error parsing respBuf: %s,"+
			" searchURL: %s, req: %#v, resp: %#v",
			respBuf, r.QueryURL, req, resp)
	}
	return rv, nil
}

func (r *PIndexClient) Fields() ([]string, error) {
	return nil, pindexClientUnimplementedErr
}

func (r *PIndexClient) DumpAll() chan interface{} {
	return nil
}

func (r *PIndexClient) DumpDoc(id string) chan interface{} {
	return nil
}

func (r *PIndexClient) DumpFields() chan interface{} {
	return nil
}

func (r *PIndexClient) Close() error {
	return pindexClientUnimplementedErr
}

func (r *PIndexClient) Mapping() *bleve.IndexMapping {
	return nil
}

func (r *PIndexClient) Stats() *bleve.IndexStat {
	return nil
}

func (r *PIndexClient) GetInternal(key []byte) ([]byte, error) {
	return nil, pindexClientUnimplementedErr
}

func (r *PIndexClient) SetInternal(key, val []byte) error {
	return pindexClientUnimplementedErr
}

func (r *PIndexClient) DeleteInternal(key []byte) error {
	return pindexClientUnimplementedErr
}

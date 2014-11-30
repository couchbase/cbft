//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

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

var bleveClientUnimplementedErr = errors.New("unimplemented")

// BleveClient implements the Search() and DocCount() subset of the
// bleve.Index interface by accessing a remote cbft server via REST
// protocol.  This allows callers to add a BleveClient as a target of
// a bleve.IndexAlias, and implements cbft protocol features like
// query consistency and auth.
//
// TODO: Implement consistency and auth in BleveClient.
type BleveClient struct {
	SearchURL         string
	DocCountURL       string
	ConsistencyParams *ConsistencyParams
}

func (r *BleveClient) Index(id string, data interface{}) error {
	return bleveClientUnimplementedErr
}

func (r *BleveClient) Delete(id string) error {
	return bleveClientUnimplementedErr
}

func (r *BleveClient) Batch(b *bleve.Batch) error {
	return bleveClientUnimplementedErr
}

func (r *BleveClient) Document(id string) (*document.Document, error) {
	return nil, bleveClientUnimplementedErr
}

func (r *BleveClient) DocCount() (uint64, error) {
	if r.DocCountURL == "" {
		return 0, fmt.Errorf("no DocCountURL provided")
	}
	resp, err := http.Get(r.DocCountURL)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("bleveClient.DocCount got status code: %d,"+
			" docCountURL: %s, resp: %#v", resp.StatusCode, r.DocCountURL, resp)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("bleveClient.DocCount error reading resp.Body,"+
			" docCountURL: %s, resp: %#v", r.DocCountURL, resp)
	}
	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{}
	err = json.Unmarshal(respBuf, &rv)
	if err != nil {
		return 0, fmt.Errorf("bleveClient.DocCount error parsing respBuf: %s,"+
			" docCountURL: %s, resp: %#v", respBuf, r.DocCountURL, resp)
	}
	return rv.Count, nil
}

func (r *BleveClient) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if r.SearchURL == "" {
		return nil, fmt.Errorf("no SearchURL provided")
	}
	// TODO: need to also add r.ConsistencyParams to buf.
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(r.SearchURL, "application/json", bytes.NewBuffer(buf))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("bleveClient.Search got status code: %d,"+
			" searchURL: %s, req: %#v, resp: %#v",
			resp.StatusCode, r.SearchURL, req, resp)
	}
	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("bleveClient.Search error reading resp.Body,"+
			" searchURL: %s, req: %#v, resp: %#v",
			r.SearchURL, req, resp)
	}
	rv := &bleve.SearchResult{}
	err = json.Unmarshal(respBuf, rv)
	if err != nil {
		return nil, fmt.Errorf("bleveClient.Search error parsing respBuf: %s,"+
			" searchURL: %s, req: %#v, resp: %#v",
			respBuf, r.SearchURL, req, resp)
	}
	return rv, nil
}

func (r *BleveClient) Fields() ([]string, error) {
	return nil, bleveClientUnimplementedErr
}

func (r *BleveClient) DumpAll() chan interface{} {
	return nil
}

func (r *BleveClient) DumpDoc(id string) chan interface{} {
	return nil
}

func (r *BleveClient) DumpFields() chan interface{} {
	return nil
}

func (r *BleveClient) Close() error {
	return bleveClientUnimplementedErr
}

func (r *BleveClient) Mapping() *bleve.IndexMapping {
	return nil
}

func (r *BleveClient) Stats() *bleve.IndexStat {
	return nil
}

func (r *BleveClient) GetInternal(key []byte) ([]byte, error) {
	return nil, bleveClientUnimplementedErr
}

func (r *BleveClient) SetInternal(key, val []byte) error {
	return bleveClientUnimplementedErr
}

func (r *BleveClient) DeleteInternal(key []byte) error {
	return bleveClientUnimplementedErr
}

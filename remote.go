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
// bleve.Index interface.  It's a HTTP/REST client that retrieves
// results from a HTTP server that's providing bleveHttp endpoints.
type BleveClient struct {
	SearchURL   string
	DocCountURL string

	// TODO: What about auth?

	// TODO: There are probably pindex consistency params needed here,
	// too, so figure how to unmix those from bleve http concerns.
	// Perhaps this should be RemotePIndexClient or just PIndexClient.
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
	return 0, bleveClientUnimplementedErr
}

func (r *BleveClient) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if r.SearchURL == "" {
		return nil, fmt.Errorf("no SearchURL provided")
	}
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

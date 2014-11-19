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
	"errors"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
)

var remoteBleveUnimplementedErr = errors.New("unimplemented")

// RemoteBleveIndex implements the Search() and DocCount() subset of
// the bleve.Index interface.  It's a HTTP/REST client that retrieves
// results from a HTTP server that provides bleveHttp endpoints.
type RemoteBleveIndex struct {
}

func (r *RemoteBleveIndex) Index(id string, data interface{}) error {
	return remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) Delete(id string) error {
	return remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) Batch(b *bleve.Batch) error {
	return remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) Document(id string) (*document.Document, error) {
	return nil, remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) DocCount() (uint64, error) {
	return 0, remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) Search(req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return nil, remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) Fields() ([]string, error) {
	return nil, remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) DumpAll() chan interface{} {
	return nil
}

func (r *RemoteBleveIndex) DumpDoc(id string) chan interface{} {
	return nil
}

func (r *RemoteBleveIndex) DumpFields() chan interface{} {
	return nil
}

func (r *RemoteBleveIndex) Close() error {
	return remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) Mapping() *bleve.IndexMapping {
	return nil
}

func (r *RemoteBleveIndex) Stats() *bleve.IndexStat {
	return nil
}

func (r *RemoteBleveIndex) GetInternal(key []byte) ([]byte, error) {
	return nil, remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) SetInternal(key, val []byte) error {
	return remoteBleveUnimplementedErr
}

func (r *RemoteBleveIndex) DeleteInternal(key []byte) error {
	return remoteBleveUnimplementedErr
}

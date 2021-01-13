//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"context"
	"errors"
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

var missingPIndexUnimplementedErr = errors.New("unimplemented")

type MissingPIndex struct {
	name string
}

func (m *MissingPIndex) Name() string {
	return m.name
}

func (m *MissingPIndex) SetName(name string) {
	m.name = name
}

func (m *MissingPIndex) Index(id string, data interface{}) error {
	return missingPIndexUnimplementedErr
}

func (m *MissingPIndex) Delete(id string) error {
	return missingPIndexUnimplementedErr
}

func (m *MissingPIndex) Batch(b *bleve.Batch) error {
	return missingPIndexUnimplementedErr
}

func (m *MissingPIndex) Document(id string) (index.Document, error) {
	return nil, missingPIndexUnimplementedErr
}

func (m *MissingPIndex) DocCount() (uint64, error) {
	return 0, nil
}

func (m *MissingPIndex) Search(req *bleve.SearchRequest) (
	*bleve.SearchResult, error) {
	return m.SearchInContext(context.Background(), req)
}

func (m *MissingPIndex) SearchInContext(ctx context.Context,
	req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return nil, fmt.Errorf("pindex not available")
}

func (m *MissingPIndex) Fields() ([]string, error) {
	return nil, missingPIndexUnimplementedErr
}

func (m *MissingPIndex) FieldDict(field string) (index.FieldDict, error) {
	return nil, missingPIndexUnimplementedErr
}

func (m *MissingPIndex) FieldDictRange(field string,
	startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	return nil, missingPIndexUnimplementedErr
}

func (m *MissingPIndex) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return nil, missingPIndexUnimplementedErr
}

func (m *MissingPIndex) DumpAll() chan interface{} {
	return nil
}

func (m *MissingPIndex) DumpDoc(id string) chan interface{} {
	return nil
}

func (m *MissingPIndex) DumpFields() chan interface{} {
	return nil
}

func (m *MissingPIndex) Close() error {
	return missingPIndexUnimplementedErr
}

func (m *MissingPIndex) Mapping() mapping.IndexMapping {
	return nil
}

func (m *MissingPIndex) NewBatch() *bleve.Batch {
	return nil
}

func (m *MissingPIndex) Stats() *bleve.IndexStat {
	return nil
}

func (m *MissingPIndex) StatsMap() map[string]interface{} {
	return nil
}

func (m *MissingPIndex) GetInternal(key []byte) ([]byte, error) {
	return nil, missingPIndexUnimplementedErr
}

func (m *MissingPIndex) SetInternal(key, val []byte) error {
	return missingPIndexUnimplementedErr
}

func (m *MissingPIndex) DeleteInternal(key []byte) error {
	return missingPIndexUnimplementedErr
}

func (m *MissingPIndex) Advanced() (index.Index, error) {
	return nil, missingPIndexUnimplementedErr
}

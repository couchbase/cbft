//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

func (m *MissingPIndex) TermFrequencies(field string, limit int, descending bool) (
	[]index.TermFreq, error) {
	return nil, nil
}

func (m *MissingPIndex) CentroidCardinalities(field string, limit int, descending bool) (
	[]index.CentroidCardinality, error) {
	return nil, nil
}

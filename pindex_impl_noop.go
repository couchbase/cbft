//  Copyright 2021-Present Couchbase, Inc.
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

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

var noopBleveIndexUnimplementedErr = errors.New("unimplemented")

// noopBleveIndex is a noop implementation for bleve.Index
type noopBleveIndex struct {
	name string
}

func (p *noopBleveIndex) Name() string {
	return p.name
}

func (p *noopBleveIndex) SetName(name string) {
	p.name = name
}

func (p *noopBleveIndex) Index(id string, data interface{}) error {
	return noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) Delete(id string) error {
	return noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) Batch(b *bleve.Batch) error {
	return noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) Document(id string) (index.Document, error) {
	return nil, nil
}

func (p *noopBleveIndex) DocCount() (uint64, error) {
	return 0, nil
}

func (p *noopBleveIndex) Search(req *bleve.SearchRequest) (
	*bleve.SearchResult, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) SearchInContext(ctx context.Context,
	req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) Fields() ([]string, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) FieldDict(field string) (index.FieldDict, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) FieldDictRange(field string,
	startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) Close() error {
	return nil
}

func (p *noopBleveIndex) Mapping() mapping.IndexMapping {
	return nil
}

func (p *noopBleveIndex) NewBatch() *bleve.Batch {
	return nil
}

func (p *noopBleveIndex) Stats() *bleve.IndexStat {
	return nil
}

func (p *noopBleveIndex) StatsMap() map[string]interface{} {
	return nil
}

func (p *noopBleveIndex) GetInternal(key []byte) ([]byte, error) {
	return nil, noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) SetInternal(key, val []byte) error {
	return noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) DeleteInternal(key []byte) error {
	return noopBleveIndexUnimplementedErr
}

func (p *noopBleveIndex) Advanced() (index.Index, error) {
	return nil, noopBleveIndexUnimplementedErr
}

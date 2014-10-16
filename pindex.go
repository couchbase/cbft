//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve"
)

// A PIndex represents a "physical" index or a index "partition".

type PIndex struct {
	indexName string
	indexPath string
	index     bleve.Index
	stream    Stream
}

func NewPIndex(indexName, indexPath string, indexMappingBytes []byte) (*PIndex, error) {
	indexMapping := bleve.NewIndexMapping()

	if len(indexMappingBytes) > 0 {
		if err := json.Unmarshal(indexMappingBytes, &indexMapping); err != nil {
			return nil, fmt.Errorf("error: could not parse index mapping: %v", err)
		}
	}

	index, err := bleve.New(indexPath, indexMapping)
	if err != nil {
		return nil, fmt.Errorf("error: new bleve index, indexPath: %s, err: %s",
			indexPath, err)
	}

	return RunPIndex(indexName, indexPath, index)
}

func OpenPIndex(indexName, indexPath string) (*PIndex, error) {
	index, err := bleve.Open(indexPath)
	if err != nil {
		return nil, fmt.Errorf("error: could not open bleve index, indexPath: %v, err: %v",
			indexPath, err)
	}
	return RunPIndex(indexName, indexPath, index)
}

func RunPIndex(indexName, indexPath string, index bleve.Index) (*PIndex, error) {
	pindex := &PIndex{
		indexName: indexName,
		indexPath: indexPath,
		index:     index,
		stream:    make(Stream),
	}
	go pindex.Run()
	return pindex, nil
}

func (pindex *PIndex) IndexName() string {
	return pindex.indexName
}

func (pindex *PIndex) IndexPath() string {
	return pindex.indexPath
}

func (pindex *PIndex) Index() bleve.Index {
	return pindex.index
}

func (pindex *PIndex) Stream() Stream {
	return pindex.stream
}

func (pindex *PIndex) Run() {
	for m := range pindex.stream {
		// TODO: probably need things like stream reset/rollback
		// and snapshot kinds of ops here, too.

		// TODO: maybe need a more batchy API?  Perhaps, yet another
		// goroutine that clumps up up updates into bigger batches?

		switch m := m.(type) {
		case *StreamUpdate:
			pindex.index.Index(string(m.Id()), m.Body())
		case *StreamDelete:
			pindex.index.Delete(string(m.Id()))
		}
	}
}

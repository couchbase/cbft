//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch"
	"github.com/blevesearch/bleve/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

// Only cache a bleve result whose size in number of hits isn't too
// large in order to avoid consuming too much memory for the cache.
var BleveResultCacheMaxHits = 100

// InitBleveResultCacheOptions initializes the bleve related result
// cache options.
func InitBleveResultCacheOptions(options map[string]string) error {
	if options["bleveResultCacheMaxHits"] != "" {
		x, err := strconv.Atoi(options["bleveResultCacheMaxHits"])
		if err != nil {
			return err
		}
		BleveResultCacheMaxHits = x
	}
	return nil
}

// bleveSearchRequestToCacheKey generates a result cache key for a
// bleve search request.
func (m *cacheBleveIndex) bleveSearchRequestToCacheKey(
	req *bleve.SearchRequest) (string, error) {
	// TODO: Might be a faster way to stringify a request than JSON.
	j, err := json.Marshal(req)
	if err != nil {
		return "", err
	}

	return m.pindex.Name + "/" + m.pindex.UUID + "/" + string(j), nil
}

// --------------------------------------------------------

var cacheBleveIndexUnimplementedErr = errors.New("unimplemented")

// A cacheBleveIndex implements the bleve.Index interface so it can be
// used as an bleve index alias target.  It's mostly just a pass-thru
// wrapper around a bleve.Index but that also provides caching of
// search results during SearchInContext().
type cacheBleveIndex struct {
	pindex *cbgt.PIndex
	bindex bleve.Index
	rev    uint64
	name   string
}

func (m *cacheBleveIndex) Name() string {
	return m.name
}

func (m *cacheBleveIndex) SetName(name string) {
	m.name = name
}

func (m *cacheBleveIndex) Index(id string, data interface{}) error {
	return cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) Delete(id string) error {
	return cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) Batch(b *bleve.Batch) error {
	return cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) Document(id string) (*document.Document, error) {
	return m.bindex.Document(id)
}

func (m *cacheBleveIndex) DocCount() (uint64, error) {
	return m.bindex.DocCount()
}

func (m *cacheBleveIndex) Search(req *bleve.SearchRequest) (
	*bleve.SearchResult, error) {
	return m.SearchInContext(context.Background(), req)
}

func (m *cacheBleveIndex) SearchInContext(ctx context.Context,
	req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if !ResultCache.enabled() {
		return m.bindex.SearchInContext(ctx, req)
	}

	key, err := m.bleveSearchRequestToCacheKey(req)
	if err != nil {
		return nil, err
	}

	resBytes, err := ResultCache.lookup(key, m.rev)
	if err == nil && len(resBytes) > 0 {
		// TODO: Use something better than JSON to copy a search result.
		var res bleve.SearchResult
		err = json.Unmarshal(resBytes, &res)
		if err == nil {
			return &res, nil
		}
	}

	res, err := m.bindex.SearchInContext(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(res.Hits) < BleveResultCacheMaxHits { // Don't cache overly large results.
		ResultCache.encache(key, func() []byte {
			// TODO: Use something better than JSON to copy a search result.
			resBytes, err = json.Marshal(res)
			if err != nil {
				return nil
			}
			return resBytes
		}, m.rev, uint64(res.Took))
	}

	return res, nil
}

func (m *cacheBleveIndex) Fields() ([]string, error) {
	return m.bindex.Fields()
}

func (m *cacheBleveIndex) FieldDict(field string) (index.FieldDict, error) {
	return m.bindex.FieldDict(field)
}

func (m *cacheBleveIndex) FieldDictRange(field string,
	startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	return m.bindex.FieldDictRange(field, startTerm, endTerm)
}

func (m *cacheBleveIndex) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return m.bindex.FieldDictPrefix(field, termPrefix)
}

func (m *cacheBleveIndex) Close() error {
	return cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) Mapping() mapping.IndexMapping {
	return m.bindex.Mapping()
}

func (m *cacheBleveIndex) NewBatch() *bleve.Batch {
	return nil
}

func (m *cacheBleveIndex) Stats() *bleve.IndexStat {
	return m.bindex.Stats()
}

func (m *cacheBleveIndex) StatsMap() map[string]interface{} {
	return m.bindex.StatsMap()
}

func (m *cacheBleveIndex) GetInternal(key []byte) ([]byte, error) {
	return m.bindex.GetInternal(key)
}

func (m *cacheBleveIndex) SetInternal(key, val []byte) error {
	return cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) DeleteInternal(key []byte) error {
	return cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) Advanced() (index.Index, store.KVStore, error) {
	return nil, nil, cacheBleveIndexUnimplementedErr
}

func (m *cacheBleveIndex) getLiveTasks(req *cbgt.TaskRequest) (
	*cbgt.TaskRequestStatus, error) {
	if req == nil {
		return nil, nil
	}
	rv := &cbgt.TaskRequestStatus{Request: req, Total: 1,
		Successful: 1}

	for _, tname := range backgroundTasks() {
		if strings.HasPrefix(tname, m.bindex.Name()) {
			if rv.Status == nil {
				rv.Status = make(map[string]string)
			}
			rv.Status[tname] = "In progress"
		}
	}

	return rv, nil
}

func (m *cacheBleveIndex) cancelTask(req *cbgt.TaskRequest) (
	*cbgt.TaskRequestStatus, error) {
	if req == nil {
		return nil, nil
	}
	if req.UUID == "" {
		return nil, fmt.Errorf("bleve: missing uuid for cancel task")
	}

	rv := &cbgt.TaskRequestStatus{Request: req, Total: 1, Successful: 1}
	var tkey string
	for _, tname := range backgroundTasks() {
		if strings.HasSuffix(tname, req.UUID) &&
			strings.HasPrefix(tname, m.bindex.Name()) {
			tkey = tname
			break
		}
	}

	err := cancelTask(tkey)
	if err != nil {
		rv.Successful = 0
		rv.Failed = 1
		rv.Errors = make(map[string]error, 1)
		rv.Errors[m.bindex.Name()] = err
	} else {
		log.Printf("bleve: cancelled the task: %s on partition: %s",
			req.UUID, m.bindex.Name())
	}

	return rv, nil
}

func (m *cacheBleveIndex) forceMerge(req *cbgt.TaskRequest) (
	*cbgt.TaskRequestStatus, error) {
	if req == nil {
		return nil, nil
	}

	mpo := mergeplan.SingleSegmentMergePlanOptions
	if v, ok := req.Contents["scorchMergePlanOptions"]; ok {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(b, &mpo)
		if err != nil {
			return nil, err
		}
	}

	index, _, err := m.bindex.Advanced()
	if err != nil {
		return nil, err
	}

	rv := &cbgt.TaskRequestStatus{Request: req, Total: 1, Successful: 1}
	if si, ok := index.(*scorch.Scorch); ok {
		go func() {
			// track the running tasks with a cancel knob
			ctx, cancel := context.WithCancel(context.Background())
			tkey := m.bindex.Name() + ":merge:" + req.UUID
			updateTaskStart(tkey, cancel)

			log.Printf("bleve: triggering merge operations on index: %s", m.bindex.Name())
			st := time.Now()
			err = si.ForceMerge(ctx, &mpo)
			et := time.Since(st).Seconds()

			// mark the completion
			updateTaskFinish(tkey)
			log.Printf("bleve: force merge finished on index: %s, "+
				" time took: %f secs", m.bindex.Name(), et)
		}()
	} else {
		err = fmt.Errorf("bleve: compaction not supported in non scorch index %s",
			m.bindex.Name())
	}

	if err != nil {
		rv.Successful = 0
		rv.Failed = 1
		rv.Errors = make(map[string]error, 1)
		rv.Errors[m.bindex.Name()] = err
	}
	return rv, err
}

// HandleTask is an implementation of the cbgt.TaskRequestHandler interface
func (m *cacheBleveIndex) HandleTask(req []byte) (
	*cbgt.TaskRequestStatus, error) {
	var task cbgt.TaskRequest
	err := json.Unmarshal(req, &task)
	if err != nil {
		return nil, err
	}

	if task.Op == "merge" {
		return m.forceMerge(&task)
	}

	if task.Op == "cancel" {
		return m.cancelTask(&task)
	}

	if task.Op == "get" {
		return m.getLiveTasks(&task)
	}

	return nil, fmt.Errorf("bleve: unknown task op: %s", task.Op)
}

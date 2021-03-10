//  Copyright (c) 2018 Couchbase, Inc.
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
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbgt/rest"
)

type QuerySupervisorContext struct {
	Query     query.Query        `json:"query"`
	Cancel    context.CancelFunc `json:"-"`
	Size      int                `json:"size"`
	From      int                `json:"from"`
	Timeout   int64              `json:"timeout"`
	IndexName string             `json:"index"`

	addedAt time.Time
}

type QuerySupervisor struct {
	m                sync.RWMutex
	queryMap         map[uint64]*QuerySupervisorContext
	indexAccessTimes map[string]time.Time
	id               uint64
}

var querySupervisor *QuerySupervisor

func init() {
	querySupervisor = &QuerySupervisor{
		queryMap:         make(map[uint64]*QuerySupervisorContext),
		indexAccessTimes: make(map[string]time.Time),
	}
}

func (qs *QuerySupervisor) AddEntry(qsc *QuerySupervisorContext) uint64 {
	if qsc == nil {
		qsc = &QuerySupervisorContext{}
	}
	qsc.addedAt = time.Now()
	qs.m.Lock()
	qs.id++
	id := qs.id
	qs.queryMap[id] = qsc
	if qsc.IndexName != "" {
		qs.indexAccessTimes[qsc.IndexName] = qsc.addedAt
	}
	qs.m.Unlock()
	return id
}

func (qs *QuerySupervisor) DeleteEntry(id uint64) {
	qs.m.Lock()
	if _, exists := qs.queryMap[id]; exists {
		delete(qs.queryMap, id)
	}
	qs.m.Unlock()
}

func (qs *QuerySupervisor) Count() uint64 {
	qs.m.RLock()
	count := uint64(len(qs.queryMap))
	qs.m.RUnlock()
	return count
}

func (qs *QuerySupervisor) GetLastAccessTimeForIndex(name string) string {
	qs.m.RLock()
	defer qs.m.RUnlock()

	if t, exists := qs.indexAccessTimes[name]; exists {
		return t.Format("2006-01-02T15:04:05.000-07:00")
	}

	return ""
}

type RunningQueryDetails struct {
	QueryContext  *QuerySupervisorContext
	ExecutionTime string `json:"executionTime"`
}

// ListLongerThanWithQueryCount filters the active running queries against the
// given duration and the index name along with the total active query count.
// TODO - Incoming queries shouldn't get blocked due to lock deprivations
// from the frequent read operations.
func (qs *QuerySupervisor) ListLongerThanWithQueryCount(longerThan time.Duration,
	indexName string) (queryMap map[uint64]*RunningQueryDetails, activeQueryCount int) {
	var i int
	qs.m.RLock()
	// upfront initialisations to save frequent allocator trips.
	queryMap = make(map[uint64]*RunningQueryDetails, len(qs.queryMap))
	pool := make([]RunningQueryDetails, len(qs.queryMap))

	for key, val := range qs.queryMap {
		timeSince := time.Since(val.addedAt)
		if timeSince > longerThan &&
			(indexName == "" || indexName == val.IndexName) {
			pool[i].QueryContext = val
			pool[i].ExecutionTime = fmt.Sprintf("%s", timeSince)
			queryMap[key] = &pool[i]
			i++
		}
	}
	activeQueryCount = len(qs.queryMap)
	qs.m.RUnlock()

	return queryMap, activeQueryCount
}

func (qs *QuerySupervisor) ExecutionTime(id uint64) (time.Duration, bool) {
	qs.m.RLock()
	defer qs.m.RUnlock()
	if val, exists := qs.queryMap[id]; exists {
		return time.Since(val.addedAt), true
	}

	return 0, false
}

func (qs *QuerySupervisor) KillQuery(id uint64) bool {
	qs.m.Lock()
	defer qs.m.Unlock()
	if val, exists := qs.queryMap[id]; exists {
		val.Cancel()
		return true
	}

	return false
}

type QuerySupervisorDetails struct{}

func NewQuerySupervisorDetails() *QuerySupervisorDetails {
	return &QuerySupervisorDetails{}
}

func (qss *QuerySupervisorDetails) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := rest.RequestVariableLookup(req, "indexName")
	queryParams := req.URL.Query()
	params := queryParams.Get("longerThan")
	var longerThan time.Duration
	if len(params) > 1 {
		duration, err := time.ParseDuration(params)
		if err != nil {
			rest.PropagateError(w, nil,
				fmt.Sprintf("query details: duration parse err: %v", err),
				http.StatusBadRequest)
			return
		}
		longerThan = duration
	}

	queryMap, queryCount := querySupervisor.ListLongerThanWithQueryCount(
		longerThan, indexName)

	type filteredQueryStats struct {
		IndexName  string                          `json:"indexName,omitempty"`
		LongerThan string                          `json:"longerThan,omitempty"`
		QueryCount uint64                          `json:"queryCount"`
		QueryMap   map[uint64]*RunningQueryDetails `json:"queryMap"`
	}

	rv := struct {
		Status                string             `json:"status"`
		TotalActiveQueryCount uint64             `json:"totalActiveQueryCount"`
		FilteredActiveQueries filteredQueryStats `json:"filteredActiveQueries"`
	}{
		Status:                "ok",
		TotalActiveQueryCount: uint64(queryCount),
		FilteredActiveQueries: filteredQueryStats{
			IndexName:  indexName,
			LongerThan: params,
			QueryCount: uint64(len(queryMap)),
			QueryMap:   queryMap,
		},
	}

	rest.MustEncode(w, rv)
}

type QueryKiller struct{}

func NewQueryKiller() *QueryKiller {
	return &QueryKiller{}
}

func (qk *QueryKiller) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	queryID := rest.RequestVariableLookup(req, "queryID")
	if queryID == "" {
		rest.PropagateError(w, nil, "query killer: query ID not provided",
			http.StatusBadRequest)
		return
	}

	qid, err := strconv.ParseUint(queryID, 10, 64)
	if err != nil {
		rest.PropagateError(w, nil,
			fmt.Sprintf("query killer: query ID '%v' not a uint64", queryID),
			http.StatusBadRequest)
		return
	}

	if !querySupervisor.KillQuery(qid) {
		rest.PropagateError(w, nil,
			fmt.Sprintf("query killer: query ID '%v' not found", qid),
			http.StatusBadRequest)
		return
	}

	rv := struct {
		Status string `json:"status"`
		Msg    string `json:"msg"`
	}{
		Status: "ok",
		Msg:    fmt.Sprintf("query with ID '%v' was canceled!", queryID),
	}
	rest.MustEncode(w, rv)
}

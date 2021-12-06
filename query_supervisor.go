//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbgt"
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

func (r *QuerySupervisorContext) UnmarshalJSON(input []byte) error {
	var temp struct {
		Q         json.RawMessage `json:"query"`
		Size      int             `json:"size"`
		From      int             `json:"from"`
		Timeout   int64           `json:"timeout"`
		IndexName string          `json:"index"`
	}
	err := UnmarshalJSON(input, &temp)
	if err != nil {
		return err
	}
	r.Size = temp.Size
	r.From = temp.From
	r.Timeout = temp.Timeout
	r.IndexName = temp.IndexName
	r.Query, err = query.ParseQuery(temp.Q)
	if err != nil {
		return err
	}
	return nil
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

func (qs *QuerySupervisor) deleteEntryForIndex(name string) {
	qs.m.Lock()
	delete(qs.indexAccessTimes, name)
	qs.m.Unlock()
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

type QuerySupervisorDetails struct {
	mgr *cbgt.Manager
}

func NewQuerySupervisorDetails(mgr *cbgt.Manager) *QuerySupervisorDetails {
	return &QuerySupervisorDetails{
		mgr: mgr,
	}
}

type filterQueryStats struct {
	IndexName  string                          `json:"indexName,omitempty"`
	LongerThan string                          `json:"longerThan,omitempty"`
	QueryCount uint64                          `json:"queryCount"`
	QueryMap   map[string]*RunningQueryDetails `json:"queryMap"`
}

type responseStats struct {
	Total      int               `json:"total,omitempty"`
	Successful int               `json:"successful,omitempty"`
	Failed     int               `json:"failed,omitempty"`
	Error      map[string]string `json:"error,omitempty"`
}

type responseData struct {
	Status                string           `json:"status"`
	Stats                 *responseStats   `json:"stats,omitempty"`
	TotalActiveQueryCount uint64           `json:"totalActiveQueryCount"`
	FilteredActiveQueries filterQueryStats `json:"filteredActiveQueries"`
}

type response struct {
	uuid string
	data responseData
}

func (qss *QuerySupervisorDetails) getURLs(nodeDefs *cbgt.NodeDefs,
	indexName string, longerThan string) (map[string]string, error) {
	if nodeDefs == nil {
		return nil, fmt.Errorf("empty node definitions")
	}
	nodeURLs := make(map[string]string)
	ss := cbgt.GetSecuritySetting()
	for uuid, node := range nodeDefs.NodeDefs {
		hostPortUrl := "http://" + node.HostPort
		if ss.EncryptionEnabled {
			if u, err := node.HttpsURL(); err == nil {
				hostPortUrl = u
			}
		}
		urlStr := hostPortUrl + "/api/query"
		if indexName != "" {
			urlStr += "/index/" + indexName
		}
		if longerThan != "" {
			urlStr += "?longerThan=" + longerThan
		}
		urlStr, err := cbgt.CBAuthURL(urlStr)
		if err != nil {
			return nil, fmt.Errorf("failed to authenticate"+
				" the URL, err :%v", err)
		}
		nodeURLs[uuid] = urlStr
	}
	return nodeURLs, nil
}

func (qss *QuerySupervisorDetails) getSupervisorMaps(nodeDefs *cbgt.NodeDefs,
	indexName string, longerThan string) ([]response, map[string]string) {
	type supervisorReq struct {
		uuid string
		url  string
	}
	type supervisorResp struct {
		uuid string
		data responseData
		err  error
	}

	supervisorDetailsURLs, err := qss.getURLs(nodeDefs, indexName,
		longerThan)
	if err != nil {
		return nil, map[string]string{qss.mgr.UUID(): fmt.Sprintf(
			"error while forming the URLs err: %v", err)}
	}
	var wg sync.WaitGroup
	groupSize := getWorkerCount(len(supervisorDetailsURLs))
	requestCh := make(chan *supervisorReq, len(supervisorDetailsURLs))
	responseCh := make(chan *supervisorResp, len(supervisorDetailsURLs))

	for i := 0; i < groupSize; i++ {
		wg.Add(1)
		go func() {
			for reqs := range requestCh {
				ctx, ctxCancel := context.WithTimeout(context.Background(),
					60*time.Second)
				req, err := http.NewRequestWithContext(ctx, "GET", reqs.url, nil)
				req.Header.Add(rest.CLUSTER_ACTION, clusterActionScatterGather)
				if err != nil {
					ctxCancel()
					responseCh <- &supervisorResp{
						uuid: reqs.uuid,
						err: fmt.Errorf("failed to form a request"+
							" to node: %s, err: %v", reqs.uuid, err),
					}
					continue
				}

				httpClient := cbgt.HttpClient()
				res, err := httpClient.Do(req)
				ctxCancel()
				if err != nil {
					responseCh <- &supervisorResp{
						uuid: reqs.uuid,
						err: fmt.Errorf("failed to send a request to node:"+
							" %s, err: %v", reqs.uuid, err),
					}
					continue
				}
				data, derr := ioutil.ReadAll(res.Body)
				res.Body.Close()
				if derr != nil {
					responseCh <- &supervisorResp{
						uuid: reqs.uuid,
						err: fmt.Errorf("error in decoding response from node:"+
							" %s, err: %v", reqs.uuid, derr),
					}
					continue
				}
				var respInst responseData
				err = UnmarshalJSON(data, &respInst)
				if err != nil {
					responseCh <- &supervisorResp{
						uuid: reqs.uuid,
						err: fmt.Errorf("json unmarshal error for response"+
							" from node: %s, err: %v", reqs.uuid, err),
					}
					continue
				}
				responseCh <- &supervisorResp{
					uuid: reqs.uuid,
					data: respInst,
					err:  nil,
				}
			}
			wg.Done()
		}()
	}

	for uuid, url := range supervisorDetailsURLs {
		requestCh <- &supervisorReq{uuid: uuid, url: url}
	}
	close(requestCh)
	wg.Wait()
	close(responseCh)

	var responses []response
	errs := make(map[string]string)
	for resp := range responseCh {
		if resp.err == nil {
			responses = append(responses, response{
				uuid: resp.uuid,
				data: resp.data,
			})
		} else {
			errs[resp.uuid] = fmt.Sprintf("recieved error err: %v", resp.err)
		}
	}
	return responses, errs
}

func (qss *QuerySupervisorDetails) mergeResponses(responses []response,
	errs map[string]string) *responseData {
	output := &responseData{
		Status: "ok",
		Stats: &responseStats{
			Total:      len(errs) + len(responses),
			Successful: len(responses),
			Failed:     len(errs),
			Error:      errs,
		},
		FilteredActiveQueries: filterQueryStats{
			IndexName:  responses[0].data.FilteredActiveQueries.IndexName,
			LongerThan: responses[0].data.FilteredActiveQueries.LongerThan,
			QueryMap:   make(map[string]*RunningQueryDetails),
		},
	}
	if len(errs) > 0 {
		output.Status = "partial"
	}
	for _, r := range responses {
		output.TotalActiveQueryCount += r.data.TotalActiveQueryCount
		output.FilteredActiveQueries.QueryCount += r.data.
			FilteredActiveQueries.QueryCount
		for k, v := range r.data.FilteredActiveQueries.QueryMap {
			key := r.uuid + "-" + k
			output.FilteredActiveQueries.QueryMap[key] = v
		}
	}
	return output
}

func (qss *QuerySupervisorDetails) changeQueryMapFormat(
	input map[uint64]*RunningQueryDetails) map[string]*RunningQueryDetails {
	output := make(map[string]*RunningQueryDetails)
	for k, v := range input {
		output[fmt.Sprintf("%v", k)] = v
	}
	return output
}

func (qss *QuerySupervisorDetails) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	indexName := rest.RequestVariableLookup(req, "indexName")
	queryParams := req.URL.Query()
	params := queryParams.Get("longerThan")
	var rv *responseData
	var longerThan time.Duration

	if len(params) > 1 {
		duration, err := time.ParseDuration(params)
		if err != nil {
			rest.ShowError(w, nil,
				fmt.Sprintf("query details: duration parse error, err: %v", err),
				http.StatusBadRequest)
			return
		}
		longerThan = duration
	}
	if req.Header.Get(rest.CLUSTER_ACTION) != "" {
		queryMap, queryCount := querySupervisor.ListLongerThanWithQueryCount(
			longerThan, indexName)
		rv = &responseData{
			Status:                "ok",
			TotalActiveQueryCount: uint64(queryCount),
			FilteredActiveQueries: filterQueryStats{
				IndexName:  indexName,
				LongerThan: params,
				QueryCount: uint64(len(queryMap)),
				QueryMap:   qss.changeQueryMapFormat(queryMap),
			},
		}
	} else {
		nodeDefs, err := qss.mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, false)
		if err != nil {
			rest.ShowError(w, nil, fmt.Sprintf("query details: could not get"+
				" the node defintions, err: %v", err),
				http.StatusInternalServerError)
			return
		}
		responses, errs := qss.getSupervisorMaps(nodeDefs, indexName, params)
		if len(responses) == 0 {
			errStats, err := MarshalJSON(responseStats{
				Total:  len(errs),
				Failed: len(errs),
				Error:  errs,
			})
			if err != nil {
				rest.ShowError(w, nil, fmt.Sprintf("query details: failed to"+
					" marshal response stats info, err: %s", err),
					http.StatusInternalServerError)
				return
			}
			rest.ShowError(w, nil,
				fmt.Sprintf("query details: failed to get query details,"+
					" err: %s", errStats), http.StatusInternalServerError)
			return
		} else {
			rv = qss.mergeResponses(responses, errs)
		}
	}
	rest.MustEncode(w, rv)
	return
}

type QueryKiller struct {
	mgr *cbgt.Manager
}

func NewQueryKiller(mgr *cbgt.Manager) *QueryKiller {
	return &QueryKiller{
		mgr: mgr,
	}
}

func (qk *QueryKiller) getURL(nodeDefs *cbgt.NodeDefs, uuid string,
	qID string) (string, error) {
	if node, ok := nodeDefs.NodeDefs[uuid]; ok {
		ss := cbgt.GetSecuritySetting()
		hostPortUrl := "http://" + node.HostPort
		if ss.EncryptionEnabled {
			if u, err := node.HttpsURL(); err == nil {
				hostPortUrl = u
			}
		}
		urlStr := hostPortUrl + "/api/query/" + qID + "/cancel"
		urlStr, err := cbgt.CBAuthURL(urlStr)
		if err != nil {
			return "", fmt.Errorf("failed to authenticate"+
				" URL, err: %v", err)
		}
		return urlStr, nil
	}
	return "", fmt.Errorf("unknown uuid: %s", uuid)
}

func (qk *QueryKiller) killQuery(queryID string, uuid string) (int, error) {
	if uuid == "" {
		qid, err := strconv.ParseUint(queryID, 10, 64)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("query cancel: query ID "+
				"'%v' not a uint64",
				queryID)
		}

		if !querySupervisor.KillQuery(qid) {
			return http.StatusBadRequest, fmt.Errorf("query cancel: query ID"+
				" '%v' not found", qid)

		}
		return http.StatusOK, nil
	}
	nodeDefs, err := qk.mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, false)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("query cancel: could"+
			"not get node definition, err: %v", err)
	}
	queryKillURL, err := qk.getURL(nodeDefs, uuid, queryID)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("query cancel: failed"+
			" to form the cancel url, err: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		60*time.Second)
	defer cancel()
	killReq, err := http.NewRequestWithContext(ctx, "POST", queryKillURL, nil)
	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("query cancel: failed"+
			" to create cancel request, err: %v", err)
	}
	httpClient := cbgt.HttpClient()
	resp, err := httpClient.Do(killReq)

	if err != nil {
		return http.StatusInternalServerError, fmt.Errorf("query cancel: cancel"+
			" request to node: %s failed, err: %v", uuid, err)
	}
	if resp.StatusCode != http.StatusOK {
		respBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return http.StatusInternalServerError, fmt.Errorf("query cancel: error"+
				" while reading the response from node: %s err: %v", uuid, err)
		}
		return http.StatusInternalServerError, fmt.Errorf("query cancel: cancel"+
			" request failed in node: %s, msg: %s", uuid, respBytes)
	}
	return http.StatusOK, nil
}

func (qk *QueryKiller) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	queryID := rest.RequestVariableLookup(req, "queryID")
	if queryID == "" {
		rest.ShowError(w, nil, "query cancel: query ID not provided",
			http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		rest.ShowError(w, nil,
			fmt.Sprintf("query cancel: could not read request body"+
				" for uuid, err: %v", err), http.StatusBadRequest)
		return
	}
	var uuid string
	if len(body) != 0 {
		r := struct {
			UUID string `json:"uuid"`
		}{}
		err = UnmarshalJSON(body, &r)
		if err != nil {
			rest.ShowError(w, nil,
				fmt.Sprintf("query cancel: could not unmarshal request body"+
					" for uuid, err: %v", err),
				http.StatusBadRequest)
			return
		}
		uuid = r.UUID
	}

	errCode, err := qk.killQuery(queryID, uuid)
	if errCode != http.StatusOK {
		rest.ShowError(w, nil, fmt.Sprintf("%v", err), errCode)
		return
	}
	if uuid == "" {
		uuid = qk.mgr.UUID()
	}
	rv := struct {
		Status string `json:"status"`
		Msg    string `json:"msg"`
	}{
		Status: "ok",
		Msg: fmt.Sprintf("query with ID '%v' on node '%s' was canceled!",
			queryID, uuid),
	}
	rest.MustEncode(w, rv)
}

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/mapping"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

func RegisterRemoteClientsForSecurity() {
	cbgt.RegisterHttpClient()
	cbgt.RegisterConfigRefreshCallback("fts/remoteClients",
		handleRefreshSecuritySettings)
}

func handleRefreshSecuritySettings(status int) error {
	if status&cbgt.AuthChange_certificates != 0 {
		resetGrpcClients()
	}
	return nil
}

// RemoteClient represents a generic interface to be implemented
// by all remote clients like IndexClient/GrpcClient.
type RemoteClient interface {
	bleve.Index

	GetHostPort() string
	GetLast() (int, []byte)
	SetStreamHandler(streamHandler)
}

type addRemoteClients func(mgr *cbgt.Manager, indexName, indexUUID string,
	remotePlanPIndexes []*cbgt.RemotePlanPIndex,
	consistencyParams *cbgt.ConsistencyParams, onlyPIndexes map[string]bool,
	collector BleveIndexCollector, groupByNode bool) ([]RemoteClient, error)

const RemoteRequestOverhead = 500 * time.Millisecond

var HttpClient = http.DefaultClient // Overridable for testability / advanced needs.

// Overridable for testability / advanced needs.
var HttpPost = func(client *http.Client,
	url string, bodyType string, body io.Reader) (*http.Response, error) {
	return client.Post(url, bodyType, body)
}

// Overridable for testability / advanced needs.
var HttpGet = func(client *http.Client, url string) (*http.Response, error) {
	return client.Get(url)
}

var indexClientUnimplementedErr = errors.New("unimplemented")

// IndexClient implements the Search() and DocCount() subset of the
// bleve.Index interface by accessing a remote cbft server via REST
// protocol.  This allows callers to add a IndexClient as a target of
// a bleve.IndexAlias, and implements cbft protocol features like
// query consistency and auth.
//
// TODO: Implement propagating auth info in IndexClient.
type IndexClient struct {
	mgr         *cbgt.Manager
	name        string
	HostPort    string
	IndexName   string
	IndexUUID   string
	PIndexNames []string
	QueryURL    string
	CountURL    string
	Consistency *cbgt.ConsistencyParams
	httpClient  *http.Client

	lastMutex        sync.RWMutex
	lastSearchStatus int
	lastErrBody      []byte
}

func (r *IndexClient) GetLast() (int, []byte) {
	r.lastMutex.RLock()
	defer r.lastMutex.RUnlock()
	return r.lastSearchStatus, r.lastErrBody
}

func (r *IndexClient) GetHostPort() string {
	return r.HostPort
}

func (r *IndexClient) Name() string {
	return r.name
}

func (r *IndexClient) SetName(name string) {
	r.name = name
}

func (r *IndexClient) Index(id string, data interface{}) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Delete(id string) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Batch(b *bleve.Batch) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Document(id string) (*document.Document, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) DocCount() (uint64, error) {
	if r.CountURL == "" {
		return 0, fmt.Errorf("remote: no CountURL provided")
	}
	u, err := UrlWithAuth(r.AuthType(), r.CountURL)
	if err != nil {
		return 0, fmt.Errorf("remote: auth for count,"+
			" countURL: %s, authType: %s, err: %v",
			r.CountURL, r.AuthType(), err)
	}

	resp, err := HttpGet(r.httpClient, u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("remote: count error reading resp.Body,"+
			" countURL: %s, resp: %#v, err: %v", r.CountURL, resp, err)
	}

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("remote: count got status code: %d,"+
			" countURL: %s, resp: %#v", resp.StatusCode, r.CountURL, resp)
	}

	rv := struct {
		Status string `json:"status"`
		Count  uint64 `json:"count"`
	}{}
	err = UnmarshalJSON(respBuf, &rv)
	if err != nil {
		return 0, fmt.Errorf("remote: count error parsing respBuf: %s,"+
			" countURL: %s, resp: %#v, err: %v",
			respBuf, r.CountURL, resp, err)
	}

	return rv.Count, nil
}

func (r *IndexClient) Search(req *bleve.SearchRequest) (
	*bleve.SearchResult, error) {
	return r.SearchInContext(context.Background(), req)
}

func (r *IndexClient) SearchInContext(ctx context.Context,
	req *bleve.SearchRequest) (*bleve.SearchResult, error) {
	if req == nil {
		return nil, fmt.Errorf("remote: no req provided")
	}

	if r.QueryURL == "" {
		return nil, fmt.Errorf("remote: no QueryURL provided")
	}

	queryCtlParams := &cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Consistency: r.Consistency,
		},
	}

	queryPIndexes := &QueryPIndexes{
		PIndexNames: r.PIndexNames,
	}

	// if timeout was set, compute time remaining
	if deadline, ok := ctx.Deadline(); ok {
		remaining := deadline.Sub(time.Now())
		// FIXME arbitrarily reducing the timeout, to increase the liklihood
		// that a live system replies via HTTP round-trip before we give up
		// on the request externally
		remaining -= RemoteRequestOverhead
		if remaining <= 0 {
			// not enough time left
			return nil, context.DeadlineExceeded
		}
		queryCtlParams.Ctl.Timeout = int64(remaining / time.Millisecond)
	}

	buf, err := MarshalJSON(struct {
		*cbgt.QueryCtlParams
		*QueryPIndexes
		*bleve.SearchRequest
	}{
		queryCtlParams,
		queryPIndexes,
		req,
	})
	if err != nil {
		return nil, err
	}

	resultCh := make(chan *bleve.SearchResult, 1)

	go func() {
		respBuf, err := r.Query(buf)
		if err != nil {
			log.Warnf("remote: Query() returned error from host: %v,"+
				" err: %v", r.HostPort, err)
			resultCh <- makeSearchResultErr(req, r.PIndexNames, err)
			return
		}

		rv := &bleve.SearchResult{
			Status: &bleve.SearchStatus{
				Errors: make(map[string]error),
			},
		}
		err = UnmarshalJSON(respBuf, rv)
		if err != nil {
			resultCh <- makeSearchResultErr(req, r.PIndexNames,
				fmt.Errorf("remote: search error parsing respBuf: %s,"+
					" queryURL: %s, err: %v", respBuf, r.QueryURL, err))
			return
		}

		resultCh <- rv
	}()

	select {
	case <-ctx.Done():
		log.Warnf("remote: scatter-gather error while awaiting results"+
			" from host: %v, err: %v", r.HostPort, ctx.Err())
		return makeSearchResultErr(req, r.PIndexNames, ctx.Err()), nil
	case rv := <-resultCh:
		return rv, nil
	}
}

func (r *IndexClient) Fields() ([]string, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) FieldDict(field string) (index.FieldDict, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) FieldDictRange(field string,
	startTerm []byte, endTerm []byte) (index.FieldDict, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) FieldDictPrefix(field string,
	termPrefix []byte) (index.FieldDict, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) DumpAll() chan interface{} {
	return nil
}

func (r *IndexClient) DumpDoc(id string) chan interface{} {
	return nil
}

func (r *IndexClient) DumpFields() chan interface{} {
	return nil
}

func (r *IndexClient) Close() error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) Mapping() mapping.IndexMapping {
	return nil
}

func (r *IndexClient) NewBatch() *bleve.Batch {
	return nil
}

func (r *IndexClient) Stats() *bleve.IndexStat {
	return nil
}

func (r *IndexClient) StatsMap() map[string]interface{} {
	return nil
}

func (r *IndexClient) GetInternal(key []byte) ([]byte, error) {
	return nil, indexClientUnimplementedErr
}

func (r *IndexClient) SetInternal(key, val []byte) error {
	return indexClientUnimplementedErr
}

func (r *IndexClient) DeleteInternal(key []byte) error {
	return indexClientUnimplementedErr
}

// -----------------------------------------------------

func (r *IndexClient) Count() (uint64, error) {
	return r.DocCount()
}

func (r *IndexClient) Query(buf []byte) ([]byte, error) {
	u, err := UrlWithAuth(r.AuthType(), r.QueryURL)
	if err != nil {
		return nil, fmt.Errorf("remote: auth for query,"+
			" queryURL: %s, authType: %s, err: %v",
			r.QueryURL, r.AuthType(), err)
	}

	req, err := http.NewRequest("POST", u, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Add(rest.CLUSTER_ACTION, "fts/scatter-gather")
	req.Header.Add("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("remote: query error reading resp.Body,"+
			" queryURL: %s, resp: %#v, err: %v", r.QueryURL, resp, err)
	}

	r.lastMutex.Lock()
	defer r.lastMutex.Unlock()

	r.lastSearchStatus = resp.StatusCode
	if resp.StatusCode != http.StatusOK {
		r.lastErrBody = respBuf
		return nil, fmt.Errorf("remote: query got status code: %d,"+
			" queryURL: %s, buf: %s, resp: %#v, err: %v",
			resp.StatusCode, r.QueryURL, buf, resp, err)
	}

	return respBuf, err
}

func (r *IndexClient) Advanced() (index.Index, store.KVStore, error) {
	return nil, nil, indexClientUnimplementedErr
}

// -----------------------------------------------------

func (r *IndexClient) AuthType() string {
	if r.mgr != nil {
		return r.mgr.Options()["authType"]
	}
	return ""
}

func (r *IndexClient) SetStreamHandler(s streamHandler) {
	// PLACEHOLDER
}

// -----------------------------------------------------

// GroupIndexClientsByHostPort groups the index clients by their
// HostPort, merging the pindexNames.  This is an enabler to allow
// scatter/gather to use fewer REST requests/connections.
func GroupIndexClientsByHostPort(clients []*IndexClient) (rv []*IndexClient, err error) {
	m := map[string]*IndexClient{}

	for _, client := range clients {
		groupByKey := client.HostPort +
			"/" + client.IndexName + "/" + client.IndexUUID

		c, exists := m[groupByKey]
		if !exists {
			prefix := ""
			if client.mgr != nil {
				prefix = client.mgr.Options()["urlPrefix"]
			}

			proto := "http://"
			if strings.Contains(client.QueryURL, "https") {
				proto = "https://"
			}

			baseURL := proto + client.HostPort +
				prefix + "/api/index/" + client.IndexName

			c = &IndexClient{
				mgr:         client.mgr,
				name:        groupByKey,
				HostPort:    client.HostPort,
				IndexName:   client.IndexName,
				IndexUUID:   client.IndexUUID,
				QueryURL:    baseURL + "/query",
				CountURL:    baseURL + "/count",
				Consistency: client.Consistency,
				httpClient:  client.httpClient,
			}

			m[groupByKey] = c

			rv = append(rv, c)
		}

		c.PIndexNames = append(c.PIndexNames, client.PIndexNames...)
	}

	return rv, nil
}

//  Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/cbgt"
)

type Collection struct {
	Uid         string `json:"uid"`
	Name        string `json:"name"`
	typeMapping string
	// is true if the collection is a synonym collection
	isSynonym bool
}

type Scope struct {
	Name        string                    `json:"name"`
	Uid         string                    `json:"uid"`
	Collections []Collection              `json:"collections"`
	Limits      map[string]map[string]int `json:"limits"`
}

func (m *Manifest) GetScopeCollectionUIDs(scopeName string, collectionNames []string) (uint64, []uint64, error) {
	var sc Scope
	for _, scope := range m.Scopes {
		if scope.Name == scopeName {
			sc = scope
		}
	}
	if len(collectionNames) == 0 {
		return 0, []uint64{}, nil
	}

	collectionMap := make(map[string]string, len(sc.Collections))
	for i := range sc.Collections {
		collectionMap[sc.Collections[i].Name] = sc.Collections[i].Uid
	}

	rv := make([]uint64, len(collectionNames))
	var err error
	for i, collectionName := range collectionNames {
		uid, exists := collectionMap[collectionName]
		if !exists {
			return 0, nil, fmt.Errorf("collection %s not found", collectionName)
		}
		rv[i], err = strconv.ParseUint(uid, 16, 32)
		if err != nil {
			return 0, nil, fmt.Errorf("error parsing collection uid %s: %v", uid, err)
		}
	}

	scopeUID, err := strconv.ParseUint(sc.Uid, 16, 32)
	if err != nil {
		return 0, nil, fmt.Errorf("error parsing scope uid %v: %w", scopeUID, err)
	}
	return scopeUID, rv, nil
}

type Manifest struct {
	Uid    string  `json:"uid"`
	Scopes []Scope `json:"scopes"`
}

type manifestResult struct {
	manifest *Manifest
	err      error
}

func (m *Manifest) GetScope(scopeName string) *Scope {
	for _, scope := range m.Scopes {
		if scope.Name == scopeName {
			return &scope
		}
	}
	return nil
}

type manifestBatch struct {
	waiters []chan manifestResult
}

// manifestBatchWindow is how long the first request for a bucket waits for
// additional concurrent requests to queue up before a single fetch is made.
const manifestBatchWindow = 50 * time.Millisecond

var (
	manifestRequestBatchMu sync.Mutex
	// manifestRequestBatches tracks in-flight manifest fetch batches per bucket name.
	// While a batch exists for a bucket, new callers join its waiters list instead
	// of issuing a separate request.
	manifestRequestBatches = map[string]*manifestBatch{}
)

// GetBucketManifest fetches the current collection manifest for a bucket.
// Batches requests to avoid blasting ns_server with requests
func GetBucketManifest(bucketName string) (*Manifest, error) {
	if CurrentNodeDefsFetcher == nil || bucketName == "" {
		return nil, fmt.Errorf("manifest: invalid input")
	}

	ch := make(chan manifestResult, 1)

	manifestRequestBatchMu.Lock()
	batch, exists := manifestRequestBatches[bucketName]
	if !exists {
		batch = &manifestBatch{}
		manifestRequestBatches[bucketName] = batch
		go runManifestBatch(bucketName, batch)
	}
	batch.waiters = append(batch.waiters, ch)
	manifestRequestBatchMu.Unlock()

	result := <-ch
	return result.manifest, result.err
}

func runManifestBatch(bucketName string, batch *manifestBatch) {
	time.Sleep(manifestBatchWindow)

	manifestRequestBatchMu.Lock()
	delete(manifestRequestBatches, bucketName)
	waiters := batch.waiters
	manifestRequestBatchMu.Unlock()

	var result manifestResult
	if CurrentNodeDefsFetcher == nil {
		result.err = fmt.Errorf("manifest: invalid input")
	} else {
		result.manifest, result.err = obtainManifest(
			CurrentNodeDefsFetcher.GetManager().Server(), bucketName)
	}

	for _, ch := range waiters {
		ch <- result
	}
}

// -----------------------------------------------------------------------------

func obtainManifest(serverURL, bucket string) (*Manifest, error) {
	if len(serverURL) == 0 || len(bucket) == 0 {
		return nil, fmt.Errorf("manifest: empty arguments")
	}

	path := fmt.Sprintf("/pools/default/buckets/%s/scopes", url.QueryEscape(bucket))
	u, err := cbgt.CBAuthURL(serverURL + path)
	if err != nil {
		return nil, fmt.Errorf("manifest: error building URL, err: %v", err)
	}

	resp, err := HttpGet(cbgt.HttpClient(), u)
	if err != nil {
		return nil, fmt.Errorf("manifest: request, err: %v", err)
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil || len(respBuf) == 0 {
		return nil, fmt.Errorf("manifest: error reading resp.Body, err: %v", err)
	}

	rv := &Manifest{}
	err = json.Unmarshal(respBuf, rv)
	if err != nil {
		return nil, fmt.Errorf("manifest: error parsing respBuf, err: %v", err)
	}

	return rv, nil
}

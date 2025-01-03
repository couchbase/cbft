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
	"sync"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

var manifestsCache *manifestCache

func init() {
	manifestsCache = &manifestCache{
		mCache: make(map[string]*Manifest, 10),
		stopCh: make(chan struct{}, 1),
	}
}

type Collection struct {
	Uid         string `json:"uid"`
	Name        string `json:"name"`
	typeMapping string
}

type Scope struct {
	Name        string                    `json:"name"`
	Uid         string                    `json:"uid"`
	Collections []Collection              `json:"collections"`
	Limits      map[string]map[string]int `json:"limits"`
}

type Manifest struct {
	Uid    string  `json:"uid"`
	Scopes []Scope `json:"scopes"`
}

// TODO manifestCache needs to be refreshed from the future
// streaming APIs from the ns_server in real time
type manifestCache struct {
	once           sync.Once
	m              sync.RWMutex
	stopCh         chan struct{}
	mCache         map[string]*Manifest // bucketName<=>Manifest
	monitorRunning bool
}

func GetBucketManifest(bucketName string) (*Manifest, error) {
	return manifestsCache.getBucketManifest(bucketName)
}

func (c *manifestCache) getBucketManifest(bucketName string) (*Manifest, error) {
	c.m.RLock()
	if m, exists := c.mCache[bucketName]; exists {
		c.m.RUnlock()
		return m, nil
	}
	c.m.RUnlock()
	manifest, err := c.fetchCollectionManifest(bucketName)
	if err != nil {
		return nil, err
	}
	c.m.Lock()
	c.mCache[bucketName] = manifest
	c.once.Do(func() { go c.monitor() })
	c.m.Unlock()
	return manifest, nil
}

func (c *manifestCache) fetchCollectionManifest(bucket string) (*Manifest, error) {
	if CurrentNodeDefsFetcher == nil || bucket == "" {
		return nil, fmt.Errorf("invalid input")
	}

	return obtainManifest(CurrentNodeDefsFetcher.GetManager().Server(), bucket)
}

func removeBucketFromManifestCache(bucket string) {
	manifestsCache.m.Lock()
	delete(manifestsCache.mCache, bucket)
	if len(manifestsCache.mCache) == 0 && manifestsCache.monitorRunning &&
		len(manifestsCache.stopCh) == 0 {
		manifestsCache.stopCh <- struct{}{}
		manifestsCache.once = sync.Once{}
		manifestsCache.monitorRunning = false
	}
	manifestsCache.m.Unlock()
}

func (c *manifestCache) monitor() {
	// TODO - until the streaming endpoints from ns_server
	mTicker := time.NewTicker(1 * time.Second)
	defer mTicker.Stop()
	c.m.Lock()
	c.monitorRunning = true
	c.m.Unlock()
	for {
		select {
		case <-c.stopCh:
			return

		case _, ok := <-mTicker.C:
			if !ok {
				return
			}
			c.m.RLock()
			manifestCache := c.mCache
			c.m.RUnlock()
			for bucket, old := range manifestCache {
				curr, err := c.fetchCollectionManifest(bucket)
				if err != nil {
					log.Debugf("manifest: manifest refresh failed for bucket %s, err: %v",
						bucket, err)
					continue
				}
				if old.Uid != curr.Uid {
					c.m.Lock()
					c.mCache[bucket] = curr
					c.m.Unlock()
				}
			}
		}
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

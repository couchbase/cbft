//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cbft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

var manifestsCache *manifestCache

func init() {
	manifestsCache = &manifestCache{mCache: make(map[string]*Manifest, 10)}
}

type Collection struct {
	Uid  string `json:"uid"`
	Name string `json:"name"`
}

type Scope struct {
	Name        string       `json:"name"`
	Uid         string       `json:"uid"`
	Collections []Collection `json:"collections"`
}

type Manifest struct {
	Uid    string  `json:"uid"`
	Scopes []Scope `json:"scopes"`
}

// TODO manifestCache needs to be refreshed from the future
// streaming APIs from the ns_server in real time
type manifestCache struct {
	once   sync.Once
	m      sync.RWMutex
	stopCh chan struct{}
	mCache map[string]*Manifest // bucketName<=>Manifest
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
	url := fmt.Sprintf("/pools/default/buckets/%s/collections", bucket)
	url = CurrentNodeDefsFetcher.GetManager().Server() + url
	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return nil, fmt.Errorf("manifest: auth for ns_server,"+
			" url: %s, authType: %s, err: %v",
			url, "cbauth", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil || len(respBuf) == 0 {
		return nil, fmt.Errorf("manifest: error reading resp.Body,"+
			" url: %s, resp: %#v, err: %v", url, resp, err)
	}

	rv := &Manifest{}
	err = json.Unmarshal(respBuf, rv)
	if err != nil {
		return nil, fmt.Errorf("manifest: error parsing respBuf: %s,"+
			" url: %s, err: %v", respBuf, url, err)

	}
	return rv, nil
}

func (c *manifestCache) monitor() {
	// TODO - until the streaming endpoints from ns_server
	mTicker := time.NewTicker(1 * time.Second)
	defer mTicker.Stop()

	for {
		select {
		case <-c.stopCh:
			return

		case _, ok := <-mTicker.C:
			if !ok {
				return
			}

			for bucket, old := range c.mCache {
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

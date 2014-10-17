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
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

type ManagerEventHandlers interface {
	OnRegisterPIndex(pindex *PIndex)
	OnUnregisterPIndex(pindex *PIndex)
}

type Manager struct {
	dataDir   string
	server    string
	m         sync.Mutex
	feeds     map[string]Feed
	pindexes  map[string]*PIndex
	janitorCh chan bool // Used to kick the janitor awake.
	meh       ManagerEventHandlers
}

func NewManager(dataDir, server string, meh ManagerEventHandlers) *Manager {
	return &Manager{
		dataDir:   dataDir,
		server:    server,
		feeds:     make(map[string]Feed),
		pindexes:  make(map[string]*PIndex),
		janitorCh: make(chan bool),
		meh:       meh,
	}
}

func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

func (mgr *Manager) PIndexPath(pindexName string) string {
	return PIndexPath(mgr.dataDir, pindexName)
}

func (mgr *Manager) ParsePIndexPath(pindexPath string) (string, bool) {
	return ParsePIndexPath(mgr.dataDir, pindexPath)
}

func (mgr *Manager) Start() error {
	// TODO: if we one day handle multiple "seed" servers, what if
	// those seeds actually come from different clusters?  Can we have
	// multiple clusters fan-in to a single cbft?
	//
	// First, check if couchbase server is valid, else exit early.
	// Afterwards, any later loss of couchbase conns, in contrast,
	// won't exit the server, where cbft will instead retry/reconnect.
	// Empty server ("") allows for unit testing.
	if mgr.server != "" {
		_, err := couchbase.Connect(mgr.server)
		if err != nil {
			return fmt.Errorf("error: could not connect to couchbase server URL: %s, err: %v",
				mgr.server, err)
		}
	}

	// walk the data dir and register pindexes
	log.Printf("scanning dataDir...")
	dirEntries, err := ioutil.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("error: could not read dataDir: %s, err: %v",
			mgr.dataDir, err)
	}

	for _, dirInfo := range dirEntries {
		path := mgr.dataDir + string(os.PathSeparator) + dirInfo.Name()
		name, ok := mgr.ParsePIndexPath(path)
		if !ok {
			log.Printf("  skipping: %s", dirInfo.Name())
			continue
		}

		log.Printf("  opening pindex: %s", name)
		pindex, err := OpenPIndex(name, path)
		if err != nil {
			log.Printf("error: could not open pindex: %s, err: %v",
				path, err)
			continue
		}

		// TODO: Need bucket UUID.
		err = mgr.FeedPIndex(pindex) // TODO: err handling.
		if err != nil {
			log.Printf("error: could not feed pindex: %s, err: %v",
				path, err)
			continue
		}
	}

	return nil
}

// Creates a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) CreateIndex(bucketName, bucketUUID,
	indexName string, indexMappingBytes []byte) error {
	// TODO: a logical index might map to multiple PIndexes, not the current 1-to-1.
	indexPath := mgr.PIndexPath(indexName)

	// TODO: need to check if this pindex already exists?
	// TODO: need to alloc a version/uuid for the pindex?
	// TODO: need to save feed reconstruction info (bucketName, bucketUUID, etc)
	//   with the pindex
	pindex, err := NewPIndex(indexName, indexPath, indexMappingBytes)
	if err != nil {
		return fmt.Errorf("error running pindex: %v", err)
	}

	err = mgr.FeedPIndex(pindex)
	if err != nil {
		// TODO: cleanup?
		return fmt.Errorf("error feeding pindex: %v", err)
	}

	// TODO: Create a uuid for the index?

	return nil
}

// Deletes a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) DeleteIndex(indexName string) error {
	// try to stop the feed
	// TODO: should be future, multiple feeds
	feed := mgr.UnregisterFeed(indexName)
	if feed != nil {
		err := feed.Close()
		if err != nil {
			log.Printf("error closing stream: %v", err)
		}
		// not returning error here
		// because we still want to try and delete it
	}

	pindex := mgr.UnregisterPIndex(indexName)
	if pindex != nil {
		// TODO: if we closed the stream right now, then the feed might
		// incorrectly try writing to a closed channel.
		// If there is multiple feeds going into one stream (fan-in)
		// then need to know how to count down to the final Close().
		// err := stream.Close()
		// if err != nil {
		// 	log.Printf("error closing pindex: %v", err)
		// }
		// not returning error here
		// because we still want to try and delete it
	}

	// TODO: what about any inflight queries or ops?

	// close the index
	// TODO: looks like the pindex should be responsible
	// for the final bleve.Close()
	// and actual subdirectory deletes?
	// indexToDelete := bleveHttp.UnregisterIndexByName(indexName)
	// if indexToDelete == nil {
	// 	log.Printf("no such index '%s'", indexName)
	//  }
	// indexToDelete.Close()
	pindex.BIndex().Close()

	// now delete it
	// TODO: should really send a msg to PIndex who's responsible for
	// actual file / subdir to do the deletion rather than here.

	return os.RemoveAll(mgr.PIndexPath(indexName))
}

// Maintains feeds, creating and deleting as necessary.
func (mgr *Manager) StartFeedsJanitor(pindex *PIndex) {
	go func() {
		for _ = range mgr.janitorCh {
			startFeeds := make(map[string]Feed)
			startPIndexes := make(map[string]*PIndex)

			// Get a snapshot of our current feeds and pindexes.
			mgr.m.Lock()
			for k, v := range mgr.feeds {
				startFeeds[k] = v
			}
			for k, v := range mgr.pindexes {
				startPIndexes[k] = v
			}
			mgr.m.Unlock()

			// TODO: create feeds that we're missing.
			// TODO: teardown feeds that we have too much of.
		}
	}()
}

func (mgr *Manager) FeedPIndex(pindex *PIndex) error {
	indexName := pindex.name // TODO: bad assumption of 1-to-1 pindex.name to indexName

	mgr.RegisterPIndex(indexName, pindex)

	bucketName := indexName // TODO: read bucketName out of bleve storage.
	bucketUUID := ""        // TODO: read bucketUUID and vbucket list out of bleve storage.
	feed, err := NewTAPFeed(mgr.server, "default", bucketName, bucketUUID)
	if err != nil {
		return fmt.Errorf("error: could not prepare TAP stream to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
		// TODO: need a way to collect these errors so REST api
		// can show them to user ("hey, perhaps you deleted a bucket
		// and should delete these related full-text indexes?
		// or the couchbase cluster is just down.");
		// perhaps as specialized clog writer?
		// TODO: cleanup on error?
	}

	if err = feed.Start(); err != nil {
		// TODO: need way to track dead cows (non-beef)
		// TODO: cleanup?
		return fmt.Errorf("error: could not start feed, server: %s, err: %v",
			mgr.server, err)
	}

	mgr.RegisterFeed(indexName, feed) // TODO: Need to figure out feed names.

	return nil
}

func (mgr *Manager) RegisterFeed(name string, feed Feed) {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	mgr.feeds[name] = feed
}

func (mgr *Manager) UnregisterFeed(name string) Feed {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	rv, ok := mgr.feeds[name]
	if ok {
		delete(mgr.feeds, name)
		return rv
	}
	return nil
}

func (mgr *Manager) RegisterPIndex(name string, pindex *PIndex) {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	mgr.pindexes[name] = pindex
	if mgr.meh != nil {
		mgr.meh.OnRegisterPIndex(pindex)
	}
}

func (mgr *Manager) UnregisterPIndex(name string) *PIndex {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	pindex, ok := mgr.pindexes[name]
	if ok {
		delete(mgr.pindexes, name)
		if mgr.meh != nil {
			mgr.meh.OnUnregisterPIndex(pindex)
		}
		return pindex
	}
	return nil
}

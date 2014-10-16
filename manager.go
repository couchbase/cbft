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
	"strings"
	"sync"

	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

const indexPathSuffix string = ".cbft"

type ManagerEventHandlers interface {
	OnRegisterPIndex(pindexName string, pindex *PIndex)
	OnUnregisterPIndex(pindexName string)
}

type Manager struct {
	dataDir  string
	server   string
	m        sync.Mutex
	feeds    map[string]Feed
	pindexes map[string]*PIndex

	meh ManagerEventHandlers
}

func NewManager(dataDir, server string, meh ManagerEventHandlers) *Manager {
	return &Manager{
		dataDir:  dataDir,
		server:   server,
		feeds:    make(map[string]Feed),
		pindexes: make(map[string]*PIndex),
	}
}

func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

func (mgr *Manager) IndexPath(indexName string) string {
	// TODO: path security checks / mapping here; ex: "../etc/pswd"
	return mgr.dataDir + string(os.PathSeparator) + indexName + indexPathSuffix
}

func (mgr *Manager) ParseIndexPath(indexPath string) (string, bool) {
	if !strings.HasSuffix(indexPath, indexPathSuffix) {
		return "", false
	}
	prefix := mgr.dataDir + string(os.PathSeparator)
	if !strings.HasPrefix(indexPath, prefix) {
		return "", false
	}
	indexName := indexPath[len(prefix):]
	indexName = indexName[0 : len(indexName)-len(indexPathSuffix)]
	return indexName, true
}

func (mgr *Manager) Start() error {
	// First, check if couchbase server is invalid to exit early.
	// Afterwards, any later loss of couchbase conns, in contrast,
	// won't exit the server, where cbft will instead retry/reconnect.

	// TODO: if we handle multiple "seed" servers, what if those
	// seeds actually come from different clusters?  Can we have
	// multiple clusters fan-in to a single cbft?
	_, err := couchbase.Connect(mgr.server)
	if err != nil {
		return fmt.Errorf("error: could not connect to couchbase server URL: %s, err: %v",
			mgr.server, err)
	}

	// walk the data dir and register index names
	log.Printf("scanning dataDir...")
	dirEntries, err := ioutil.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("error: could not read dataDir: %v, err: %v",
			mgr.dataDir, err)
	}

	for _, dirInfo := range dirEntries {
		indexPath := mgr.dataDir + string(os.PathSeparator) + dirInfo.Name()
		indexName, ok := mgr.ParseIndexPath(indexPath)
		if !ok {
			log.Printf("  skipping dataDir entry: %s", indexPath)
			continue
		}

		log.Printf("  opening dataDir entry: %s", indexPath)
		pindex, err := OpenPIndex(indexName, indexPath)
		if err != nil {
			log.Printf("error: could not open indexPath: %v, err: %v",
				indexPath, err)
			continue
		}

		mgr.FeedPIndex(indexName, indexName, pindex) // TODO: err handling.
	}

	return nil
}

func (mgr *Manager) FeedPIndex(bucketName, indexName string, pindex *PIndex) error {
	mgr.RegisterPIndex(pindex.indexName, pindex)

	// TODO: This shouldn't really go here, so need a separate Feed creator.
	// TODO: Also, for now indexName == bucketName.
	// make sure there is a bucket with this name
	uuid := "" // TODO: read bucket UUID and vbucket list out of bleve storage.
	feed, err := NewTAPFeed(mgr.server, "default", bucketName, uuid)
	if err != nil {
		return fmt.Errorf("error: could not prepare TAP stream to server: %s,"+
			" indexName: %s, err: %v", mgr.server, indexName, err)
		// TODO: need a way to collect these errors so REST api
		// can show them to user ("hey, perhaps you deleted a bucket
		// and should delete these related full-text indexes?
		// or the couchbase cluster is just down.")
		// TODO: cleanup?
	}

	if err = feed.Start(); err != nil {
		// TODO: need way to track dead cows (non-beef)
		// TODO: cleanup?
		return fmt.Errorf("error: could not start feed: %v, err: %v",
			server, err)
	}

	mgr.RegisterFeed(indexName, feed)

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
		mgr.meh.OnRegisterPIndex(name, pindex)
	}
}

func (mgr *Manager) UnregisterPIndex(name string) *PIndex {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	rv, ok := mgr.pindexes[name]
	if ok {
		delete(mgr.pindexes, name)
		if mgr.meh != nil {
			mgr.meh.OnUnregisterPIndex(name)
		}
		return rv
	}
	return nil
}

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
)

type ManagerEventHandlers interface {
	OnRegisterPIndex(pindex *PIndex)
	OnUnregisterPIndex(pindex *PIndex)
}

type Manager struct {
	cfg       Cfg
	dataDir   string
	server    string // The datasource that cbft will index.
	m         sync.Mutex
	feeds     map[string]Feed
	pindexes  map[string]*PIndex
	plannerCh chan bool // Used to kick the planner that there's more work.
	janitorCh chan bool // Used to kick the janitor that there's more work.
	meh       ManagerEventHandlers
}

func NewManager(cfg Cfg, dataDir string, server string,
	meh ManagerEventHandlers) *Manager {
	return &Manager{
		cfg:       cfg,
		dataDir:   dataDir,
		server:    server,
		feeds:     make(map[string]Feed),
		pindexes:  make(map[string]*PIndex),
		plannerCh: make(chan bool),
		janitorCh: make(chan bool),
		meh:       meh,
	}
}

func (mgr *Manager) Start() error {
	// TODO: Write our cbft-ID into the cfg.

	if err := mgr.LoadDataDir(); err != nil {
		return err
	}

	go mgr.PlannerLoop()
	mgr.plannerCh <- true

	go mgr.JanitorLoop()
	mgr.janitorCh <- true

	return nil
}

func (mgr *Manager) LoadDataDir() error {
	// walk the data dir and register pindexes
	log.Printf("loading dataDir...")
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

		mgr.RegisterPIndex(pindex)
	}

	return nil
}

func (mgr *Manager) RegisterFeed(feed Feed) error {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if _, exists := mgr.feeds[feed.Name()]; exists {
		return fmt.Errorf("error: registered feed already exists, name: %s",
			feed.Name())
	}
	mgr.feeds[feed.Name()] = feed
	return nil
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

func (mgr *Manager) RegisterPIndex(pindex *PIndex) error {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if _, exists := mgr.pindexes[pindex.Name()]; exists {
		return fmt.Errorf("error: registered pindex already exists, name: %s",
			pindex.Name())
	}
	mgr.pindexes[pindex.Name()] = pindex
	if mgr.meh != nil {
		mgr.meh.OnRegisterPIndex(pindex)
	}
	return nil
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

// Returns a snapshot copy of the current feeds and pindexes.
func (mgr *Manager) CurrentMaps() (map[string]Feed, map[string]*PIndex) {
	feeds := make(map[string]Feed)
	pindexes := make(map[string]*PIndex)

	mgr.m.Lock()
	defer mgr.m.Unlock()

	for k, v := range mgr.feeds {
		feeds[k] = v
	}
	for k, v := range mgr.pindexes {
		pindexes[k] = v
	}
	return feeds, pindexes
}

func (mgr *Manager) PIndexPath(pindexName string) string {
	return PIndexPath(mgr.dataDir, pindexName)
}

func (mgr *Manager) ParsePIndexPath(pindexPath string) (string, bool) {
	return ParsePIndexPath(mgr.dataDir, pindexPath)
}

func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

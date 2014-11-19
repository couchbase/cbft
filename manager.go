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
	"reflect"
	"sync"
	"time"

	log "github.com/couchbaselabs/clog"
)

type Manager struct {
	startTime time.Time
	version   string // Our software VERSION.
	cfg       Cfg
	uuid      string          // Unique to every Manager instance.
	tags      []string        // The tags at Manager start.
	tagsMap   map[string]bool // The tags at Manager start, mapped for performance.
	bindAddr  string
	dataDir   string
	server    string // The datasource that cbft will index.

	m         sync.Mutex
	feeds     map[string]Feed    // Key is Feed.Name().
	pindexes  map[string]*PIndex // Key is PIndex.Name().
	plannerCh chan *WorkReq      // Used to kick the planner that there's more work.
	janitorCh chan *WorkReq      // Used to kick the janitor that there's more work.
	meh       ManagerEventHandlers

	lastIndexDefs          *IndexDefs
	lastIndexDefsByName    map[string]*IndexDef
	lastPlanPIndexes       *PlanPIndexes
	lastPlanPIndexesByName map[string][]*PlanPIndex
}

type ManagerEventHandlers interface {
	OnRegisterPIndex(pindex *PIndex)
	OnUnregisterPIndex(pindex *PIndex)
}

func NewManager(version string, cfg Cfg, uuid string, tags []string,
	bindAddr, dataDir string, server string, meh ManagerEventHandlers) *Manager {
	return &Manager{
		startTime: time.Now(),
		version:   version,
		cfg:       cfg,
		uuid:      uuid,
		tags:      tags,
		tagsMap:   StringsToMap(tags),
		bindAddr:  bindAddr,
		dataDir:   dataDir,
		server:    server,
		feeds:     make(map[string]Feed),
		pindexes:  make(map[string]*PIndex),
		plannerCh: make(chan *WorkReq),
		janitorCh: make(chan *WorkReq),
		meh:       meh,
	}
}

func (mgr *Manager) Start(registerAsWanted bool) error {
	// Save our nodeDef (with our UUID) into the Cfg.
	if err := mgr.SaveNodeDef(NODE_DEFS_KNOWN); err != nil {
		return err
	}
	if registerAsWanted {
		if err := mgr.SaveNodeDef(NODE_DEFS_WANTED); err != nil {
			return err
		}
	}

	if mgr.tagsMap == nil || mgr.tagsMap["pindex"] {
		if err := mgr.LoadDataDir(); err != nil {
			return err
		}
	}

	if mgr.tagsMap == nil || mgr.tagsMap["planner"] {
		go mgr.PlannerLoop()
		go mgr.PlannerKick("start")
	}

	if mgr.tagsMap == nil || (mgr.tagsMap["pindex"] && mgr.tagsMap["janitor"]) {
		go mgr.JanitorLoop()
		go mgr.JanitorKick("start")
	}

	if mgr.cfg != nil { // TODO: err handling for Cfg subscriptions.
		go func() {
			ei := make(chan CfgEvent)
			mgr.cfg.Subscribe(INDEX_DEFS_KEY, ei)
			for _ = range ei {
				mgr.GetIndexDefs(true)
			}
		}()
		go func() {
			ep := make(chan CfgEvent)
			mgr.cfg.Subscribe(PLAN_PINDEXES_KEY, ep)
			for _ = range ep {
				mgr.GetPlanPIndexes(true)
			}
		}()
	}

	return nil
}

// ---------------------------------------------------------------

func (mgr *Manager) SaveNodeDef(kind string) error {
	if mgr.cfg == nil {
		return nil // Occurs during testing.
	}

	for {
		nodeDefs, cas, err := CfgGetNodeDefs(mgr.cfg, kind)
		if err != nil {
			return err
		}
		if nodeDefs == nil {
			nodeDefs = NewNodeDefs(mgr.version)
		}
		nodeDef, exists := nodeDefs.NodeDefs[mgr.bindAddr]
		if exists {
			// TODO: need a way to force overwrite other node's UUID?
			if nodeDef.UUID != mgr.uuid {
				return fmt.Errorf("some other node is running at our bindAddr: %v,"+
					" with different uuid: %s, than our uuid: %s",
					mgr.bindAddr, nodeDef.UUID, mgr.uuid)
			}
			if nodeDef.ImplVersion == mgr.version &&
				reflect.DeepEqual(nodeDef.Tags, mgr.tags) {
				return nil // No changes, so leave the existing nodeDef.
			}
		}

		nodeDef = &NodeDef{
			HostPort:    mgr.bindAddr, // TODO: need FQDN:port instead of ":8095".
			UUID:        mgr.uuid,
			ImplVersion: mgr.version,
			Tags:        mgr.tags,
		}

		nodeDefs.UUID = NewUUID()
		nodeDefs.NodeDefs[mgr.bindAddr] = nodeDef
		nodeDefs.ImplVersion = mgr.version // TODO: ImplVersion bump?

		_, err = CfgSetNodeDefs(mgr.cfg, kind, nodeDefs, cas)
		if err != nil {
			if _, ok := err.(*CfgCASError); ok {
				// Retry if it was a CAS mismatch, as perhaps
				// multiple nodes are all racing to register themselves,
				// such as in a full datacenter power restart.
				continue
			}
			return err
		}
		break
	}
	return nil
}

// ---------------------------------------------------------------

// Walk the data dir and register pindexes.
func (mgr *Manager) LoadDataDir() error {
	log.Printf("loading dataDir...")

	dirEntries, err := ioutil.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("error: could not read dataDir: %s, err: %v",
			mgr.dataDir, err)
	}

	for _, dirInfo := range dirEntries {
		path := mgr.dataDir + string(os.PathSeparator) + dirInfo.Name()
		_, ok := mgr.ParsePIndexPath(path)
		if !ok {
			continue // Skip the entry that doesn't match the naming pattern.
		}

		log.Printf("  opening pindex: %s", path)
		pindex, err := OpenPIndex(mgr, path)
		if err != nil {
			log.Printf("error: could not open pindex: %s, err: %v",
				path, err)
			continue
		}

		mgr.registerPIndex(pindex)
	}

	log.Printf("loading dataDir... done")
	return nil
}

// ---------------------------------------------------------------

func (mgr *Manager) Kick(msg string) {
	mgr.PlannerKick(msg)
	mgr.JanitorKick(msg)
}

// ---------------------------------------------------------------

func (mgr *Manager) ClosePIndex(pindex *PIndex) error {
	return SyncWorkReq(mgr.janitorCh, JANITOR_CLOSE_PINDEX, "api-ClosePIndex", pindex)
}

func (mgr *Manager) RemovePIndex(pindex *PIndex) error {
	return SyncWorkReq(mgr.janitorCh, JANITOR_REMOVE_PINDEX, "api-RemovePIndex", pindex)
}

func (mgr *Manager) registerPIndex(pindex *PIndex) error {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if _, exists := mgr.pindexes[pindex.Name]; exists {
		return fmt.Errorf("error: registered pindex already exists, name: %s",
			pindex.Name)
	}
	mgr.pindexes[pindex.Name] = pindex
	if mgr.meh != nil {
		mgr.meh.OnRegisterPIndex(pindex)
	}
	return nil
}

func (mgr *Manager) unregisterPIndex(name string) *PIndex {
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

// ---------------------------------------------------------------

func (mgr *Manager) registerFeed(feed Feed) error {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if _, exists := mgr.feeds[feed.Name()]; exists {
		return fmt.Errorf("error: registered feed already exists, name: %s",
			feed.Name())
	}
	mgr.feeds[feed.Name()] = feed
	return nil
}

func (mgr *Manager) unregisterFeed(name string) Feed {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	rv, ok := mgr.feeds[name]
	if ok {
		delete(mgr.feeds, name)
		return rv
	}
	return nil
}

// ---------------------------------------------------------------

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

// ---------------------------------------------------------------

// Returns read-only snapshot of the IndexDefs, also with IndexDef's
// organized by name.  Use refresh of true to force a read from Cfg.
func (mgr *Manager) GetIndexDefs(refresh bool) (
	*IndexDefs, map[string]*IndexDef, error) {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if mgr.lastIndexDefs == nil || refresh {
		indexDefs, _, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			return nil, nil, err
		}
		mgr.lastIndexDefs = indexDefs

		mgr.lastIndexDefsByName = make(map[string]*IndexDef)
		if indexDefs != nil {
			for _, indexDef := range indexDefs.IndexDefs {
				mgr.lastIndexDefsByName[indexDef.Name] = indexDef
			}
		}
	}

	return mgr.lastIndexDefs, mgr.lastIndexDefsByName, nil
}

// Returns read-only snapshot of the PlanPIndexes, also with PlanPIndex's
// organized by IndexName.  Use refresh of true to force a read from Cfg.
func (mgr *Manager) GetPlanPIndexes(refresh bool) (
	*PlanPIndexes, map[string][]*PlanPIndex, error) {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if mgr.lastPlanPIndexes == nil || refresh {
		planPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
		if err != nil {
			return nil, nil, err
		}
		mgr.lastPlanPIndexes = planPIndexes

		mgr.lastPlanPIndexesByName = make(map[string][]*PlanPIndex)
		if planPIndexes != nil {
			for _, planPIndex := range planPIndexes.PlanPIndexes {
				a := mgr.lastPlanPIndexesByName[planPIndex.IndexName]
				if a == nil {
					a = make([]*PlanPIndex, 0)
				}
				mgr.lastPlanPIndexesByName[planPIndex.IndexName] =
					append(a, planPIndex)
			}
		}
	}

	return mgr.lastPlanPIndexes, mgr.lastPlanPIndexesByName, nil
}

// ---------------------------------------------------------------

func (mgr *Manager) PIndexPath(pindexName string) string {
	return PIndexPath(mgr.dataDir, pindexName)
}

func (mgr *Manager) ParsePIndexPath(pindexPath string) (string, bool) {
	return ParsePIndexPath(mgr.dataDir, pindexPath)
}

// ---------------------------------------------------------------

func (mgr *Manager) Cfg() Cfg {
	return mgr.cfg
}

func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

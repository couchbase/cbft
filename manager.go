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

package cbft

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/couchbase/clog"
)

// A Manager represents a runtime node in a cluster.
//
// Although often used like a singleton, multiple Manager instances
// can be instantiated in a process to simulate a cluster of nodes.
//
// A Manager has two related child, actor-like goroutines:
// - planner
// - janitor
//
// A planner splits index definitions into index partitions (pindexes)
// and assigns those pindexes to nodes.  A planner wakes up and runs
// whenever the index definitions change or the set of nodes changes
// (which are both read from the Cfg system).  A planner stores the
// latest plans into the Cfg system.
//
// A janitor running on each node maintains runtime PIndex and Feed
// instances, creating, deleting & hooking them up as necessary to try
// to match to latest plans from the planner.  A janitor wakes up and
// runs whenever it sees that latest plans in the Cfg have changed.
type Manager struct {
	startTime time.Time
	version   string // See VERSION.
	cfg       Cfg
	uuid      string          // Unique to every Manager instance.
	tags      []string        // The tags at Manager start.
	tagsMap   map[string]bool // The tags at Manager start, performance opt.
	container string          // '/' separated containment path (optional).
	weight    int
	extras    string
	bindHttp  string
	dataDir   string
	server    string // The default datasource that will be indexed.

	m         sync.Mutex
	feeds     map[string]Feed    // Key is Feed.Name().
	pindexes  map[string]*PIndex // Key is PIndex.Name().
	plannerCh chan *workReq      // Used to kick planner that there's more work.
	janitorCh chan *workReq      // Used to kick janitor that there's more work.
	meh       ManagerEventHandlers

	lastIndexDefs          *IndexDefs
	lastIndexDefsByName    map[string]*IndexDef
	lastPlanPIndexes       *PlanPIndexes
	lastPlanPIndexesByName map[string][]*PlanPIndex

	stats  ManagerStats
	events *list.List
}

// ManagerStats represents the stats/metrics tracked by a Manager
// instance.
type ManagerStats struct {
	TotKick uint64

	TotSaveNodeDef             uint64
	TotSaveNodeDefGetErr       uint64
	TotSaveNodeDefSetErr       uint64
	TotSaveNodeDefUUIDTakenErr uint64
	TotSaveNodeDefUUIDErr      uint64
	TotSaveNodeDefOk           uint64

	TotCreateIndex    uint64
	TotCreateIndexOk  uint64
	TotDeleteIndex    uint64
	TotDeleteIndexOk  uint64
	TotIndexControl   uint64
	TotIndexControlOk uint64

	TotPlannerNOOP              uint64
	TotPlannerNOOPOk            uint64
	TotPlannerKick              uint64
	TotPlannerKickStart         uint64
	TotPlannerKickChanged       uint64
	TotPlannerKickErr           uint64
	TotPlannerKickOk            uint64
	TotPlannerUnknownErr        uint64
	TotPlannerSubscriptionEvent uint64

	TotJanitorNOOP              uint64
	TotJanitorNOOPOk            uint64
	TotJanitorKick              uint64
	TotJanitorKickStart         uint64
	TotJanitorKickErr           uint64
	TotJanitorKickOk            uint64
	TotJanitorClosePIndex       uint64
	TotJanitorRemovePIndex      uint64
	TotJanitorUnknownErr        uint64
	TotJanitorSubscriptionEvent uint64
}

// MANAGER_MAX_EVENTS limits the number of events tracked by a Manager
// for diagnosis/debugging.
const MANAGER_MAX_EVENTS = 10

// ManagerEventHandlers represents the callback interface where an
// application can receive important event callbacks from a Manager.
type ManagerEventHandlers interface {
	OnRegisterPIndex(pindex *PIndex)
	OnUnregisterPIndex(pindex *PIndex)
}

// NewManager returns a new, ready-to-be-started Manager instance.
func NewManager(version string, cfg Cfg, uuid string, tags []string,
	container string, weight int, extras, bindHttp, dataDir, server string,
	meh ManagerEventHandlers) *Manager {
	return &Manager{
		startTime: time.Now(),
		version:   version,
		cfg:       cfg,
		uuid:      uuid,
		tags:      tags,
		tagsMap:   StringsToMap(tags),
		container: container,
		weight:    weight,
		extras:    extras,
		bindHttp:  bindHttp, // TODO: Need FQDN:port instead of ":8095".
		dataDir:   dataDir,
		server:    server,
		feeds:     make(map[string]Feed),
		pindexes:  make(map[string]*PIndex),
		plannerCh: make(chan *workReq),
		janitorCh: make(chan *workReq),
		meh:       meh,
		events:    list.New(),
	}
}

// Start will start and register a Manager instance with its
// configured Cfg system, based on the register parameter.  See
// Manager.StartRegister().
func (mgr *Manager) Start(register string) error {
	err := mgr.StartRegister(register)
	if err != nil {
		return err
	}

	if mgr.tagsMap == nil || mgr.tagsMap["pindex"] {
		err := mgr.LoadDataDir()
		if err != nil {
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

	if mgr.cfg != nil { // TODO: Need err handling for Cfg subscriptions.
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

// StartRegister will register or unregister a Manager with its
// configured Cfg system, based on the register parameter, which can
// have these values:
// * wanted - register this node as wanted
// * wantedForce - same as wanted, but force a Cfg update
// * known - register this node as known
// * knownForce - same as unknown, but force a Cfg update
// * unwanted - unregister this node no longer wanted
// * unknown - unregister this node no longer wanted and no longer known
// * unchanged - don't change any Cfg registrations for this node
func (mgr *Manager) StartRegister(register string) error {
	if register == "unchanged" {
		return nil
	}
	if register == "unwanted" || register == "unknown" {
		err := mgr.RemoveNodeDef(NODE_DEFS_WANTED)
		if err != nil {
			return err
		}
		if register == "unknown" {
			err := mgr.RemoveNodeDef(NODE_DEFS_KNOWN)
			if err != nil {
				return err
			}
		}
	}
	if register == "known" || register == "knownForce" ||
		register == "wanted" || register == "wantedForce" {
		// Save our nodeDef (with our UUID) into the Cfg as a known node.
		err := mgr.SaveNodeDef(NODE_DEFS_KNOWN, register == "knownForce")
		if err != nil {
			return err
		}
		if register == "wanted" || register == "wantedForce" {
			// Save our nodeDef (with our UUID) into the Cfg as a wanted node.
			err := mgr.SaveNodeDef(NODE_DEFS_WANTED, register == "wantedForce")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------

// SaveNodeDef updates the NodeDef registrations in the Cfg system for
// this Manager node instance.
func (mgr *Manager) SaveNodeDef(kind string, force bool) error {
	atomic.AddUint64(&mgr.stats.TotSaveNodeDef, 1)

	if mgr.cfg == nil {
		atomic.AddUint64(&mgr.stats.TotSaveNodeDefOk, 1)
		return nil // Occurs during testing.
	}

	nodeDef := &NodeDef{
		HostPort:    mgr.bindHttp,
		UUID:        mgr.uuid,
		ImplVersion: mgr.version,
		Tags:        mgr.tags,
		Container:   mgr.container,
		Weight:      mgr.weight,
		Extras:      mgr.extras,
	}

	for {
		nodeDefs, cas, err := CfgGetNodeDefs(mgr.cfg, kind)
		if err != nil {
			atomic.AddUint64(&mgr.stats.TotSaveNodeDefGetErr, 1)
			return err
		}
		if nodeDefs == nil {
			nodeDefs = NewNodeDefs(mgr.version)
		}
		for _, nodeDef := range nodeDefs.NodeDefs {
			if nodeDef.UUID == mgr.uuid &&
				nodeDef.HostPort != mgr.bindHttp {
				atomic.AddUint64(&mgr.stats.TotSaveNodeDefUUIDTakenErr, 1)
				return fmt.Errorf("manager:"+
					" another node is running with our uuid: %s,"+
					" but at different bindHttp: %s, than our bindHttp: %s",
					mgr.uuid, nodeDef.HostPort, mgr.bindHttp)
			}
		}
		nodeDefPrev, exists := nodeDefs.NodeDefs[mgr.bindHttp]
		if exists && !force {
			// If a previous entry exists, do some double-checking
			// before we overwrite the entry with our entry.
			if nodeDefPrev.UUID != mgr.uuid {
				atomic.AddUint64(&mgr.stats.TotSaveNodeDefUUIDErr, 1)
				return fmt.Errorf("manager:"+
					" another node is running at our bindHttp: %s,"+
					" with a different uuid: %s, than our uuid: %s",
					mgr.bindHttp, nodeDefPrev.UUID, mgr.uuid)
			}
			if reflect.DeepEqual(nodeDefPrev, nodeDef) {
				atomic.AddUint64(&mgr.stats.TotSaveNodeDefOk, 1)
				return nil // No changes, so leave the existing nodeDef.
			}
		}

		nodeDefs.UUID = NewUUID()
		nodeDefs.NodeDefs[mgr.bindHttp] = nodeDef
		nodeDefs.ImplVersion = mgr.version // TODO: ImplVersion bump?

		_, err = CfgSetNodeDefs(mgr.cfg, kind, nodeDefs, cas)
		if err != nil {
			if _, ok := err.(*CfgCASError); ok {
				// Retry if it was a CAS mismatch, as perhaps
				// multiple nodes are all racing to register themselves,
				// such as in a full datacenter power restart.
				continue
			}
			atomic.AddUint64(&mgr.stats.TotSaveNodeDefSetErr, 1)
			return err
		}
		break
	}
	atomic.AddUint64(&mgr.stats.TotSaveNodeDefOk, 1)
	return nil
}

// ---------------------------------------------------------------

// SaveNodeDef removes the NodeDef registrations in the Cfg system for
// this Manager node instance.
func (mgr *Manager) RemoveNodeDef(kind string) error {
	if mgr.cfg == nil {
		return nil // Occurs during testing.
	}

	for {
		nodeDefs, cas, err := CfgGetNodeDefs(mgr.cfg, kind)
		if err != nil {
			return err
		}
		if nodeDefs == nil {
			return nil
		}
		nodeDefPrev, exists := nodeDefs.NodeDefs[mgr.bindHttp]
		if !exists || nodeDefPrev == nil {
			return nil
		}
		delete(nodeDefs.NodeDefs, mgr.bindHttp)

		nodeDefs.UUID = NewUUID()
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

// Walk the data dir and register pindexes for a Manager instance.
func (mgr *Manager) LoadDataDir() error {
	log.Printf("manager: loading dataDir...")

	dirEntries, err := ioutil.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("manager: could not read dataDir: %s, err: %v",
			mgr.dataDir, err)
	}

	for _, dirInfo := range dirEntries {
		path := mgr.dataDir + string(os.PathSeparator) + dirInfo.Name()
		_, ok := mgr.ParsePIndexPath(path)
		if !ok {
			continue // Skip the entry that doesn't match the naming pattern.
		}

		log.Printf("manager: opening pindex: %s", path)
		pindex, err := OpenPIndex(mgr, path)
		if err != nil {
			log.Printf("manager: could not open pindex: %s, err: %v",
				path, err)
			continue
		}

		mgr.registerPIndex(pindex)
	}

	log.Printf("manager: loading dataDir... done")
	return nil
}

// ---------------------------------------------------------------

// Schedule kicks of the planner and janitor of a Manager.
func (mgr *Manager) Kick(msg string) {
	atomic.AddUint64(&mgr.stats.TotKick, 1)

	mgr.PlannerKick(msg)
	mgr.JanitorKick(msg)
}

// ---------------------------------------------------------------

// ClosePIndex synchronously has the janitor close a pindex.
func (mgr *Manager) ClosePIndex(pindex *PIndex) error {
	return syncWorkReq(mgr.janitorCh, JANITOR_CLOSE_PINDEX,
		"api-ClosePIndex", pindex)
}

// ClosePIndex synchronously has the janitor remove a pindex.
func (mgr *Manager) RemovePIndex(pindex *PIndex) error {
	return syncWorkReq(mgr.janitorCh, JANITOR_REMOVE_PINDEX,
		"api-RemovePIndex", pindex)
}

// GetPIndex retrieves a named pindex instance.
func (mgr *Manager) GetPIndex(pindexName string) *PIndex {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	return mgr.pindexes[pindexName]
}

func (mgr *Manager) registerPIndex(pindex *PIndex) error {
	mgr.m.Lock()
	defer mgr.m.Unlock()

	if _, exists := mgr.pindexes[pindex.Name]; exists {
		return fmt.Errorf("manager: registered pindex exists, name: %s",
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
		return fmt.Errorf("manager: registered feed already exists, name: %s",
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

// PIndexPath returns the filesystem path for a given named pindex.
// See also ParsePIndexPath().
func (mgr *Manager) PIndexPath(pindexName string) string {
	return PIndexPath(mgr.dataDir, pindexName)
}

// ParsePIndexPath returns the name for a pindex given a filesystem
// path.  See also PIndexPath().
func (mgr *Manager) ParsePIndexPath(pindexPath string) (string, bool) {
	return ParsePIndexPath(mgr.dataDir, pindexPath)
}

// ---------------------------------------------------------------

// Returns the configured Cfg of a Manager.
func (mgr *Manager) Cfg() Cfg {
	return mgr.cfg
}

// Returns the UUID (the "node UUID") of a Manager.
func (mgr *Manager) UUID() string {
	return mgr.uuid
}

// Returns the configured data dir of a Manager.
func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

// --------------------------------------------------------

func (mgr *Manager) addEvent(jsonBytes []byte) {
	mgr.m.Lock()
	for mgr.events.Len() >= MANAGER_MAX_EVENTS {
		mgr.events.Remove(mgr.events.Front())
	}
	mgr.events.PushBack(jsonBytes)
	mgr.m.Unlock()
}

// --------------------------------------------------------

// AtomicCopyTo copies metrics from s to r (from source to result).
func (s *ManagerStats) AtomicCopyTo(r *ManagerStats) {
	rve := reflect.ValueOf(r).Elem()
	sve := reflect.ValueOf(s).Elem()
	svet := sve.Type()
	for i := 0; i < svet.NumField(); i++ {
		rvef := rve.Field(i)
		svef := sve.Field(i)
		if rvef.CanAddr() && svef.CanAddr() {
			rvefp := rvef.Addr().Interface()
			svefp := svef.Addr().Interface()
			atomic.StoreUint64(rvefp.(*uint64),
				atomic.LoadUint64(svefp.(*uint64)))
		}
	}
}

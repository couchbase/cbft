//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

func InitTimeSeriesStatTracker() {
	st = &timeSeriesStatsTracker{
		entries: make(map[string]*statDetails),
	}
}

func TrackStatistic(name string, numEntries int, trackGrowthRate bool) {
	if st != nil {
		st.trackStatistic(name, numEntries, trackGrowthRate)
	}
}

func DetermineNewAverage(name string, val uint64) uint64 {
	if st == nil {
		return val
	}

	return st.determineNewAverage(name, val)
}

// -----------------------------------------------------------------------------

// timeSeriesStatsTracker to be initialized at start of process if
// intended to be utilized.
var st *timeSeriesStatsTracker

type timeSeriesStatsTracker struct {
	m       sync.Mutex
	entries map[string]*statDetails
}

type statDetails struct {
	values []uint64
	size   int

	trackGrowthRate bool
	lastEntry       uint64
	lastRecord      time.Time

	count int
	sum   uint64

	cursor int
}

func (s *timeSeriesStatsTracker) trackStatistic(name string, numEntries int,
	trackGrowthRate bool) {
	s.m.Lock()
	s.entries[name] = &statDetails{
		values:          make([]uint64, numEntries),
		size:            numEntries,
		trackGrowthRate: trackGrowthRate,
		lastRecord:      time.Now(),
	}
	s.m.Unlock()
}

func (s *timeSeriesStatsTracker) determineNewAverage(name string, val uint64) uint64 {
	s.m.Lock()
	defer s.m.Unlock()

	sd, exists := s.entries[name]
	if !exists {
		return val
	}

	sd.sum -= sd.values[sd.cursor]
	newEntry := val
	if sd.trackGrowthRate {
		if newEntry > sd.lastEntry {
			newEntry -= sd.lastEntry
			sd.lastEntry = val
		} else {
			newEntry = 0
		}

		timeNow := time.Now()
		newEntry = uint64(math.Round(float64(newEntry) /
			timeNow.Sub(sd.lastRecord).Seconds()))
		sd.lastRecord = timeNow
	}
	sd.values[sd.cursor] = newEntry
	sd.sum += newEntry

	sd.cursor = (sd.cursor + 1) % sd.size

	if sd.count < sd.size {
		sd.count++
	}

	return sd.sum / uint64(sd.count)
}

type nodeUtilStatsTracker struct {
	mgr          *cbgt.Manager
	idle         atomic.Bool
	lastActivity time.Time

	m             sync.RWMutex
	nodeUtilStats map[string]*NodeUtilStats
}

var utilStatsTracker *nodeUtilStatsTracker
var statsTrackerPeriodicity time.Duration = time.Duration(5 * time.Second)
var statsTrackerLifetime time.Duration = time.Duration(5 * time.Minute)

func (n *nodeUtilStatsTracker) spawnNodeUtilStatsTracker() {
	n.lastActivity = time.Now()
	log.Printf("nodeUtilStatsTracker: spawning a cluster wide utilization stats tracker")
	go utilStatsTracker.run()
}

// isClusterIdle maintains the state which says whether a cluster is idle, ie
// if all the nodes' utilization stats haven't incremented. It helps to decide
// when to exit the stats tracker routine and to when to start a new tracker
// (mainly when the system gets new traffic)
func (n *nodeUtilStatsTracker) isClusterIdle() bool {
	return n.idle.Load()
}

func (n *nodeUtilStatsTracker) getStats(uuid string) (rv *NodeUtilStats) {
	n.m.RLock()
	rv = n.nodeUtilStats[uuid]
	n.m.RUnlock()
	return rv
}

func (n *nodeUtilStatsTracker) run() {
	n.idle.Store(false)
	ticker := time.NewTicker(statsTrackerPeriodicity)
	for {
		var err error
		select {
		case <-ticker.C:
			err = n.gatherStats()
		}

		if time.Now().Sub(n.lastActivity) > statsTrackerLifetime {
			n.idle.Store(true)
			log.Printf("nodeUtilStatsTracker: exiting due to timeout")
			return
		}

		if err != nil {
			log.Errorf("nodeUtilStatsTracker: error while gathering cluster wide "+
				"util stats err: %v", err)
		}
	}
}

func nodeIdle(prevNodeUtilStats, currNodeUtilStats *NodeUtilStats) bool {
	if prevNodeUtilStats == nil {
		return false
	}
	if prevNodeUtilStats.DiskUsage < currNodeUtilStats.DiskUsage ||
		prevNodeUtilStats.MemoryUsage < currNodeUtilStats.MemoryUsage ||
		prevNodeUtilStats.CPUUsage < currNodeUtilStats.CPUUsage {
		return false
	}
	return true
}

func (nh *nodeUtilStatsTracker) gatherStats() error {
	nodeDefs, err := nh.mgr.GetNodeDefs(cbgt.NODE_DEFS_KNOWN, false)
	if err != nil {
		return fmt.Errorf("gatherClusterUtilStats: error getting nodeDefs %v", err)
	}

	nodeUtilStats := make(map[string]*NodeUtilStats)
	systemIdle := true
	for k, node := range nodeDefs.NodeDefs {
		if k == nh.mgr.UUID() {
			continue
		}
		stats, err := obtainNodeUtilStats(node)
		if err != nil {
			return fmt.Errorf("gatherClusterUtilStats: error obtaining node %s "+
				"stats %v", node.UUID, err)
		}
		nh.m.RLock()
		prevNodeUtilStats := nh.nodeUtilStats[k]
		nh.m.RUnlock()
		if systemIdle && !nodeIdle(prevNodeUtilStats, stats) {
			systemIdle = false
		}
		nodeUtilStats[k] = stats
	}

	currNodeStats := make(map[string]interface{})
	gatherNodeUtilStats(nh.mgr, currNodeStats)
	byteStats, err := MarshalJSON(currNodeStats)
	if err != nil {
		return err
	}

	nh.m.RLock()
	prevNodeUtilStats := nh.nodeUtilStats[nh.mgr.UUID()]
	nh.m.RUnlock()

	nodeUtilStats[nh.mgr.UUID()] = &NodeUtilStats{}
	err = UnmarshalJSON(byteStats, nodeUtilStats[nh.mgr.UUID()])
	if err != nil {
		return err
	}

	nh.m.Lock()
	if !systemIdle || !nodeIdle(prevNodeUtilStats, nodeUtilStats[nh.mgr.UUID()]) {
		nh.lastActivity = time.Now()
	}
	nh.nodeUtilStats = nodeUtilStats
	nh.m.Unlock()

	return nil
}

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
	"os"
	"strings"

	log "github.com/couchbaselabs/clog"
)

// A janitor maintains PIndexes and Feeds, creating, deleting &
// hooking them up as necessary to try to match to latest plans.

const JANITOR_CLOSE_PINDEX = "janitor_close_pindex"

func (mgr *Manager) JanitorNOOP(msg string) {
	SyncWorkReq(mgr.janitorCh, WORK_NOOP, msg, nil)
}

func (mgr *Manager) JanitorKick(msg string) {
	SyncWorkReq(mgr.janitorCh, WORK_KICK, msg, nil)
}

func (mgr *Manager) JanitorLoop() {
	if mgr.cfg != nil { // Might be nil for testing.
		go func() {
			ec := make(chan CfgEvent)
			mgr.cfg.Subscribe(PLAN_PINDEXES_KEY, ec)
			mgr.cfg.Subscribe(CfgNodeDefsKey(NODE_DEFS_WANTED), ec)
			for e := range ec {
				mgr.JanitorKick("cfg changed, key: " + e.Key)
			}
		}()
	}

	for m := range mgr.janitorCh {
		var err error
		if m.op == WORK_KICK {
			err = mgr.JanitorOnce(m.msg)
			if err != nil {
				// Keep looping as perhaps it's a transient issue.
				// TODO: perhaps need a rescheduled janitor kick.
				log.Printf("error: JanitorOnce, err: %v", err)
			}
		} else if m.op == WORK_NOOP {
			// NOOP.
		} else if m.op == JANITOR_CLOSE_PINDEX {
			mgr.stopPIndex(m.obj.(*PIndex))
		} else {
			err = fmt.Errorf("error: unknown janitor op: %s, m: %#v", m.op, m)
		}
		if m.resCh != nil {
			if err != nil {
				m.resCh <- err
			}
			close(m.resCh)
		}
	}
}

func (mgr *Manager) JanitorOnce(reason string) error {
	log.Printf("janitor awakes, reason: %s", reason)

	if mgr.cfg == nil { // Can occur during testing.
		return fmt.Errorf("janitor skipped due to nil cfg")
	}

	// NOTE: The janitor doesn't reconfirm that we're a wanted node
	// because instead some planner will see that & update the plan;
	// then relevant janitors will react by closing pindexes & feeds.

	planPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
	if err != nil {
		return fmt.Errorf("janitor skipped on CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexes == nil {
		// Might happen if janitor wins an initialization race.
		return fmt.Errorf("janitor skipped on nil planPIndexes")
	}

	currFeeds, currPIndexes := mgr.CurrentMaps()

	addPlanPIndexes, removePIndexes :=
		CalcPIndexesDelta(mgr.uuid, currPIndexes, planPIndexes)

	log.Printf("janitor pindexes to add: %d", len(addPlanPIndexes))
	for _, ppi := range addPlanPIndexes {
		log.Printf("  %+v", ppi)
	}
	log.Printf("janitor pindexes to remove: %d", len(removePIndexes))
	for _, pi := range removePIndexes {
		log.Printf("  %+v", pi)
	}

	// First, teardown pindexes that need to be removed.
	for _, removePIndex := range removePIndexes {
		log.Printf("janitor removing pindex: %s", removePIndex.Name)
		err = mgr.stopPIndex(removePIndex)
		if err != nil {
			return fmt.Errorf("error: janitor removing pindex: %s, err: %v",
				removePIndex.Name, err)
		}
	}
	// Then, (re-)create pindexes that we're missing.
	for _, addPlanPIndex := range addPlanPIndexes {
		log.Printf("janitor adding pindex: %s", addPlanPIndex.Name)
		err = mgr.startPIndex(addPlanPIndex)
		if err != nil {
			return fmt.Errorf("error: janitor adding pindex: %s, err: %v",
				addPlanPIndex.Name, err)
		}
	}

	currFeeds, currPIndexes = mgr.CurrentMaps()

	addFeeds, removeFeeds :=
		CalcFeedsDelta(mgr.uuid, planPIndexes, currFeeds, currPIndexes)

	log.Printf("janitor feeds to add:")
	for _, targetPIndexes := range addFeeds {
		if len(targetPIndexes) > 0 {
			log.Printf("  %s", FeedName(targetPIndexes[0]))
		}
	}
	log.Printf("janitor feeds to remove:")
	for _, removeFeed := range removeFeeds {
		log.Printf("  %s", removeFeed.Name())
	}

	// First, teardown feeds that need to be removed.
	for _, removeFeed := range removeFeeds {
		err = mgr.stopFeed(removeFeed)
		if err != nil {
			return fmt.Errorf("error: janitor removing feed name: %s, err: %v",
				removeFeed.Name(), err)
		}
	}
	// Then, (re-)create feeds that we're missing.
	for _, addFeedTargetPIndexes := range addFeeds {
		err = mgr.startFeed(addFeedTargetPIndexes)
		if err != nil {
			return fmt.Errorf("error: janitor adding feed, err: %v", err)
		}
	}

	return nil
}

// --------------------------------------------------------

// Functionally determine the delta of which pindexes need creation
// and which should be shut down on our local node (mgrUUID).
func CalcPIndexesDelta(mgrUUID string,
	currPIndexes map[string]*PIndex,
	wantedPlanPIndexes *PlanPIndexes) (
	addPlanPIndexes []*PlanPIndex,
	removePIndexes []*PIndex) {
	// Allocate our return arrays.
	addPlanPIndexes = make([]*PlanPIndex, 0)
	removePIndexes = make([]*PIndex, 0)

	// Just for fast transient lookups.
	mapWantedPlanPIndex := make(map[string]*PlanPIndex)
	mapRemovePIndex := make(map[string]*PIndex)

	// For each wanted plan pindex, if a pindex does not exist or is
	// different, then schedule to add.
	for _, wantedPlanPIndex := range wantedPlanPIndexes.PlanPIndexes {
	nodeUUIDs:
		for nodeUUID, nodeState := range wantedPlanPIndex.NodeUUIDs {
			if nodeUUID != mgrUUID || nodeState == "" {
				continue nodeUUIDs
			}

			mapWantedPlanPIndex[wantedPlanPIndex.Name] = wantedPlanPIndex

			currPIndex, exists := currPIndexes[wantedPlanPIndex.Name]
			if !exists {
				addPlanPIndexes = append(addPlanPIndexes, wantedPlanPIndex)
			} else if PIndexMatchesPlan(currPIndex, wantedPlanPIndex) == false {
				addPlanPIndexes = append(addPlanPIndexes, wantedPlanPIndex)
				removePIndexes = append(removePIndexes, currPIndex)
				mapRemovePIndex[currPIndex.Name] = currPIndex
			}

			break nodeUUIDs
		}
	}

	// For each existing pindex, if not part of wanted plan pindex,
	// then schedule for removal.
	for _, currPIndex := range currPIndexes {
		if _, exists := mapWantedPlanPIndex[currPIndex.Name]; !exists {
			if _, exists = mapRemovePIndex[currPIndex.Name]; !exists {
				removePIndexes = append(removePIndexes, currPIndex)
				mapRemovePIndex[currPIndex.Name] = currPIndex
			}
		}
	}

	return addPlanPIndexes, removePIndexes
}

// --------------------------------------------------------

// Functionally determine the delta of which feeds need creation and
// which should be shut down.
func CalcFeedsDelta(nodeUUID string, planPIndexes *PlanPIndexes,
	currFeeds map[string]Feed, pindexes map[string]*PIndex) (
	addFeeds [][]*PIndex, removeFeeds []Feed) {
	// Allocate result holders.
	addFeeds = make([][]*PIndex, 0)
	removeFeeds = make([]Feed, 0)

	// Group the writable pindexes by their feed names.  Non-writable
	// pindexes (such as paused) will have their feeds removed.
	groupedPIndexes := make(map[string][]*PIndex)
	for _, pindex := range pindexes {
		planPIndex, exists := planPIndexes.PlanPIndexes[pindex.Name]
		if exists && planPIndex != nil &&
			strings.Contains(planPIndex.NodeUUIDs[nodeUUID], PLAN_PINDEX_NODE_WRITE) {
			feedName := FeedName(pindex)
			arr, exists := groupedPIndexes[feedName]
			if !exists {
				arr = make([]*PIndex, 0)
			}
			groupedPIndexes[feedName] = append(arr, pindex)
		}
	}

	for feedName, feedPIndexes := range groupedPIndexes {
		if _, exists := currFeeds[feedName]; !exists {
			addFeeds = append(addFeeds, feedPIndexes)
		}
	}

	for currFeedName, currFeed := range currFeeds {
		if _, exists := groupedPIndexes[currFeedName]; !exists {
			removeFeeds = append(removeFeeds, currFeed)
		}
	}

	return addFeeds, removeFeeds
}

func FeedName(pindex *PIndex) string {
	// NOTE: We're depending on the IndexName/IndexUUID to "cover" the
	// SourceType, SourceName, SourceUUID, SourceParams values, so we
	// don't need to encode those source parts into the feed name.
	return pindex.IndexName + "_" + pindex.IndexUUID
}

// --------------------------------------------------------

func (mgr *Manager) startPIndex(planPIndex *PlanPIndex) error {
	var pindex *PIndex
	var err error

	path := mgr.PIndexPath(planPIndex.Name)

	// First, try reading the path with OpenPIndex().  An
	// existing path might happen during a case of rollback.
	if _, err = os.Stat(path); err == nil {
		pindex, err = OpenPIndex(mgr, path)
		if err == nil {
			if !PIndexMatchesPlan(pindex, planPIndex) {
				fmt.Printf("pindex does not match plan,"+
					" cleaning up and trying NewPIndex, path: %s, err: %v",
					path, err)
				close(pindex.Stream)
				pindex = nil
				os.RemoveAll(path)
			}
		} else {
			fmt.Printf("OpenPIndex error,"+
				" cleaning up and trying NewPIndex, path: %s, err: %v",
				path, err)
			os.RemoveAll(path)
		}
	}

	if pindex == nil {
		pindex, err = NewPIndex(mgr, planPIndex.Name, NewUUID(),
			planPIndex.IndexType,
			planPIndex.IndexName,
			planPIndex.IndexUUID,
			planPIndex.IndexSchema,
			planPIndex.SourceType,
			planPIndex.SourceName,
			planPIndex.SourceUUID,
			planPIndex.SourceParams,
			planPIndex.SourcePartitions,
			path)
		if err != nil {
			return fmt.Errorf("error: NewPIndex, name: %s, err: %v",
				planPIndex.Name, err)
		}
	}

	if err = mgr.registerPIndex(pindex); err != nil {
		close(pindex.Stream)
		return err
	}

	return nil
}

func (mgr *Manager) stopPIndex(pindex *PIndex) error {
	// First, stop any feeds that might be sending to the pindex's stream.
	feeds, _ := mgr.CurrentMaps()
	for _, feed := range feeds {
		for _, stream := range feed.Streams() {
			if stream == pindex.Stream {
				if err := mgr.stopFeed(feed); err != nil {
					panic(fmt.Sprintf("error: could not stop feed, err: %v", err))
				}
			}
		}
	}

	pindexUnreg := mgr.unregisterPIndex(pindex.Name)
	if pindexUnreg != nil && pindexUnreg != pindex {
		panic("unregistered pindex isn't the one we're stopping")
	}

	close(pindex.Stream)

	return nil
}

// --------------------------------------------------------

func (mgr *Manager) startFeed(pindexes []*PIndex) error {
	if len(pindexes) <= 0 {
		return nil
	}

	pindexFirst := pindexes[0]
	feedName := FeedName(pindexFirst)

	streams := make(map[string]Stream)
	for _, pindex := range pindexes {
		if f := FeedName(pindex); f != feedName {
			panic(fmt.Sprintf("error: unexpected feedName: %s != %s", f, feedName))
		}

		addSourcePartition := func(sourcePartition string) {
			if _, exists := streams[pindex.SourcePartitions]; exists {
				panic(fmt.Sprintf("error: startFeed saw sourcePartition collision: %s",
					sourcePartition))
			}
			streams[sourcePartition] = pindex.Stream
		}

		if pindex.SourcePartitions == "" {
			addSourcePartition("")
		} else {
			sourcePartitionsArr := strings.Split(pindex.SourcePartitions, ",")
			for _, sourcePartition := range sourcePartitionsArr {
				addSourcePartition(sourcePartition)
			}
		}
	}

	return mgr.startFeedByType(feedName,
		pindexFirst.IndexName, pindexFirst.IndexUUID,
		pindexFirst.SourceType, pindexFirst.SourceName, pindexFirst.SourceUUID,
		streams)
}

func (mgr *Manager) stopFeed(feed Feed) error {
	feedUnreg := mgr.unregisterFeed(feed.Name())
	if feedUnreg != nil && feedUnreg != feed {
		panic("error: unregistered feed isn't the one we're closing")
	}

	// NOTE: We're depending on feed to synchronously close, so we
	// know it'll no longer be sending to any of its streams anymore.
	return feed.Close()
}

// --------------------------------------------------------

// TODO: need way to track dead cows (non-beef)
// TODO: need a way to collect these errors so REST api
// can show them to user ("hey, perhaps you deleted a bucket
// and should delete these related full-text indexes?
// or the couchbase cluster is just down.");
// perhaps as specialized clog writer?

func (mgr *Manager) startFeedByType(feedName, indexName, indexUUID,
	sourceType, sourceName, sourceUUID string,
	streams map[string]Stream) error {
	if sourceType == "couchbase" {
		// TODO: Should default to DCP feed or projector feed one day.
		return mgr.startTAPFeed(feedName, indexName, indexUUID,
			sourceName, sourceUUID, streams)
	}

	if sourceType == "simple" {
		return mgr.startSimpleFeed(feedName, streams)
	}

	if sourceType == "nil" {
		return mgr.registerFeed(NewNILFeed(feedName, streams))
	}

	return fmt.Errorf("error: startFeed() got unknown source type: %s", sourceType)
}

func (mgr *Manager) startTAPFeed(feedName, indexName, indexUUID,
	bucketName, bucketUUID string, streams map[string]Stream) error {
	feed, err := NewTAPFeed(feedName, mgr.server, "default",
		bucketName, bucketUUID, BasicPartitionFunc, streams)
	if err != nil {
		return fmt.Errorf("error: could not prepare TAP stream to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	if err = feed.Start(); err != nil {
		return fmt.Errorf("error: could not start tap feed, server: %s, err: %v",
			mgr.server, err)
	}
	if err = mgr.registerFeed(feed); err != nil {
		feed.Close()
		return err
	}
	return nil
}

func (mgr *Manager) startSimpleFeed(feedName string,
	streams map[string]Stream) error {
	feed, err := NewSimpleFeed(feedName, make(Stream), BasicPartitionFunc, streams)
	if err != nil {
		return err
	}
	if err = feed.Start(); err != nil {
		return fmt.Errorf("error: could not start simple feed, server: %s, err: %v",
			mgr.server, err)
	}
	if err = mgr.registerFeed(feed); err != nil {
		feed.Close()
		return err
	}

	return nil
}

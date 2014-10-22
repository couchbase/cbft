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

	log "github.com/couchbaselabs/clog"
)

// A janitor maintains PIndexes and Feeds,,, creating, deleting, and
// hooking them up as necessary to try to match to latest plans.

func (mgr *Manager) JanitorKick(msg string) {
	resCh := make(chan error)
	mgr.janitorCh <- &WorkReq{msg: msg, resCh: resCh}
	<-resCh
}

func (mgr *Manager) JanitorLoop() {
	for m := range mgr.janitorCh {
		err := mgr.JanitorOnce(m.msg)
		if err != nil {
			log.Printf("error: JanitorOnce, err: %v", err)
			// Keep looping as perhaps it's a transient issue.
			// TODO: Need a better way to track warnings from errors,
			// and which need a future, rescheduled janitor kick.
		}
		if m.resCh != nil {
			close(m.resCh)
		}
	}
}

func (mgr *Manager) JanitorOnce(reason string) error {
	log.Printf("janitor awakes, reason: %s", reason)

	if mgr.cfg == nil { // Can occur during testing.
		return fmt.Errorf("janitor skipped due to nil cfg")
	}

	planPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
	if err != nil {
		return fmt.Errorf("janitor skipped on CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexes == nil {
		// Might happen if janitor wins a race.
		// TODO: Should we reschedule another future janitor kick?
		return fmt.Errorf("janitor skipped on nil planPIndexes")
	}

	currFeeds, currPIndexes := mgr.CurrentMaps()

	addPlanPIndexes, removePIndexes :=
		CalcPIndexesDelta(mgr.uuid, currPIndexes, planPIndexes)

	log.Printf("janitor pindexes to add:")
	for _, ppi := range addPlanPIndexes {
		log.Printf("  %+v", ppi)
	}
	log.Printf("janitor pindexes to remove:")
	for _, pi := range removePIndexes {
		log.Printf("  %+v", pi)
	}

	// First, teardown pindexes that need to be removed.
	for _, removePIndex := range removePIndexes {
		log.Printf("janitor removing pindex: %s", removePIndex.Name)
		err = mgr.StopPIndex(removePIndex)
		if err != nil {
			return fmt.Errorf("error: janitor removing pindex: %s, err: %v",
				removePIndex.Name, err)
		}
	}
	// Then, (re-)create pindexes that we're missing.
	for _, addPlanPIndex := range addPlanPIndexes {
		log.Printf("janitor adding pindex: %s", addPlanPIndex.Name)
		err = mgr.StartPIndex(addPlanPIndex)
		if err != nil {
			return fmt.Errorf("error: janitor adding pindex: %s, err: %v",
				addPlanPIndex.Name, err)
		}
	}

	currFeeds, currPIndexes = mgr.CurrentMaps()

	addFeeds, removeFeeds := CalcFeedsDelta(currFeeds, currPIndexes)

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
		err = mgr.StopFeed(removeFeed)
		if err != nil {
			return fmt.Errorf("error: janitor removing feed name: %s, err: %v",
				removeFeed.Name, err)
		}
	}
	// Then, (re-)create feeds that we're missing.
	for addFeedName, targetPIndexes := range addFeeds {
		err = mgr.StartFeed(targetPIndexes)
		if err != nil {
			return fmt.Errorf("error: janitor adding feed, addFeedName: %s, err: %v",
				addFeedName, err)
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
			if nodeUUID != mgrUUID || nodeState != "active" {
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

func PIndexMatchesPlan(pindex *PIndex, planPIndex *PlanPIndex) bool {
	same := pindex.Name == planPIndex.Name &&
		pindex.IndexName == planPIndex.IndexName &&
		pindex.IndexUUID == planPIndex.IndexUUID &&
		pindex.IndexMapping == planPIndex.IndexMapping &&
		pindex.SourceType == planPIndex.SourceType &&
		pindex.SourceName == planPIndex.SourceName &&
		pindex.SourceUUID == planPIndex.SourceUUID &&
		pindex.SourcePartitions == planPIndex.SourcePartitions
	if !same {
		log.Printf("PIndexMatchesPlan false, pindex: %#v, planPIndex: %#v",
			pindex, planPIndex)
	}
	return same
}

// --------------------------------------------------------

// Functionally determine the delta of which feeds need creation and
// which should be shut down.
func CalcFeedsDelta(currFeeds map[string]Feed, pindexes map[string]*PIndex) (
	addFeeds [][]*PIndex, removeFeeds []Feed) {
	// Allocate result holders.
	addFeeds = make([][]*PIndex, 0)
	removeFeeds = make([]Feed, 0)

	// Group the pindexes by their feed names.
	groupedPIndexes := make(map[string][]*PIndex)
	for _, pindex := range pindexes {
		feedName := FeedName(pindex)
		arr, exists := groupedPIndexes[feedName]
		if !exists {
			arr = make([]*PIndex, 0)
		}
		groupedPIndexes[feedName] = append(arr, pindex)
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
	// TODO: Different feed types might have different name formulas
	// depending on whether they have a "single cluster" abstraction
	// or work on a "node by node" basis (in where stream destinations
	// need to be part of the name encoding).
	//
	// NOTE: We're depending on the IndexName/IndexUUID to
	// functionally "cover" the SourceType/SourceName/SourceUUID, so
	// we don't need to encode the source parts into the feed name.
	return pindex.IndexName + "_" + pindex.IndexUUID
}

// --------------------------------------------------------

func (mgr *Manager) StartPIndex(planPIndex *PlanPIndex) error {
	pindex, err := NewPIndex(planPIndex.Name, NewUUID(),
		planPIndex.IndexName,
		planPIndex.IndexUUID,
		planPIndex.IndexMapping,
		planPIndex.SourceType,
		planPIndex.SourceName,
		planPIndex.SourceUUID,
		planPIndex.SourcePartitions,
		mgr.PIndexPath(planPIndex.Name))
	if err != nil {
		return fmt.Errorf("error: NewPIndex, name: %s, err: %v",
			planPIndex.Name, err)
	}

	if err = mgr.RegisterPIndex(pindex); err != nil {
		close(pindex.Stream)
		return err
	}

	return nil
}

func (mgr *Manager) StopPIndex(pindex *PIndex) error {
	feeds, _ := mgr.CurrentMaps()
	for _, feed := range feeds {
		for _, stream := range feed.Streams() {
			if stream == pindex.Stream {
				if err := mgr.StopFeed(feed); err != nil {
					panic(fmt.Sprintf("error: could not stop feed, err: %v", err))
				}
			}
		}
	}

	pindexUnreg := mgr.UnregisterPIndex(pindex.Name)
	if pindexUnreg != nil && pindexUnreg != pindex {
		panic("unregistered pindex isn't the one we're stopping")
	}

	close(pindex.Stream)

	return nil
}

// --------------------------------------------------------

func (mgr *Manager) StartFeed(pindexes []*PIndex) error {
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

		streams[pindex.SourcePartitions] = pindex.Stream
	}

	// TODO: Make this more extensible one day for more SourceType possibilities.
	if pindexFirst.SourceType == "couchbase" {
		// TODO: Should default to DCP feed or projector feed one day.
		return mgr.StartTAPFeed(feedName,
			pindexFirst.IndexName, pindexFirst.IndexUUID,
			pindexFirst.SourceName, pindexFirst.SourceUUID, streams)
	}

	return fmt.Errorf("error: StartFeed() got unknown source type: %s",
		pindexFirst.SourceType)
}

func (mgr *Manager) StopFeed(feed Feed) error {
	feedUnreg := mgr.UnregisterFeed(feed.Name())
	if feedUnreg != nil && feedUnreg != feed {
		panic("error: unregistered feed isn't the one we're closing")
	}

	// NOTE: We're depending on feed to synchronously close, so we
	// know it'll no longer be sending to any of its streams anymore.
	//
	return feed.Close()
}

// --------------------------------------------------------

func (mgr *Manager) StartTAPFeed(feedName, indexName, indexUUID,
	bucketName, bucketUUID string, streams map[string]Stream) error {
	feed, err := NewTAPFeed(feedName, mgr.server, "default",
		bucketName, bucketUUID, streams)
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

	if err = mgr.RegisterFeed(feed); err != nil {
		// TODO: cleanup?
		return err
	}

	return nil
}

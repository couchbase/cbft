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

// A janitor maintains feeds, creating and deleting as necessary.

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
	log.Printf("janitor feeds add: %+v, remove: %+v",
		addFeeds, removeFeeds)

	// First, teardown feeds that need to be removed.
	for _, removeFeed := range removeFeeds {
		err = mgr.StopFeed(removeFeed)
		if err != nil {
			return fmt.Errorf("error: janitor removing feed: %s, err: %v",
				removeFeed.Name, err)
		}
	}
	// Then, (re-)create feeds that we're missing.
	for _, targePIndexes := range addFeeds {
		err = mgr.StartFeed(targePIndexes)
		if err != nil {
			return fmt.Errorf("error: janitor adding feed, err: %v", err)
		}
	}

	return nil
}

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
			if nodeUUID == mgrUUID && nodeState == "active" {
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

// Functionally determine the delta of which feeds need creation and
// which should be shut down.
func CalcFeedsDelta(currFeeds map[string]Feed, pindexes map[string]*PIndex) (
	addFeeds [][]*PIndex, removeFeeds []Feed) {
	addFeeds = make([][]*PIndex, 0)
	removeFeeds = make([]Feed, 0)

	for _, pindex := range pindexes {
		addFeedName := FeedName("default", pindex.SourceName, "")
		if _, ok := currFeeds[addFeedName]; !ok {
			addFeeds = append(addFeeds, []*PIndex{pindex})

			// TODO: this doesn't seem to do delta calc right.
		}
	}

	return addFeeds, removeFeeds
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
				feedUnreg := mgr.UnregisterFeed(feed.Name())
				if feedUnreg != feed {
					panic("error: unregistered feed isn't the one we're closing")
				}

				// NOTE: We're depending on feed to synchronously
				// close, where we know it will no longer be writing
				// any pindex streams anymore.
				if err := feed.Close(); err != nil {
					panic(fmt.Sprintf("error: could not close feed, err: %v", err))
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
	// TODO: Need to create a fan-out feed.
	for _, pindex := range pindexes {
		// TODO: Need bucket UUID.
		err := mgr.StartTAPFeed(pindex) // TODO: err handling.
		if err != nil {
			return err
		}
	}
	return nil
}

func (mgr *Manager) StopFeed(feed Feed) error {
	// TODO.
	return nil
}

// --------------------------------------------------------

func (mgr *Manager) StartTAPFeed(pindex *PIndex) error {
	// TODO: bad assumption of 1-to-1 pindex.name to indexName
	indexName := pindex.IndexName

	// TODO: do more with SourceType, SourceName, SourceUUID.
	bucketName := pindex.SourceName
	bucketUUID := pindex.SourceUUID

	// TODO: utiilzed SourcePartitions
	streams := map[string]Stream{
		"": pindex.Stream,
	}
	feed, err := NewTAPFeed(mgr.server, "default", bucketName, bucketUUID, streams)
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

// --------------------------------------------------------

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

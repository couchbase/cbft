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
func (mgr *Manager) JanitorLoop() {
	for reason := range mgr.janitorCh {
		log.Printf("janitor awakes, reason: %s", reason)

		if mgr.cfg == nil { // Can occur during testing.
			log.Printf("janitor skipped due to nil cfg")
			continue
		}

		planPIndexes, _, err := CfgGetPlanPIndexes(mgr.cfg)
		if err != nil {
			log.Printf("janitor skipped due to CfgGetPlanPIndexes err: %v", err)
			continue
		}
		if planPIndexes == nil {
			log.Printf("janitor skipped due to nil planPIndexes")
			continue
		}

		currFeeds, currPIndexes := mgr.CurrentMaps()

		addPlanPIndexes, removePIndexes :=
			CalcPIndexesDelta(mgr.uuid, currPIndexes, planPIndexes)
		log.Printf("janitor pindexes add: %v, remove: %v",
			addPlanPIndexes, removePIndexes)

		// Create pindexes that we're missing.
		for _, addPlanPIndex := range addPlanPIndexes {
			mgr.StartPIndex(addPlanPIndex)
		}

		// Teardown pindexes that need to be removed.
		for _, removePIndex := range removePIndexes {
			mgr.StopPIndex(removePIndex)
		}

		currFeeds, currPIndexes = mgr.CurrentMaps()

		addFeeds, removeFeeds :=
			CalcFeedsDelta(currFeeds, currPIndexes)
		log.Printf("janitor feeds add: %v, remove: %v",
			addFeeds, removeFeeds)

		// Create feeds that we're missing.
		for _, targetPindexes := range addFeeds {
			mgr.StartFeed(targetPindexes)
		}

		// Teardown feeds that need to be removed.
		for _, removeFeed := range removeFeeds {
			mgr.StopFeed(removeFeed)
		}
	}
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
		for _, nodeUUID := range wantedPlanPIndex.NodeUUIDs {
			if nodeUUID == mgrUUID {
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
		}
	}

	return addFeeds, removeFeeds
}

// --------------------------------------------------------

func (mgr *Manager) StartPIndex(planPIndex *PlanPIndex) error {
	// TODO.
	return nil
}

func (mgr *Manager) StopPIndex(pindex *PIndex) error {
	// TODO.
	return nil
}

// --------------------------------------------------------

func (mgr *Manager) StartFeed(pindexes []*PIndex) error {
	// TODO: Need to create a fan-out feed.
	for _, pindex := range pindexes {
		// TODO: Need bucket UUID.
		err := mgr.StartSimpleFeed(pindex) // TODO: err handling.
		if err != nil {
			log.Printf("error: could not start feed for pindex: %s, err: %v",
				pindex.Name, err)
		}
	}
	return nil
}

func (mgr *Manager) StopFeed(feed Feed) error {
	// TODO.
	return nil
}

// --------------------------------------------------------

func (mgr *Manager) StartSimpleFeed(pindex *PIndex) error {
	indexName := pindex.Name // TODO: bad assumption of 1-to-1 pindex.name to indexName

	bucketName := indexName // TODO: read bucketName out of bleve storage.
	bucketUUID := ""        // TODO: read bucketUUID & vbucket list from bleve storage.
	feed, err := NewTAPFeed(mgr.server, "default", bucketName, bucketUUID,
		pindex.Stream)
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
	same :=
		pindex.Name == planPIndex.Name &&
			pindex.UUID == planPIndex.UUID &&
			pindex.IndexName == planPIndex.IndexName &&
			pindex.IndexUUID == planPIndex.IndexUUID &&
			pindex.IndexMapping == planPIndex.IndexMapping &&
			pindex.SourceType == planPIndex.SourceType &&
			pindex.SourceName == planPIndex.SourceName &&
			pindex.SourceUUID == planPIndex.SourceUUID &&
			pindex.SourcePartitions == planPIndex.SourcePartitions
	return same
}

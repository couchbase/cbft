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

const PLANNER_VERSION = "0.0.0" // Follow semver rules.

type Plan struct {
}

type LogicalIndex struct {
}

type LogicalIndexes []*LogicalIndex

type Indexer struct {
}

type Indexers []*Indexer

// A planner assigns partitions to cbft's and to PIndexes on each cbft.
func (mgr *Manager) PlannerLoop() {
	for _ = range mgr.plannerCh {
		if !mgr.CheckPlannerVersion() {
			continue
		}
		plan, err := mgr.CalcPlan(nil, nil)
		if err != nil {
			log.Printf("error: CalcPlan, err: %v", err)
		}
		if plan != nil {
			// TODO: save the plan.
		}
	}
}

func (mgr *Manager) CalcPlan(logicalIndexes LogicalIndexes,
	indexers Indexers) (*Plan, error) {
	// TODO: implement the grand plans for the planner.
	// First gen planner should keep it simple, such as...
	// - a single Feed for every datasource node.
	// - a Feed might "fan out" to multiple Streams/PIndexes.
	// - have a single PIndex for all datasource partitions
	//   (vbuckets) to start.
	return nil, fmt.Errorf("TODO")
}

func (mgr *Manager) CheckPlannerVersion() bool {
	for mgr.cfg != nil {
		version, cas, err := mgr.cfg.Get("plannerVersion", 0)
		if version == nil || version == "" || err != nil {
			version = PLANNER_VERSION
		}
		if !VersionGTE(PLANNER_VERSION, version.(string)) {
			log.Printf("planning skipped for obsoleted version: %v < %v",
				PLANNER_VERSION, version)
			return false
		}
		if PLANNER_VERSION == version {
			return true
		}
		// We have a higher PLANNER_VERSION than the read
		// version, so save PLANNER_VERSION and retry.
		mgr.cfg.Set("plannerVersion", PLANNER_VERSION, cas)
	}

	return false // Never reached.
}

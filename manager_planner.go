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

// A planner assigns partitions to cbft's and to PIndexes on each cbft.
// NOTE: You *must* update PLANNER_VERSION if these planning algorithm
// or schema changes, following semver rules.

func (mgr *Manager) PlannerLoop() {
	for reason := range mgr.plannerCh {
		log.Printf("planner awakes, reason: %s", reason)

		if mgr.cfg == nil { // Can occur during testing.
			log.Printf("planner skipped due to nil cfg")
			continue
		}
		ok, err := CheckVersion(mgr.cfg, mgr.version)
		if err != nil {
			log.Printf("planner skipped due to CheckVersion err: %v", err)
			continue
		}
		if !ok {
			log.Printf("planner skipped because version is too low: %v",
				mgr.version)
			continue
		}

		// TODO: What about downgrades?

		indexDefs, _, err := CfgGetIndexDefs(mgr.cfg)
		if err != nil {
			log.Printf("planner skipped due to CfgGetIndexDefs err: %v", err)
			continue
		}
		if indexDefs == nil {
			log.Printf("planner ended since no IndexDefs")
			continue
		}
		if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
			log.Printf("planner ended since indexDefs.ImplVersion: %s"+
				"> mgr.version: %s", indexDefs.ImplVersion, mgr.version)
			continue
		}

		nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg)
		if err != nil {
			log.Printf("planner skipped due to CfgGetNodeDefs err: %v", err)
			continue
		}
		if nodeDefs == nil {
			log.Printf("planner ended since no NodeDefs")
			continue
		}
		if VersionGTE(mgr.version, nodeDefs.ImplVersion) == false {
			log.Printf("planner ended since nodeDefs.ImplVersion: %s"+
				"> mgr.version: %s", indexDefs.ImplVersion, mgr.version)
			continue
		}

		plan, err := mgr.CalcPlan(indexDefs, nodeDefs)
		if err != nil {
			log.Printf("error: CalcPlan, err: %v", err)
		}
		if plan != nil {
			// TODO: save the plan.
			// TODO: kick the janitor if the plan changed.
		}
	}
}

func (mgr *Manager) CalcPlan(indexDefs *IndexDefs, nodeDefs *NodeDefs) (
	*Plan, error) {
	// TODO: implement the grand plans for the planner.
	// First gen planner should keep it simple, such as...
	// - a single Feed for every datasource node.
	// - a Feed might "fan out" to multiple Streams/PIndexes.
	// - have a single PIndex for all datasource partitions
	//   (vbuckets) to start.
	return nil, fmt.Errorf("TODO")
}

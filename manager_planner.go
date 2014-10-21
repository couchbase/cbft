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
	log "github.com/couchbaselabs/clog"
)

// A planner assigns partitions to cbft's and to PIndexes on each cbft.
// NOTE: You *must* update PLANNER_VERSION if these planning algorithm
// or schema changes, following semver rules.

func (mgr *Manager) PlannerLoop() {
	for m := range mgr.plannerCh {
		mgr.PlannerOnce(m.msg)
		if m.resCh != nil {
			close(m.resCh)
		}
	}

	close(mgr.janitorCh)
}

func (mgr *Manager) PlannerOnce(reason string) bool {
	log.Printf("planner awakes, reason: %s", reason)

	if mgr.cfg == nil { // Can occur during testing.
		log.Printf("planner skipped due to nil cfg")
		return false
	}
	ok, err := CheckVersion(mgr.cfg, mgr.version)
	if err != nil {
		log.Printf("planner skipped due to CheckVersion err: %v", err)
		return false
	}
	if !ok {
		log.Printf("planner skipped because version is too low: %v",
			mgr.version)
		return false
	}

	// TODO: What about downgrades?

	indexDefs, _, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		log.Printf("planner skipped due to CfgGetIndexDefs err: %v", err)
		return false
	}
	if indexDefs == nil {
		log.Printf("planner ended since no IndexDefs")
		return false
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		log.Printf("planner ended since indexDefs.ImplVersion: %s"+
			" > mgr.version: %s", indexDefs.ImplVersion, mgr.version)
		return false
	}

	nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg, NODE_DEFS_WANTED)
	if err != nil {
		log.Printf("planner skipped due to CfgGetNodeDefs err: %v", err)
		return false
	}
	if nodeDefs == nil {
		log.Printf("planner ended since no NodeDefs")
		return false
	}
	if VersionGTE(mgr.version, nodeDefs.ImplVersion) == false {
		log.Printf("planner ended since nodeDefs.ImplVersion: %s"+
			" > mgr.version: %s", nodeDefs.ImplVersion, mgr.version)
		return false
	}

	planPIndexesPrev, cas, err := CfgGetPlanPIndexes(mgr.cfg)
	if err != nil {
		log.Printf("planner skipped due to CfgGetPlanPIndexes err: %v", err)
		return false
	}
	if planPIndexesPrev == nil {
		planPIndexesPrev = NewPlanPIndexes(mgr.version)
	}
	if VersionGTE(mgr.version, planPIndexesPrev.ImplVersion) == false {
		log.Printf("planner ended since planPIndexesPrev.ImplVersion: %s"+
			" > mgr.version: %s", planPIndexesPrev.ImplVersion, mgr.version)
		return false
	}

	planPIndexes, err := CalcPlan(indexDefs, nodeDefs, planPIndexesPrev, mgr.version)
	if err != nil {
		log.Printf("error: CalcPlan, err: %v", err)
	}
	if planPIndexes == nil {
		log.Printf("planner found no plans from CalcPlan()")
		return false
	}
	if SamePlanPIndexes(planPIndexes, planPIndexesPrev) {
		log.Printf("planner found no changes")
		return false
	}
	_, err = CfgSetPlanPIndexes(mgr.cfg, planPIndexes, cas)
	if err != nil {
		log.Printf("planner could not save new plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
		return false
	}

	// TODO: need some distributed notify/event facility,
	// perhaps in the Cfg, to kick any remote janitors.
	//
	resCh := make(chan error)
	mgr.janitorCh <- &WorkReq{
		msg:   "the plans have changed",
		resCh: resCh,
	}
	<-resCh

	return true
}

// Split logical indexes into Pindexes and assign PIndexes to nodes.
func CalcPlan(indexDefs *IndexDefs, nodeDefs *NodeDefs, planPIndexesPrev *PlanPIndexes,
	version string) (
	*PlanPIndexes, error) {
	// First planner attempt here is naive & simple, where every
	// single Index is "split" into just a single PIndex (so all the
	// datasource partitions (vbuckets) will feed into that single
	// PIndex).  And, every single node has a replica of that PIndex.
	// This takes care of the "single node" requirement of a developer
	// preview.
	//
	// TODO: Implement more advanced planners another day,
	// - multiple PIndexes per Index.
	// - different fan-out/fan-in topologies from datasource to
	//   Feed to PIndex.
	//
	if indexDefs == nil || nodeDefs == nil {
		return nil, nil
	}

	planPIndexes := NewPlanPIndexes(version)

	for _, indexDef := range indexDefs.IndexDefs {
		planPIndex := &PlanPIndex{
			// This simple PlanPIndex.Name here only works for simple
			// 1-to-1 case.  NOTE: PlanPIndex.Name must be unique
			// across the cluster and functionally based off of the
			// indexDef.
			//
			// TODO: More advanced planners will probably have to
			// incorporate SourcePartitions info into the
			// PlanPIndex.Name.
			Name:             indexDef.Name + "_" + indexDef.UUID,
			UUID:             NewUUID(),
			IndexName:        indexDef.Name,
			IndexUUID:        indexDef.UUID,
			IndexMapping:     indexDef.Mapping,
			SourceType:       indexDef.SourceType,
			SourceName:       indexDef.SourceName,
			SourceUUID:       indexDef.SourceUUID,
			SourcePartitions: "", // Simple version is get all partitions.
			NodeUUIDs:        make(map[string]string),
		}
		for _, nodeDef := range nodeDefs.NodeDefs {
			// TODO: better val needed.
			planPIndex.NodeUUIDs[nodeDef.UUID] = "active"
		}
		planPIndexes.PlanPIndexes[planPIndex.Name] = planPIndex
	}

	return planPIndexes, nil
}

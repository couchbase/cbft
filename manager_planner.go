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
// NOTE: You *must* update PLANNER_VERSION if the planning algorithm
// or schema changes, following semver rules.

func (mgr *Manager) PlannerNOOP(msg string) {
	SyncWorkReq(mgr.plannerCh, WORK_NOOP, msg)
}

func (mgr *Manager) PlannerKick(msg string) {
	SyncWorkReq(mgr.plannerCh, WORK_KICK, msg)
}

func (mgr *Manager) PlannerLoop() {
	for m := range mgr.plannerCh {
		if m.op == WORK_KICK {
			changed, err := mgr.PlannerOnce(m.msg)
			if err != nil {
				log.Printf("error: PlannerOnce, err: %v", err)
				// Keep looping as perhaps it's a transient issue.
			} else if changed {
				// TODO: need some distributed notify/event facility,
				// perhaps in the Cfg, to kick any remote janitors.
				//
				mgr.JanitorKick("the plans have changed")
			}
		} else if m.op == WORK_NOOP {
			// NOOP.
		} else {
			log.Printf("error: unknown planner op: %s, m: %#v", m.op, m)
		}
		if m.resCh != nil {
			close(m.resCh)
		}
	}

	close(mgr.janitorCh)
}

func (mgr *Manager) PlannerOnce(reason string) (bool, error) {
	log.Printf("planner awakes, reason: %s", reason)

	if mgr.cfg == nil { // Can occur during testing.
		return false, fmt.Errorf("planner skipped due to nil cfg")
	}

	ok, err := CheckVersion(mgr.cfg, mgr.version)
	if err != nil {
		return false, fmt.Errorf("planner skipped on CheckVersion err: %v", err)
	}
	if !ok {
		return false, fmt.Errorf("planner skipped with version too low: %v",
			mgr.version)
	}

	// TODO: What about downgrades?

	indexDefs, _, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return false, fmt.Errorf("planner skipped on CfgGetIndexDefs err: %v", err)
	}
	if indexDefs == nil {
		return false, fmt.Errorf("planner ended since no IndexDefs")
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return false, fmt.Errorf("planner ended since indexDefs.ImplVersion: %s"+
			" > mgr.version: %s", indexDefs.ImplVersion, mgr.version)
	}

	nodeDefs, _, err := CfgGetNodeDefs(mgr.cfg, NODE_DEFS_WANTED)
	if err != nil {
		return false, fmt.Errorf("planner skipped on CfgGetNodeDefs err: %v", err)
	}
	if nodeDefs == nil {
		return false, fmt.Errorf("planner ended since no NodeDefs")
	}
	if VersionGTE(mgr.version, nodeDefs.ImplVersion) == false {
		return false, fmt.Errorf("planner ended since nodeDefs.ImplVersion: %s"+
			" > mgr.version: %s", nodeDefs.ImplVersion, mgr.version)
	}

	planPIndexesPrev, cas, err := CfgGetPlanPIndexes(mgr.cfg)
	if err != nil {
		return false, fmt.Errorf("planner skipped on CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexesPrev == nil {
		planPIndexesPrev = NewPlanPIndexes(mgr.version)
	}
	if VersionGTE(mgr.version, planPIndexesPrev.ImplVersion) == false {
		return false, fmt.Errorf("planner ended on planPIndexesPrev.ImplVersion: %s"+
			" > mgr.version: %s", planPIndexesPrev.ImplVersion, mgr.version)
	}

	planPIndexes, err := CalcPlan(indexDefs, nodeDefs, planPIndexesPrev, mgr.version)
	if err != nil {
		return false, fmt.Errorf("planner ended on CalcPlan, err: %v", err)
	}
	if planPIndexes == nil {
		return false, fmt.Errorf("planner found no plans from CalcPlan")
	}
	if SamePlanPIndexes(planPIndexes, planPIndexesPrev) {
		return false, nil
	}
	_, err = CfgSetPlanPIndexes(mgr.cfg, planPIndexes, cas)
	if err != nil {
		return false, fmt.Errorf("planner could not save new plan,"+
			" perhaps a concurrent planner won, cas: %d, err: %v",
			cas, err)
	}

	return true, nil
}

// Split logical indexes into PIndexes and assign PIndexes to nodes.
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
		// This simple planner puts every PIndex on every node, so
		// there is not "real" partitioning.  This is good enough for
		// developer preview level requirement.
		sourcePartitions := ""

		// This simple PlanPIndex.Name here only works for simple
		// 1-to-1 case, which is developer preview level requirement.
		// NOTE: PlanPIndex.Name must be unique across the cluster and
		// functionally based off of the indexDef.
		name := indexDef.Name + "_" + indexDef.UUID + "_" + sourcePartitions

		planPIndex := &PlanPIndex{
			// TODO: More advanced planners will probably have to
			// incorporate SourcePartitions info into the
			// PlanPIndex.Name.
			Name:             name,
			UUID:             NewUUID(),
			IndexName:        indexDef.Name,
			IndexUUID:        indexDef.UUID,
			IndexMapping:     indexDef.Mapping,
			SourceType:       indexDef.SourceType,
			SourceName:       indexDef.SourceName,
			SourceUUID:       indexDef.SourceUUID,
			SourcePartitions: sourcePartitions,
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

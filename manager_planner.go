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
	"hash/crc32"
	"io"
	"strings"

	"github.com/couchbaselabs/blance"
	log "github.com/couchbaselabs/clog"
)

// A planner assigns partitions to cbft's and to PIndexes on each cbft.
// NOTE: You *must* update PLANNER_VERSION if the planning algorithm
// or schema changes, following semver rules.

// PlannerNOOP sends a synchronous NOOP request to the manager's planner, if any.
func (mgr *Manager) PlannerNOOP(msg string) {
	if mgr.tagsMap == nil || mgr.tagsMap["planner"] {
		SyncWorkReq(mgr.plannerCh, WORK_NOOP, msg, nil)
	}
}

// PlannerKick synchronously kicks the manager's planner, if any.
func (mgr *Manager) PlannerKick(msg string) {
	if mgr.tagsMap == nil || mgr.tagsMap["planner"] {
		SyncWorkReq(mgr.plannerCh, WORK_KICK, msg, nil)
	}
}

// PlannerLoop is the main loop for the planner.
func (mgr *Manager) PlannerLoop() {
	if mgr.cfg != nil { // Might be nil for testing.
		go func() {
			ec := make(chan CfgEvent)
			mgr.cfg.Subscribe(INDEX_DEFS_KEY, ec)
			mgr.cfg.Subscribe(CfgNodeDefsKey(NODE_DEFS_WANTED), ec)
			for e := range ec {
				mgr.PlannerKick("cfg changed, key: " + e.Key)
			}
		}()
	}

	for m := range mgr.plannerCh {
		var err error
		if m.op == WORK_KICK {
			changed, err := mgr.PlannerOnce(m.msg)
			if err != nil {
				log.Printf("error: PlannerOnce, err: %v", err)
				// Keep looping as perhaps it's a transient issue.
			} else if changed {
				mgr.JanitorKick("the plans have changed")
			}
		} else if m.op == WORK_NOOP {
			// NOOP.
		} else {
			err = fmt.Errorf("error: unknown planner op: %s, m: %#v", m.op, m)
		}
		if m.resCh != nil {
			if err != nil {
				m.resCh <- err
			}
			close(m.resCh)
		}
	}
}

func (mgr *Manager) PlannerOnce(reason string) (bool, error) {
	log.Printf("planner awakes, reason: %s", reason)

	if mgr.cfg == nil { // Can occur during testing.
		return false, fmt.Errorf("planner skipped due to nil cfg")
	}
	err := PlannerCheckVersion(mgr.cfg, mgr.version)
	if err != nil {
		return false, err
	}
	indexDefs, err := PlannerGetIndexDefs(mgr.cfg, mgr.version)
	if err != nil {
		return false, err
	}
	nodeDefs, err := PlannerGetNodeDefs(mgr.cfg, mgr.version, mgr.uuid, mgr.bindAddr)
	if err != nil {
		return false, err
	}
	planPIndexesPrev, cas, err := PlannerGetPlanPIndexes(mgr.cfg, mgr.version)
	if err != nil {
		return false, err
	}

	planPIndexes, err :=
		CalcPlan(indexDefs, nodeDefs, planPIndexesPrev, mgr.version, mgr.server)
	if err != nil {
		return false, fmt.Errorf("planner ended on CalcPlan, err: %v", err)
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

func PlannerCheckVersion(cfg Cfg, version string) error {
	ok, err := CheckVersion(cfg, version)
	if err != nil {
		return fmt.Errorf("planner skipped on CheckVersion err: %v", err)
	}
	if !ok {
		return fmt.Errorf("planner skipped with version too low: %v", version)
	}
	return nil
}

func PlannerGetIndexDefs(cfg Cfg, version string) (*IndexDefs, error) {
	indexDefs, _, err := CfgGetIndexDefs(cfg)
	if err != nil {
		return nil, fmt.Errorf("planner skipped on CfgGetIndexDefs err: %v", err)
	}
	if indexDefs == nil {
		return nil, fmt.Errorf("planner ended since no IndexDefs")
	}
	if VersionGTE(version, indexDefs.ImplVersion) == false {
		return nil, fmt.Errorf("planner ended since indexDefs.ImplVersion: %s"+
			" > version: %s", indexDefs.ImplVersion, version)
	}
	return indexDefs, nil
}

func PlannerGetNodeDefs(cfg Cfg, version, uuid, bindAddr string) (*NodeDefs, error) {
	nodeDefs, _, err := CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	if err != nil {
		return nil, fmt.Errorf("planner skipped on CfgGetNodeDefs err: %v", err)
	}
	if nodeDefs == nil {
		return nil, fmt.Errorf("planner ended since no NodeDefs")
	}
	if VersionGTE(version, nodeDefs.ImplVersion) == false {
		return nil, fmt.Errorf("planner ended since nodeDefs.ImplVersion: %s"+
			" > version: %s", nodeDefs.ImplVersion, version)
	}
	nodeDef, exists := nodeDefs.NodeDefs[bindAddr]
	if !exists || nodeDef == nil {
		return nil, fmt.Errorf("planner ended since no NodeDef, bindAddr: %s", bindAddr)
	}
	if nodeDef.ImplVersion != version {
		return nil, fmt.Errorf("planner ended since NodeDef, bindAddr: %s,"+
			" NodeDef.ImplVersion: %s != version: %s",
			bindAddr, nodeDef.ImplVersion, version)
	}
	if nodeDef.UUID != uuid {
		return nil, fmt.Errorf("planner ended since NodeDef, bindAddr: %s,"+
			" NodeDef.UUID: %s != uuid: %s",
			bindAddr, nodeDef.UUID, uuid)
	}
	isPlanner := true
	if nodeDef.Tags != nil && len(nodeDef.Tags) > 0 {
		isPlanner = false
		for _, tag := range nodeDef.Tags {
			if tag == "planner" {
				isPlanner = true
			}
		}
	}
	if !isPlanner {
		return nil, fmt.Errorf("planner ended since node, bindAddr: %s,"+
			" is not a planner, tags: %#v", bindAddr, nodeDef.Tags)
	}
	return nodeDefs, nil
}

func PlannerGetPlanPIndexes(cfg Cfg, version string) (*PlanPIndexes, uint64, error) {
	planPIndexesPrev, cas, err := CfgGetPlanPIndexes(cfg)
	if err != nil {
		return nil, 0, fmt.Errorf("planner skipped on CfgGetPlanPIndexes err: %v", err)
	}
	if planPIndexesPrev == nil {
		planPIndexesPrev = NewPlanPIndexes(version)
	}
	if VersionGTE(version, planPIndexesPrev.ImplVersion) == false {
		return nil, 0, fmt.Errorf("planner ended on planPIndexesPrev.ImplVersion: %s"+
			" > version: %s", planPIndexesPrev.ImplVersion, version)
	}
	return planPIndexesPrev, cas, nil
}

// Split logical indexes into PIndexes and assign PIndexes to nodes.
func CalcPlan(indexDefs *IndexDefs, nodeDefs *NodeDefs,
	planPIndexesPrev *PlanPIndexes, version, server string) (
	*PlanPIndexes, error) {
	// This simple planner assigns at most MaxPartitionsPerPIndex
	// number of partitions onto a PIndex.  And then uses blance to
	// assign the PIndex to 1 or more nodes (based on NumReplicas).
	if indexDefs == nil || nodeDefs == nil {
		return nil, nil
	}

	// Consider only the nodeDefs that can support pindexes.
	nodeUUIDs := make([]string, 0)
	for _, nodeDef := range nodeDefs.NodeDefs {
		tags := StringsToMap(nodeDef.Tags)
		if tags == nil || tags["pindex"] {
			nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)
		}
	}

	// Retrieve the nodeUUID's from the previous plan.
	nodeUUIDsPrev := make([]string, 0)
	if planPIndexesPrev != nil {
		for _, planPIndexPrev := range planPIndexesPrev.PlanPIndexes {
			for nodeUUIDPrev := range planPIndexPrev.NodeUUIDs {
				nodeUUIDsPrev = append(nodeUUIDsPrev, nodeUUIDPrev)
			}
		}
	}
	nodeUUIDsPrev = StringsIntersectStrings(nodeUUIDsPrev, nodeUUIDsPrev) // Remove dupes.

	// Calculate node deltas (nodes added & nodes removed).
	nodeUUIDsAll := make([]string, 0)
	nodeUUIDsAll = append(nodeUUIDsAll, nodeUUIDs...)
	nodeUUIDsAll = append(nodeUUIDsAll, nodeUUIDsPrev...)
	nodeUUIDsAll = StringsIntersectStrings(nodeUUIDsAll, nodeUUIDsAll) // Remove dupes.
	nodeUUIDsToAdd := StringsRemoveStrings(nodeUUIDsAll, nodeUUIDsPrev)
	nodeUUIDsToRemove := StringsRemoveStrings(nodeUUIDsAll, nodeUUIDs)

	// Examine every indexDef...
	planPIndexes := NewPlanPIndexes(version)

	for _, indexDef := range indexDefs.IndexDefs {
		// Split each indexDef into 1 or more PlanPIndexes.
		pindexImplType, exists := pindexImplTypes[indexDef.Type]
		if !exists ||
			pindexImplType == nil ||
			pindexImplType.New == nil ||
			pindexImplType.Open == nil {
			// TODO: Should we pop up a warning here?
			continue // Skip indexDef's with no instantiatable pindexImplType.
		}

		maxPartitionsPerPIndex := indexDef.PlanParams.MaxPartitionsPerPIndex

		sourcePartitionsArr, err := DataSourcePartitions(indexDef.SourceType,
			indexDef.SourceName, indexDef.SourceUUID, indexDef.SourceParams, server)
		if err != nil {
			log.Printf("error: planner could not get partitions,"+
				" indexDef: %#v, err: %v", indexDef, err)
			continue
		}

		planPIndexesForIndex := map[string]*PlanPIndex{}

		addPlanPIndex := func(sourcePartitionsCurr []string) {
			sourcePartitions := strings.Join(sourcePartitionsCurr, ",")

			planPIndex := &PlanPIndex{
				Name:             PlanPIndexName(indexDef, sourcePartitions),
				UUID:             NewUUID(),
				IndexType:        indexDef.Type,
				IndexName:        indexDef.Name,
				IndexUUID:        indexDef.UUID,
				IndexSchema:      indexDef.Schema,
				SourceType:       indexDef.SourceType,
				SourceName:       indexDef.SourceName,
				SourceUUID:       indexDef.SourceUUID,
				SourceParams:     indexDef.SourceParams,
				SourcePartitions: sourcePartitions,
				NodeUUIDs:        make(map[string]string),
			}

			planPIndexes.PlanPIndexes[planPIndex.Name] = planPIndex

			planPIndexesForIndex[planPIndex.Name] = planPIndex
		}

		sourcePartitionsCurr := []string{}
		for _, sourcePartition := range sourcePartitionsArr {
			sourcePartitionsCurr = append(sourcePartitionsCurr, sourcePartition)
			if maxPartitionsPerPIndex > 0 &&
				len(sourcePartitionsCurr) >= maxPartitionsPerPIndex {
				addPlanPIndex(sourcePartitionsCurr)
				sourcePartitionsCurr = []string{}
			}
		}

		if len(sourcePartitionsCurr) > 0 || // Assign any leftover partitions.
			len(planPIndexesForIndex) <= 0 { // Assign at least 1 PlanPIndex.
			addPlanPIndex(sourcePartitionsCurr)
		}

		// Once we have a 1 or more PlanPIndexes for the Index, use
		// blance to assign the PlanPIndexes to nodes, depending on
		// the numReplicas setting.
		//
		// First, reconstruct previous blance map from planPIndexesPrev.
		blancePrevMap := blance.PartitionMap{}
		for _, planPIndex := range planPIndexesForIndex {
			blancePartition := &blance.Partition{
				Name:         planPIndex.Name,
				NodesByState: map[string][]string{},
			}
			blancePrevMap[planPIndex.Name] = blancePartition
			if planPIndexesPrev != nil {
				planPIndexPrev, exists := planPIndexesPrev.PlanPIndexes[planPIndex.Name]
				if exists && planPIndexPrev != nil {
					for nodeUUIDPrev, _ := range planPIndexPrev.NodeUUIDs {
						blancePartition.NodesByState["master"] =
							append(blancePartition.NodesByState["master"], nodeUUIDPrev)
					}
				}
			}
		}

		modelMaster := blance.PartitionModel{
			"master": &blance.PartitionModelState{Priority: 0},
		}
		modelConstraints := map[string]int{
			"master": indexDef.PlanParams.NumReplicas + 1,
		}

		// TODO: Leverage these blance features.
		partitionWeights := map[string]int(nil)
		stateStickiness := map[string]int(nil)
		nodeWeights := map[string]int(nil)
		nodeHierarchy := map[string]string(nil)
		hierarchyRules := blance.HierarchyRules(nil)

		blanceNextMap, warnings := blance.PlanNextMap(blancePrevMap,
			nodeUUIDsAll,
			nodeUUIDsToRemove,
			nodeUUIDsToAdd,
			modelMaster,
			modelConstraints,
			partitionWeights,
			stateStickiness,
			nodeWeights,
			nodeHierarchy,
			hierarchyRules)
		for _, warning := range warnings {
			log.Printf("indexDef.Name: %s, PlanNextMap warning: %s, indexDef: %#v",
				indexDef.Name, warning, indexDef)
		}

		for planPIndexName, blancePartition := range blanceNextMap {
			planPIndex := planPIndexesForIndex[planPIndexName]
			for _, nodeUUID := range blancePartition.NodesByState["master"] {
				planPIndex.NodeUUIDs[nodeUUID] =
					PLAN_PINDEX_NODE_READ + PLAN_PINDEX_NODE_WRITE
			}
		}
	}

	return planPIndexes, nil
}

// NOTE: PlanPIndex.Name must be unique across the cluster and ideally
// functionally based off of the indexDef so that the SamePlanPIndex()
// comparison works even if concurrent planners are racing to
// calculate plans.
//
// NOTE: We can't use sourcePartitions directly as part of a
// PlanPIndex.Name suffix because in vbucket/hash partitioning the
// string would be too long -- since PIndexes might use
// PlanPIndex.Name for filesystem paths.
func PlanPIndexName(indexDef *IndexDef, sourcePartitions string) string {
	h := crc32.NewIEEE()
	io.WriteString(h, sourcePartitions)
	return indexDef.Name + "_" + indexDef.UUID + "_" + fmt.Sprintf("%x", h.Sum32())
}

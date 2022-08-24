//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"fmt"
	"strconv"
	"syscall"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rebalance"
	log "github.com/couchbase/clog"
)

func registerServerlessHooks(options map[string]string, dataDir string) map[string]string {
	if options["deploymentModel"] != "serverless" {
		return options
	}

	cbft.ServerlessMode = true

	// enable partition node stickiness, to disable default node
	// weight normalisation
	options["enablePartitionNodeStickiness"] = "true"

	// initialize usage thresholds
	options["resourceUtilizationHighWaterMark"] = defaultHighWaterMark
	options["resourceUtilizationLowWaterMark"] = defaultLowWaterMark
	options["resourceUnderUtilizationWaterMark"] = defaultUnderUtilizationWaterMark

	// FIXME
	options["maxBillableUnitsRate"] = "0"
	options["maxDisk"] = determineDiskCapacity(dataDir)

	cbgt.PlannerHooks["serverless"] = serverlessPlannerHook
	options["plannerHookName"] = "serverless"

	rebalance.RebalanceHook = serverlessRebalanceHook

	return options
}

func determineDiskCapacity(path string) string {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return "0"
	}
	return strconv.FormatUint(fs.Blocks*uint64(fs.Bsize), 10)
}

// -----------------------------------------------------------------------------

const defaultHighWaterMark = "0.8"
const defaultLowWaterMark = "0.5"
const defaultUnderUtilizationWaterMark = "0.3"

func shouldNodeAccommodateIndexPartition(nodeDef *cbgt.NodeDef) bool {
	if err := cbft.CanNodeAccommodateRequest(nodeDef); err != nil {
		return false
	}

	return true
}

const minNodeWeight = -100000

// -----------------------------------------------------------------------------

// serverlessPlannerHook is intended to be invoked in the serverless
// deployment model to adjust node weights and be able to group index
// partitions belonging to the same tenant/bucket as possible.
func serverlessPlannerHook(in cbgt.PlannerHookInfo) (cbgt.PlannerHookInfo, bool, error) {
	if in.PlanPIndexesPrev == nil ||
		len(in.PlanPIndexesPrev.PlanPIndexes) == 0 ||
		in.NodeDefs == nil {
		return in, false, nil
	}

	switch in.PlannerHookPhase {
	case "begin":
		return in, false, nil

	case "nodes":
		return in, false, nil

	case "indexDef.begin":
		return in, false, nil

	case "indexDef.split":
		// Check if planPIndexesForIndex are all already in planPIndexesPrev
		for pindexName, _ := range in.PlanPIndexesForIndex {
			if _, exists := in.PlanPIndexesPrev.PlanPIndexes[pindexName]; exists {
				// Skip relocating this already existing index
				log.Debugf("serverlessPlannerHook: pindex: %v already part of prev", pindexName)
				return in, true, nil
			}
			break
		}

		// New index introduction, influence location for it's partitions

		numPlanPIndexes := len(in.PlanPIndexesPrev.PlanPIndexes)
		nodePartitionCount := make(map[string]int) // nodeUUID to partition count
		nodeWeights := make(map[string]int)
		for _, nodeDef := range in.NodeDefs.NodeDefs {
			// Initialize to (0 - total number of existing index partitions in the system)
			nodeWeights[nodeDef.UUID] = -(numPlanPIndexes * 4)
			nodePartitionCount[nodeDef.UUID] = 0 // initialize
		}

		nodeAssignments := make(map[string]struct{}) // nodeUUID+sourceName

		for _, pindex := range in.PlanPIndexesPrev.PlanPIndexes {
			for nodeUUID := range pindex.Nodes {
				nodePartitionCount[nodeUUID]++
				nodeAssignments[nodeUUID+pindex.SourceName] = struct{}{}
			}
		}

		// Check if there're nodes out there that already hold index
		// partitions whose source is the same as the current index definition
		for uuid, count := range nodePartitionCount {
			if !shouldNodeAccommodateIndexPartition(in.NodeDefs.NodeDefs[uuid]) {
				nodeWeights[uuid] = minNodeWeight // this node is very high on usage
			} else {
				// Higher the resident partition count, lower the node weight
				if _, exists := nodeAssignments[uuid+in.IndexDef.SourceName]; exists {
					// Increase weight to prioritize this node, which holds
					// partitions sharing the source with the current index
					nodeWeights[uuid] = -count
					continue
				}

				// No node available with index partitions sharing the same source with
				// the current index, so prefer a node whose partition count is lesser
				nodeWeights[uuid] -= count
			}
		}

		in.NodeWeights = nodeWeights

		log.Debugf("serverlessPlannerHook: index: %v, proposed NodeWeights: %+v",
			in.IndexDef.Name, nodeWeights)
		return in, false, nil

	case "indexDef.balanced":
		return in, false, nil

	case "end":
		return in, false, nil

	default:
		return in, false, fmt.Errorf("unrecognized PlannerHookPhase: %v", in.PlannerHookPhase)
	}
}

// -----------------------------------------------------------------------------

// serverlessRebalanceHook is intended to be invoked in the serverless
// deployment model to adjust node weights to be able to influence
// partition positioning for the current index during rebalance.
func serverlessRebalanceHook(in rebalance.RebalanceHookInfo) (
	out rebalance.RebalanceHookInfo, err error) {
	if in.BegNodeDefs == nil {
		return in, nil
	}

	if in.BegPlanPIndexes == nil || len(in.BegPlanPIndexes.PlanPIndexes) == 0 {
		// No plan pindexes in the system (yet), no need to adjust node weights.
		return in, nil
	}

	if len(in.NodeUUIDsToRemove) == 0 {
		// Rebalance-in operation, with enableNodePartitionStickiness set to
		// true, no other node weights adjustment is necessary. Current index
		// partitions to remain where they were.
		return in, nil
	}

	numPlanPIndexes := len(in.BegPlanPIndexes.PlanPIndexes)
	nodePartitionCount := make(map[string]int) // nodeUUID to partition count
	nodeWeights := make(map[string]int)
	for _, nodeUUID := range in.NodeUUIDsAll {
		// Initialize to (0 - total number of existing index partitions in the system)
		nodeWeights[nodeUUID] = -(numPlanPIndexes * 4)
		nodePartitionCount[nodeUUID] = 0 // init
	}

	nodeAssignments := make(map[string]struct{}) // nodeUUID+sourceName
	indexNodeUUIDs := make(map[string]struct{})  // nodeUUIDs that hold current index

	for _, pindex := range in.BegPlanPIndexes.PlanPIndexes {
		for nodeUUID := range pindex.Nodes {
			nodePartitionCount[nodeUUID]++
			nodeAssignments[nodeUUID+pindex.SourceName] = struct{}{}
			if pindex.IndexUUID == in.IndexDef.UUID {
				indexNodeUUIDs[nodeUUID] = struct{}{}
			}
		}
	}

	// Rebalance to be applicable on affected indexes ONLY
	var indexAffected bool
	for _, nodeUUID := range in.NodeUUIDsToRemove {
		if _, exists := indexNodeUUIDs[nodeUUID]; exists {
			indexAffected = true
			// Remove this nodeUUID from the index map
			delete(indexNodeUUIDs, nodeUUID)
		}
	}
	if !indexAffected {
		// No need to adjust any node weights, favor stickiness
		return in, nil
	}

	if len(in.NodeUUIDsToAdd) == len(in.NodeUUIDsToRemove) {
		// SWAP REBALANCE;
		// p.s. This only comes into play when there's no node
		// at the beginning of rebalance without resident pindexes

		for _, nodeUUID := range in.NodeUUIDsToAdd {
			// Prioritize moving partition from outbound node(s) to inbound node(s)
			nodeWeights[nodeUUID] = 1
		}
	} else {
		// Check if there're nodes out there that already hold index
		// partitions whose source is the same as the current index definition
		for uuid, count := range nodePartitionCount {
			if !shouldNodeAccommodateIndexPartition(in.BegNodeDefs.NodeDefs[uuid]) {
				nodeWeights[uuid] = minNodeWeight // this node is very high on usage
			} else {
				// Higher the resident partition count, lower the node weight
				if _, exists := nodeAssignments[uuid+in.IndexDef.SourceName]; exists {
					// Increase weight to prioritize this node, which holds
					// partitions sharing the source with the current index
					nodeWeights[uuid] = -count
					continue
				}

				// No node available with index partitions sharing the same source with
				// the current index, so prefer a node whose partition count is lesser
				nodeWeights[uuid] -= count
			}
		}
	}

	// Boost weight of the nodes holding the index's partitions that
	// were untouched by the rebalance, to keep the already resident
	// partitions where they are only if node's usage is within
	// permissible limits.
	for nodeUUID := range indexNodeUUIDs {
		if nodeWeights[nodeUUID] != minNodeWeight {
			nodeWeights[nodeUUID] = 2
		}
	}

	in.NodeWeights = nodeWeights

	log.Debugf("serverlessRebalanceHook: index: %v, proposed NodeWeights: %+v",
		in.IndexDef.Name, nodeWeights)
	return in, nil
}

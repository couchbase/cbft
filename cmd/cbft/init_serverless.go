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

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/ctl"
	"github.com/couchbase/cbgt/rebalance"
	log "github.com/couchbase/clog"
)

func registerServerlessHooks(options map[string]string) map[string]string {
	if options["deploymentModel"] != "serverless" {
		return options
	}

	cbft.ServerlessMode = true

	// enable partition node stickiness, to disable default node
	// weight normalisation
	options["enablePartitionNodeStickiness"] = "true"

	// initialize usage thresholds
	if _, exists := options["resourceUtilizationHighWaterMark"]; !exists {
		options["resourceUtilizationHighWaterMark"] = defaultHighWaterMark
	}
	if _, exists := options["resourceUtilizationLowWaterMark"]; !exists {
		options["resourceUtilizationLowWaterMark"] = defaultLowWaterMark
	}
	if _, exists := options["resourceUnderUtilizationWaterMark"]; !exists {
		options["resourceUnderUtilizationWaterMark"] = defaultUnderUtilizationWaterMark
	}

	// initialize limits
	if _, exists := options["maxBillableUnitsRate"]; !exists {
		// TODO
		options["maxBillableUnitsRate"] = "0"
	}
	if _, exists := options["maxDiskBytes"]; !exists {
		// TODO
		options["maxDiskBytes"] = "0"
	}

	// Track statistics to determine moving average
	// Prometheus pulls low cardinality stats about every 10s, setting numEntries
	// to 360 to track moving average over 1 hour (3600/10).
	cbft.InitTimeSeriesStatTracker()
	cbft.TrackStatistic("memoryBytes", 360, false)
	cbft.TrackStatistic("totalUnitsMetered", 360, true)
	cbft.TrackStatistic("cpuPercent", 360, false)

	cbgt.PlannerHooks["serverless"] = serverlessPlannerHook
	options["plannerHookName"] = "serverless"

	rebalance.RebalanceHook = serverlessRebalanceHook

	ctl.DefragmentedUtilizationHook = defragmentationUtilizationHook

	return options
}

// -----------------------------------------------------------------------------

const defaultHighWaterMark = "0.8"
const defaultLowWaterMark = "0.5"
const defaultUnderUtilizationWaterMark = "0.3"

const minNodeWeight = -100000

// -----------------------------------------------------------------------------

// serverlessPlannerHook is intended to be invoked in the serverless
// deployment model to adjust node weights and be able to group index
// partitions belonging to the same tenant/bucket as possible.
func serverlessPlannerHook(in cbgt.PlannerHookInfo) (cbgt.PlannerHookInfo, bool, error) {
	if in.PlanPIndexesPrev == nil || len(in.PlanPIndexesPrev.PlanPIndexes) == 0 ||
		in.NodeDefs == nil || len(in.NodeDefs.NodeDefs) == 0 {
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
			if !cbft.CanNodeAccommodateRequest(in.NodeDefs.NodeDefs[uuid]) {
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
	if in.BegNodeDefs == nil || len(in.BegNodeDefs.NodeDefs) == 0 {
		return in, nil
	}

	if in.BegPlanPIndexes == nil || len(in.BegPlanPIndexes.PlanPIndexes) == 0 {
		// No plan pindexes in the system (yet), no need to adjust node weights.
		return in, nil
	}

	nodeUUIDsWithUsageOverHWM := cbft.NodeUUIDsWithUsageOverHWM(in.BegNodeDefs)

	if len(in.NodeUUIDsToRemove) == 0 {
		// Possibly a rebalance-in operation or auto-rebalance;
		// Only when resource usage on all nodes is below HWM, we don't need to edit the
		// node weights here because with enableNodePartitionStickiness set to true -
		// the resident index partitions will be favored to remain where they are.
		if len(nodeUUIDsWithUsageOverHWM) == 0 {
			return in, nil
		}
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
			// Remove this nodeUUID from the index map, so we don't
			// attempt to keep this partition on the same node.
			delete(indexNodeUUIDs, nodeUUID)
		}
	}
	// Also, check if current index is resident on nodes showing usage over HWM
	for _, nodeUUID := range nodeUUIDsWithUsageOverHWM {
		if _, exists := indexNodeUUIDs[nodeUUID]; exists {
			indexAffected = true
			// Remove this nodeUUID from the index map, so we don't
			// attempt to keep this partition on the same node.
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
		// Check if there are nodes out there that already hold index
		// partitions whose source is the same as the current index definition
		for uuid, count := range nodePartitionCount {
			if !cbft.CanNodeAccommodateRequest(in.BegNodeDefs.NodeDefs[uuid]) {
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

// -----------------------------------------------------------------------------

// defragmentationUtilizationHook will serve as a function override for the RPC
// that lets the cluster-manager/control-plane know if the service will benefit
// from a (auto) rebalance operation (index scrambling).
func defragmentationUtilizationHook(nodeDefs *cbgt.NodeDefs) (
	*service.DefragmentedUtilizationInfo, error) {
	nodesUtilStats := cbft.NodesUtilStats(nodeDefs)
	if len(nodesUtilStats) == 0 {
		return nil, fmt.Errorf("no node definitions/stats available")
	}

	var nodesBelowHWM, nodesAboveHWM []string
	var sumBillableUnitsRate, sumDiskUsage, sumMemoryUsage, sumCpuUsage uint64

	for uuid, stats := range nodesUtilStats {
		if stats.IsUtilizationOverHWM() {
			nodesAboveHWM = append(nodesAboveHWM, uuid)
		} else {
			nodesBelowHWM = append(nodesBelowHWM, uuid)
		}

		// This is a pretty basic "defragmented utilization" estimation
		// which simply averages the sum of usages across all nodes.
		// TODO: Perhaps we should take into account node hierarchy (server
		//  group) information for estimating the outcome.
		sumBillableUnitsRate += stats.BillableUnitsRate
		sumDiskUsage += stats.DiskUsage
		sumMemoryUsage += stats.MemoryUsage
		sumCpuUsage += stats.CPUUsage
	}

	rv := service.DefragmentedUtilizationInfo{}

	if len(nodesAboveHWM) > 0 && len(nodesBelowHWM) > 0 {
		// FTS could benefit from a rebalance
		samples := uint64(len(nodesUtilStats))
		for uuid, _ := range nodesUtilStats {
			nodeDef, _ := nodeDefs.NodeDefs[uuid]
			rv[nodeDef.HostPort] = map[string]interface{}{
				"billableUnitsRate": sumBillableUnitsRate / samples,
				"diskBytes":         sumDiskUsage / samples,
				"memoryBytes":       sumMemoryUsage / samples,
				"cpuPercent":        sumCpuUsage / samples,
			}
		}
	} else {
		// Either a rebalance is NOT necessary, or there aren't resources
		// available to benefit from a rebalance. Adding nodes will help
		// in the latter situation.
		// Either ways - we report node stats as is.
		for uuid, stats := range nodesUtilStats {
			nodeDef, _ := nodeDefs.NodeDefs[uuid]
			rv[nodeDef.HostPort] = map[string]interface{}{
				"billableUnitsRate": stats.BillableUnitsRate,
				"diskBytes":         stats.DiskUsage,
				"memoryBytes":       stats.MemoryUsage,
				"cpuPercent":        stats.CPUUsage,
			}
		}
	}

	return &rv, nil
}

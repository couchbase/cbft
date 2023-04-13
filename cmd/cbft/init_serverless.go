//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/ctl"
	"github.com/couchbase/cbgt/hibernate"
	"github.com/couchbase/cbgt/rebalance"
	log "github.com/couchbase/clog"
)

func registerServerlessHooks(options map[string]string) map[string]string {
	if options["deploymentModel"] != "serverless" {
		return options
	}

	cbft.ServerlessMode = true

	// serverless index and partition limits
	cbft.IndexLimitPerSource = 20
	cbft.ActivePartitionLimit = 1
	cbft.ReplicaPartitionLimit = 1

	// re-configure index and query quota fractions to better match the low/high watermarks
	defaultFTSMemIndexingFraction = 0.70
	defaultFTSMemQueryingFraction = 0.80

	cbgt.GocbcoreConnectionBufferSize = uint(16 * 1024) // 16KB for high tenant density

	// raise gocbcore DCP agent share limit per source to 10; meaning 10
	// partitions residing on the same node can share a single DCP agent.
	//
	// This number is chosen to ideally* be able to limit a user to the
	// use of 4 DCP agents if they've deployed 20 indexes (maximum) for
	// their bucket - which would deploy 20 active + 20 replica partitions.
	options["maxFeedsPerDCPAgent"] = "10"

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

	if _, exists := options["bucketInHibernation"]; !exists {
		options["bucketInHibernation"] = cbgt.NoBucketInHibernation
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

	cbgt.LimitIndexDefHook = cbft.LimitIndexDef
	cbgt.HibernatePartitionsHook = cbft.HibernatePartitions
	cbgt.HibernationBucketStateTrackerHook = cbft.TrackPauseBucketState
	cbgt.UnhibernationBucketStateTrackerHook = cbft.TrackResumeBucketState
	cbgt.HibernationClientHook = cbft.GetS3Client

	hibernate.GetRemoteBucketAndPathHook = cbft.GetRemoteBucketAndPathHook
	hibernate.DownloadMetadataHook = cbft.DownloadMetadata
	hibernate.UploadMetadataHook = cbft.UploadMetadata
	hibernate.CheckIfRemotePathIsValidHook = cbft.CheckIfRemotePathIsValid
	hibernate.BucketStateTrackerHook = cbft.TrackBucketState

	return options
}

// -----------------------------------------------------------------------------

const defaultHighWaterMark = "0.9"
const defaultLowWaterMark = "0.75"
const defaultUnderUtilizationWaterMark = "0.3"

const minNodeWeight = -100000

// -----------------------------------------------------------------------------

// serverlessPlannerHook is intended to be invoked in the serverless
// deployment model to adjust node weights and be able to group index
// partitions belonging to the same tenant/bucket as possible.
func serverlessPlannerHook(in cbgt.PlannerHookInfo) (cbgt.PlannerHookInfo, bool, error) {
	if in.NodeDefs == nil || len(in.NodeDefs.NodeDefs) == 0 {
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

		nodeWeights := make(map[string]int)
		for _, nodeDef := range in.NodeDefs.NodeDefs {
			nodeWeights[nodeDef.UUID] = 0
		}

		for uuid := range in.NodeDefs.NodeDefs {
			if !cbft.CanNodeAccommodateRequest(in.NodeDefs.NodeDefs[uuid]) {
				nodeWeights[uuid] = minNodeWeight // this node is very high on usage
			} else {
				// Deprioritise nodes with a large number of actives of the same source.
				nodeWeights[uuid] += 4 * (4*in.NumPlanPIndexes - in.NodeSourceActives[uuid+":"+in.IndexDef.SourceName])

				// Deprioritise nodes with a large number of actives of another source.
				nodeWeights[uuid] += 3 * (4*in.NumPlanPIndexes - (in.NodeTotalActives[uuid] -
					in.NodeSourceActives[uuid+":"+in.IndexDef.SourceName]))

				// Prioritise nodes with replicas of the same source to maximise connection sharing.
				nodeWeights[uuid] += 2 * in.NodeSourceReplicas[uuid+":"+in.IndexDef.SourceName]

				nodeWeights[uuid] += (in.NumPlanPIndexes - in.NodePartitionCount[uuid])
			}

			// picking out nodes suitable for replica partitions in a decreasing
			// order so that it's applicable even if Elixir decides to have >1 replica
			nodeWeights[uuid] += in.NodeSourceActives[uuid+":"+in.IndexDef.SourceName]
			nodeWeights[uuid] += in.NodeSourceReplicas[uuid+":"+in.IndexDef.SourceName]
			nodeWeights[uuid] += in.NumPlanPIndexes - in.NodePartitionCount[uuid]
		}

		in.NodeWeights = nodeWeights

		log.Debugf("serverlessPlannerHook: index: %v, proposed NodeWeights: %+v",
			in.IndexDef.Name, nodeWeights)
		return in, false, nil

	case "indexDef.balanced":
		// Updating the maps here since we know which partition has been
		// assigned to which node.
		for name, pindex := range in.PlanPIndexesForIndex {
			for nodeUUID, planNode := range pindex.Nodes {
				in.NodePartitionCount[nodeUUID]++
				if planNode.Priority == 0 {
					in.NodeSourceActives[nodeUUID+":"+pindex.SourceName]++
					in.NodeTotalActives[nodeUUID]++
				} else {
					in.NodeSourceReplicas[nodeUUID+":"+pindex.SourceName]++
				}
			}
			in.ExistingPlans.PlanPIndexes[name] = pindex
		}
		in.NumPlanPIndexes += len(in.PlanPIndexesForIndex)

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

	if in.ExistingPlanPIndexes == nil || len(in.ExistingPlanPIndexes.PlanPIndexes) == 0 {
		// No plan pindexes in the system (yet), no need to adjust node weights.
		return in, nil
	}

	if len(in.NodeUUIDsToAdd) > 0 && len(in.NodeUUIDsToRemove) == 0 {
		// SCALE OUT;
		// Early exit, no need to adjust node weights.
		log.Printf("serverlessRebalanceHook: scale out operation (adding: %v),"+
			" not moving index: %s", in.NodeUUIDsToAdd, in.IndexDef.Name)
		return in, nil
	}

	numPlanPIndexes := len(in.ExistingPlanPIndexes.PlanPIndexes)
	nodeWeights := make(map[string]int)
	for _, nodeUUID := range in.NodeUUIDsAll {
		nodeWeights[nodeUUID] = -1
	}

	indexNodeUUIDs := make(map[string]struct{}) // nodeUUIDs that hold current index
	for _, pindex := range in.ExistingPlanPIndexes.PlanPIndexes {
		for nodeUUID := range pindex.Nodes {
			if pindex.IndexUUID == in.IndexDef.UUID {
				indexNodeUUIDs[nodeUUID] = struct{}{}
			}
		}
	}

	if len(in.NodeUUIDsToRemove) > 0 && len(in.NodeUUIDsToAdd) == 0 {
		// SCALE IN;
		// Rebalance to be applicable on affected indexes ONLY
		var indexAffected bool
		for _, nodeUUID := range in.NodeUUIDsToRemove {
			if _, exists := indexNodeUUIDs[nodeUUID]; exists {
				indexAffected = true
			}
		}

		if !indexAffected {
			// No need to adjust any node weights, favor stickiness
			return in, nil
		}
	}

	if len(in.NodeUUIDsToAdd) == len(in.NodeUUIDsToRemove) && len(in.NodeUUIDsToAdd) > 0 {
		// SWAP REBALANCE;
		// p.s. This only comes into play when there's no node
		// at the beginning of rebalance without resident pindexes
		for _, nodeUUID := range in.NodeUUIDsToAdd {
			// Prioritize moving partition from outbound node(s) to inbound node(s)
			nodeWeights[nodeUUID] = 10
		}
	} else {
		// AUTO REBALANCE / SCALE IN;
		// Simply normalize node weights
		nodeWeights = cbgt.NormaliseNodeWeights(nodeWeights,
			in.EndPlanPIndexes, numPlanPIndexes)
	}

	in.NodeWeights = nodeWeights

	log.Printf("serverlessRebalanceHook: index: %s, proposed NodeWeights: %+v",
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

		if out, err := json.Marshal(nodesUtilStats); err == nil {
			log.Printf("defragmentationUtilizationHook: service could benefit"+
				" from a rebalance, nodeUtilStats: %s", string(out))
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

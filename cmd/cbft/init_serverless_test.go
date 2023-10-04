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
	"reflect"
	"testing"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rebalance"
)

func Test_serverlessPlannerHook_addIndex(t *testing.T) {
	prevCanNodeAccommodateRequest := cbft.CanNodeAccommodateRequest
	cbft.CanNodeAccommodateRequest = func(nodeDef *cbgt.NodeDef) bool {
		return true
	}
	defer func() {
		cbft.CanNodeAccommodateRequest = prevCanNodeAccommodateRequest
	}()

	// Begin: 4 Nodes, 3 Index defs executed
	// End: 4 Nodes, 4 Index defs to be executed (same source)
	input := cbgt.PlannerHookInfo{
		PlannerHookPhase: "indexDef.split",
		ExistingPlans: &cbgt.PlanPIndexes{
			PlanPIndexes: map[string]*cbgt.PlanPIndex{
				"one_12345": &cbgt.PlanPIndex{
					IndexName:  "one",
					IndexUUID:  "1",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
				"two_12345": &cbgt.PlanPIndex{
					IndexName:  "two",
					IndexUUID:  "2",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
					},
				},
				"three_12345": &cbgt.PlanPIndex{
					IndexName:  "three",
					IndexUUID:  "3",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
			},
		},
		NumPlanPIndexes: 3,
		NodeSourceReplicas: map[string]int{
			"n_0:x": 1,
			"n_1:x": 2,
		},
		NodeSourceActives: map[string]int{
			"n_0:x": 2,
			"n_1:x": 1,
		},
		NodeTotalActives: map[string]int{
			"n_0": 2,
			"n_1": 1,
		},
		NodePartitionCount: map[string]int{
			"n_0": 3,
			"n_1": 3,
		},
		NodeDefs: &cbgt.NodeDefs{
			NodeDefs: map[string]*cbgt.NodeDef{
				"n_0": &cbgt.NodeDef{
					UUID:      "n_0",
					Container: "group1",
				},
				"n_1": &cbgt.NodeDef{
					UUID:      "n_1",
					Container: "group2",
				},
				"n_2": &cbgt.NodeDef{
					UUID:      "n_2",
					Container: "group1",
				},
				"n_3": &cbgt.NodeDef{
					UUID:      "n_3",
					Container: "group2",
				},
			},
		},
		IndexDefs: &cbgt.IndexDefs{
			IndexDefs: map[string]*cbgt.IndexDef{
				"one": &cbgt.IndexDef{
					Name:       "one",
					UUID:       "1",
					SourceName: "x",
				},
				"two": &cbgt.IndexDef{
					Name:       "two",
					UUID:       "2",
					SourceName: "x",
				},
				"three": &cbgt.IndexDef{
					Name:       "three",
					UUID:       "3",
					SourceName: "x",
				},
				"four": &cbgt.IndexDef{
					Name:       "four",
					UUID:       "4",
					SourceName: "x",
				},
			},
		},
		IndexDef: &cbgt.IndexDef{
			Name:       "four",
			UUID:       "4",
			SourceName: "x",
		},
	}

	output, skip, err := serverlessPlannerHook(input)
	if err != nil || skip {
		t.Fatalf("skip: %v, err: %v", skip, err)
	}

	// Attempt to populate emptier nodes since the other nodes have existing partitions.
	if output.NodeWeights["n_2"] < output.NodeWeights["n_0"] ||
		output.NodeWeights["n_2"] < output.NodeWeights["n_1"] ||
		output.NodeWeights["n_3"] < output.NodeWeights["n_0"] ||
		output.NodeWeights["n_3"] < output.NodeWeights["n_1"] {
		t.Fatalf("Expect node weights of n_2 and n_3 to be higher for index `four`")
	}
}

// Test where an index is introduced into a 2-node cluster, one of which already
// has an active of the same source.
func Test_serverlessPlannerHook_addIndex2(t *testing.T) {
	prevCanNodeAccommodateRequest := cbft.CanNodeAccommodateRequest
	cbft.CanNodeAccommodateRequest = func(nodeDef *cbgt.NodeDef) bool {
		return true
	}
	defer func() {
		cbft.CanNodeAccommodateRequest = prevCanNodeAccommodateRequest
	}()

	input := cbgt.PlannerHookInfo{
		PlannerHookPhase: "indexDef.split",
		ExistingPlans: &cbgt.PlanPIndexes{
			PlanPIndexes: map[string]*cbgt.PlanPIndex{
				"one_12345": &cbgt.PlanPIndex{
					IndexName:  "one",
					IndexUUID:  "1",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
				"two_12345": &cbgt.PlanPIndex{
					IndexName:  "two",
					IndexUUID:  "2",
					SourceName: "y",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
					},
				},
			},
		},
		NodeDefs: &cbgt.NodeDefs{
			NodeDefs: map[string]*cbgt.NodeDef{
				"n_0": &cbgt.NodeDef{
					UUID:      "n_0",
					Container: "group1",
				},
				"n_1": &cbgt.NodeDef{
					UUID:      "n_1",
					Container: "group2",
				},
			},
		},
		IndexDefs: &cbgt.IndexDefs{
			IndexDefs: map[string]*cbgt.IndexDef{
				"one": &cbgt.IndexDef{
					Name:       "one",
					UUID:       "1",
					SourceName: "x",
				},
				"two": &cbgt.IndexDef{
					Name:       "two",
					UUID:       "2",
					SourceName: "y",
				},
			},
		},
		IndexDef: &cbgt.IndexDef{
			Name:       "three",
			UUID:       "3",
			SourceName: "x",
		},
	}

	output, skip, err := serverlessPlannerHook(input)
	if err != nil || skip {
		t.Fatalf("skip: %v, err: %v", skip, err)
	}

	// The active for index "three" should be introduced on n_0 since
	// it has lesser actives of the same source.
	if output.NodeWeights["n_0"] < output.NodeWeights["n_1"] {
		t.Fatalf("Expect node weight of n_0 to be higher than that of n_1")
	}
}

// Test to determine active and replica placement in a 2 node
// cluster which has existing partitions from different sources.
// n_0 -> active of x, replicas of y and a
// n_1 -> active of y and a, replica of x
// Introducing index of source z should position partitions in
// such a way that there are equal partitions on all nodes after planning.
func Test_serverlessPlannerHook_addInde_differentSource(t *testing.T) {
	prevCanNodeAccommodateRequest := cbft.CanNodeAccommodateRequest
	cbft.CanNodeAccommodateRequest = func(nodeDef *cbgt.NodeDef) bool {
		return true
	}
	defer func() {
		cbft.CanNodeAccommodateRequest = prevCanNodeAccommodateRequest
	}()

	input := cbgt.PlannerHookInfo{
		PlannerHookPhase: "indexDef.split",
		ExistingPlans: &cbgt.PlanPIndexes{
			PlanPIndexes: map[string]*cbgt.PlanPIndex{
				"one_12345": &cbgt.PlanPIndex{
					IndexName:  "one",
					IndexUUID:  "1",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
				"two_12345": &cbgt.PlanPIndex{
					IndexName:  "two",
					IndexUUID:  "2",
					SourceName: "y",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
				"four_12345": &cbgt.PlanPIndex{
					IndexName:  "four",
					IndexUUID:  "4",
					SourceName: "a",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
			},
		},
		NodeDefs: &cbgt.NodeDefs{
			NodeDefs: map[string]*cbgt.NodeDef{
				"n_0": &cbgt.NodeDef{
					UUID:      "n_0",
					Container: "group1",
				},
				"n_1": &cbgt.NodeDef{
					UUID:      "n_1",
					Container: "group2",
				},
			},
		},
		NodeSourceReplicas: map[string]int{
			"n_0:x": 1,
			"n_1:y": 1,
			"n_0:a": 1,
		},
		NodeSourceActives: map[string]int{
			"n_0:x": 1,
			"n_1:y": 1,
			"n_1:a": 1,
		},
		NodeTotalActives: map[string]int{
			"n_0": 1,
			"n_1": 2,
		},
		NodePartitionCount: map[string]int{
			"n_0": 3,
			"n_1": 3,
		},
		NumPlanPIndexes: 3,
		IndexDefs: &cbgt.IndexDefs{
			IndexDefs: map[string]*cbgt.IndexDef{
				"one": &cbgt.IndexDef{
					Name:       "one",
					UUID:       "1",
					SourceName: "x",
				},
				"two": &cbgt.IndexDef{
					Name:       "two",
					UUID:       "2",
					SourceName: "y",
				},
				"four": &cbgt.IndexDef{
					Name:       "four",
					UUID:       "2",
					SourceName: "a",
				},
			},
		},
		IndexDef: &cbgt.IndexDef{
			Name:       "three",
			UUID:       "3",
			SourceName: "z",
		},
	}

	output, skip, err := serverlessPlannerHook(input)
	if err != nil || skip {
		t.Fatalf("skip: %v, err: %v", skip, err)
	}

	// Prefer n_0 for the active since it has fewer total actives.
	if output.NodeWeights["n_0"] < output.NodeWeights["n_1"] {
		t.Fatalf("Expected node weight of n_0 to be greater than that of n_1")
	}
}

func Test_serverlessRebalanceHook_addNodes(t *testing.T) {
	prevCanNodeAccommodateRequest := cbft.CanNodeAccommodateRequest
	cbft.CanNodeAccommodateRequest = func(nodeDef *cbgt.NodeDef) bool {
		return true
	}
	prevNodesUtilStats := cbft.NodesUtilStats
	cbft.NodesUtilStats = func(nodeDefs *cbgt.NodeDefs) map[string]*cbft.NodeUtilStats {
		return nil
	}
	defer func() {
		cbft.CanNodeAccommodateRequest = prevCanNodeAccommodateRequest
		cbft.NodesUtilStats = prevNodesUtilStats
	}()

	input := rebalance.RebalanceHookInfo{
		Phase: rebalance.RebalanceHookPhaseAdjustNodeWeights,
		BegNodeDefs: &cbgt.NodeDefs{
			NodeDefs: map[string]*cbgt.NodeDef{
				"n_0": &cbgt.NodeDef{
					UUID:      "n_0",
					Container: "group1",
				},
				"n_1": &cbgt.NodeDef{
					UUID:      "n_1",
					Container: "group2",
				},
				"n_2": &cbgt.NodeDef{
					UUID:      "n_2",
					Container: "group1",
				},
				"n_3": &cbgt.NodeDef{
					UUID:      "n_3",
					Container: "group2",
				},
			},
		},
		BegPlanPIndexes: &cbgt.PlanPIndexes{
			PlanPIndexes: map[string]*cbgt.PlanPIndex{
				"one_12345": &cbgt.PlanPIndex{
					IndexName:  "one",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
				"two_12345": &cbgt.PlanPIndex{
					IndexName:  "two",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
					},
				},
			},
		},
		NodeUUIDsToAdd: []string{
			"n_0",
			"n_1",
			"n_2",
			"n_3",
		},
		NodeWeights: map[string]int{
			"n_0": 1,
			"n_1": 1,
		},
	}

	output, _, err := serverlessRebalanceHook(input)
	if err != nil {
		t.Fatal(err)
	}

	// Expect node weights to remain untouched
	if len(output.NodeWeights) != 2 ||
		output.NodeWeights["n_0"] != 1 ||
		output.NodeWeights["n_1"] != 1 {
		t.Fatalf("Unexpected node weights: %v", output.NodeWeights)
	}
}

func Test_serverlessRebalanceHook_removeNode(t *testing.T) {
	prevCanNodeAccommodateRequest := cbft.CanNodeAccommodateRequest
	cbft.CanNodeAccommodateRequest = func(nodeDef *cbgt.NodeDef) bool {
		return true
	}
	prevNodesUtilStats := cbft.NodesUtilStats
	cbft.NodesUtilStats = func(nodeDefs *cbgt.NodeDefs) map[string]*cbft.NodeUtilStats {
		return nil
	}
	defer func() {
		cbft.CanNodeAccommodateRequest = prevCanNodeAccommodateRequest
		cbft.NodesUtilStats = prevNodesUtilStats
	}()

	input := rebalance.RebalanceHookInfo{
		Phase: rebalance.RebalanceHookPhaseAdjustNodeWeights,
		BegNodeDefs: &cbgt.NodeDefs{
			NodeDefs: map[string]*cbgt.NodeDef{
				"n_0": &cbgt.NodeDef{
					UUID:      "n_0",
					Container: "group1",
				},
				"n_1": &cbgt.NodeDef{
					UUID:      "n_1",
					Container: "group2",
				},
				"n_2": &cbgt.NodeDef{
					UUID:      "n_2",
					Container: "group1",
				},
				"n_3": &cbgt.NodeDef{
					UUID:      "n_3",
					Container: "group2",
				},
			},
		},
		BegPlanPIndexes: &cbgt.PlanPIndexes{
			PlanPIndexes: map[string]*cbgt.PlanPIndex{
				"one_12345": &cbgt.PlanPIndex{
					IndexName:  "one",
					IndexUUID:  "1",
					SourceName: "x",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_0": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_1": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
				"two_12345": &cbgt.PlanPIndex{
					IndexName:  "two",
					IndexUUID:  "2",
					SourceName: "y",
					Nodes: map[string]*cbgt.PlanPIndexNode{
						"n_2": &cbgt.PlanPIndexNode{
							Priority: 0,
						},
						"n_3": &cbgt.PlanPIndexNode{
							Priority: 1,
						},
					},
				},
			},
		},
		IndexDef: &cbgt.IndexDef{
			Name:       "two",
			UUID:       "2",
			SourceName: "y",
		},
		NodeUUIDsToAdd: []string{
			"n_0",
			"n_1",
			"n_2",
		},
		NodeUUIDsToRemove: []string{
			"n_3",
		},
	}

	output, _, err := serverlessRebalanceHook(input)
	if err != nil {
		t.Fatal(err)
	}

	// Expect n_2's node weight to be the highest for index `two`,
	// so partitions resident on n_2 aren't moved.
	if output.NodeWeights["n_2"] < output.NodeWeights["n_0"] ||
		output.NodeWeights["n_2"] < output.NodeWeights["n_1"] ||
		output.NodeWeights["n_2"] < output.NodeWeights["n_3"] {
		t.Fatalf("Unexpected node weights: %v", output.NodeWeights)
	}
}

func Test_defragmentationUtilizationHook(t *testing.T) {
	prevNodesUtilStats := cbft.NodesUtilStats
	cbft.NodesUtilStats = func(nodeDefs *cbgt.NodeDefs) map[string]*cbft.NodeUtilStats {
		return map[string]*cbft.NodeUtilStats{
			"n_0": &cbft.NodeUtilStats{
				HighWaterMark:     0.8,
				BillableUnitsRate: 600,
				UtilizationStats: cbft.UtilizationStats{
					CPUUsage:    85,
					DiskUsage:   750,
					MemoryUsage: 700,
				},
				LimitDiskUsage:         1000,
				LimitBillableUnitsRate: 1000,
				LimitMemoryUsage:       1000,
			},
			"n_1": &cbft.NodeUtilStats{
				HighWaterMark:     0.8,
				BillableUnitsRate: 700,
				UtilizationStats: cbft.UtilizationStats{
					CPUUsage:    85,
					DiskUsage:   700,
					MemoryUsage: 600,
				},
				LimitBillableUnitsRate: 1000,
				LimitDiskUsage:         1000,
				LimitMemoryUsage:       1000,
			},
			"n_2": &cbft.NodeUtilStats{
				HighWaterMark:     0.8,
				BillableUnitsRate: 500,
				UtilizationStats: cbft.UtilizationStats{
					CPUUsage:    50,
					DiskUsage:   500,
					MemoryUsage: 300,
				},
				LimitBillableUnitsRate: 1000,
				LimitDiskUsage:         1000,
				LimitMemoryUsage:       1000,
			},
			"n_3": &cbft.NodeUtilStats{
				HighWaterMark:     0.8,
				BillableUnitsRate: 400,
				UtilizationStats: cbft.UtilizationStats{
					CPUUsage:    50,
					DiskUsage:   450,
					MemoryUsage: 200,
				},
				LimitBillableUnitsRate: 1000,
				LimitDiskUsage:         1000,
				LimitMemoryUsage:       1000,
			},
		}
	}
	defer func() {
		cbft.NodesUtilStats = prevNodesUtilStats
	}()

	input := &cbgt.NodeDefs{
		NodeDefs: map[string]*cbgt.NodeDef{
			"n_0": &cbgt.NodeDef{
				UUID:      "n_0",
				HostPort:  "n_0:9200",
				Container: "group1",
			},
			"n_1": &cbgt.NodeDef{
				UUID:      "n_1",
				HostPort:  "n_1:9202",
				Container: "group2",
			},
			"n_2": &cbgt.NodeDef{
				UUID:      "n_2",
				HostPort:  "n_2:9204",
				Container: "group1",
			},
			"n_3": &cbgt.NodeDef{
				UUID:      "n_3",
				HostPort:  "n_3:9206",
				Container: "group2",
			},
		},
	}

	expectBytes := []byte(`{
		"n_0:9200": {
			"billableUnitsRate":550,"diskBytes":600,"memoryBytes":450,"cpuPercent":67
		},
		"n_1:9202": {
			"billableUnitsRate":550,"diskBytes":600,"memoryBytes":450,"cpuPercent":67
		},
		"n_2:9204": {
			"billableUnitsRate":550,"diskBytes":600,"memoryBytes":450,"cpuPercent":67
		},
		"n_3:9206": {
			"billableUnitsRate":550,"diskBytes":600,"memoryBytes":450,"cpuPercent":67
		}
	}`)

	output, err := defragmentationUtilizationHook(input)
	if err != nil {
		t.Fatal(err)
	}

	outputBytes, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}

	var expect, got interface{}
	if err := json.Unmarshal(expectBytes, &expect); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(outputBytes, &got); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expect, got) {
		t.Fatalf("Expected: %s, Got: %s", string(expectBytes), string(outputBytes))
	}
}

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
		PlanPIndexesPrev: &cbgt.PlanPIndexes{
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

	if output.NodeWeights["n_0"] < output.NodeWeights["n_2"] ||
		output.NodeWeights["n_0"] < output.NodeWeights["n_3"] ||
		output.NodeWeights["n_1"] < output.NodeWeights["n_2"] ||
		output.NodeWeights["n_1"] < output.NodeWeights["n_3"] {
		t.Fatalf("Expect node weights of n_0 and n_1 to be higher for index `four`")
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

	output, err := serverlessRebalanceHook(input)
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

	output, err := serverlessRebalanceHook(input)
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
				HighWaterMark:          0.8,
				BillableUnitsRate:      600,
				LimitBillableUnitsRate: 1000,
				DiskUsage:              850,
				LimitDiskUsage:         1000,
				MemoryUsage:            700,
				LimitMemoryUsage:       1000,
			},
			"n_1": &cbft.NodeUtilStats{
				HighWaterMark:          0.8,
				BillableUnitsRate:      700,
				LimitBillableUnitsRate: 1000,
				DiskUsage:              850,
				LimitDiskUsage:         1000,
				MemoryUsage:            600,
				LimitMemoryUsage:       1000,
			},
			"n_2": &cbft.NodeUtilStats{
				HighWaterMark:          0.8,
				BillableUnitsRate:      500,
				LimitBillableUnitsRate: 1000,
				DiskUsage:              500,
				LimitDiskUsage:         1000,
				MemoryUsage:            300,
				LimitMemoryUsage:       1000,
			},
			"n_3": &cbft.NodeUtilStats{
				HighWaterMark:          0.8,
				BillableUnitsRate:      400,
				LimitBillableUnitsRate: 1000,
				DiskUsage:              500,
				LimitDiskUsage:         1000,
				MemoryUsage:            200,
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
			"billableUnitsRate":550,"diskBytes":675,"memoryBytes":450,"cpuPercent":0
		},
		"n_1:9202": {
			"billableUnitsRate":550,"diskBytes":675,"memoryBytes":450,"cpuPercent":0
		},
		"n_2:9204": {
			"billableUnitsRate":550,"diskBytes":675,"memoryBytes":450,"cpuPercent":0
		},
		"n_3:9206": {
			"billableUnitsRate":550,"diskBytes":675,"memoryBytes":450,"cpuPercent":0
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

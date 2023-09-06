// Copyright 2021-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package cbft

import (
	"reflect"
	"sort"
	"testing"

	"github.com/couchbase/cbgt"
)

func mockNodeDefs() *cbgt.NodeDefs {
	value := []byte(`{"uuid":"uuidSelf","nodeDefs":{"uuidSelf":
		{"hostPort":"127.0.0.1:9202","uuid":"uuidSelf",
		"implVersion":"5.0.0","tags":["feed","janitor","pindex","queryer","cbauth_service"],
		"container":"","weight":1,"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":` +
		`\"127.0.0.1:9002\",\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\": ` +
		`\"v0.5.0\"}"},"7879038ec4529cc4815f5d927c3df476":{"hostPort":"192.168.1.3:9200",
		"uuid":"7879038ec4529cc4815f5d927c3df476","implVersion":"5.0.0","tags":["feed",
		"janitor","pindex","queryer","cbauth_service"],"container":"","weight":1,
		"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":\"192.168.1.3:9000\", ` +
		`\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\":\"v0.5.0\"}"},
		"pi2":{"hostPort":"127.0.0.1:9201",
		"uuid":"pi2","implVersion":"5.0.0",
		"tags":["feed","janitor","pindex","queryer","cbauth_service"],"container":"",
		"weight":1,"extras":"{\"features\":\"leanPlan\",\"nsHostPort\":\"127.0.0.1:9001\",` +
		`\"version-cbft.app\":\"v0.5.0\",\"version-cbft.lib\":\"v0.5.0\"}"}},
		"implVersion":"5.0.0"}`)

	nodeDefs := &cbgt.NodeDefs{}
	err := UnmarshalJSON(value, nodeDefs)
	if err != nil {
		return nil
	}
	return nodeDefs
}

func mockPlanPIndex() *cbgt.PlanPIndex {
	value := []byte(`{
        "name": "pindex1",
        "uuid": "202387d93a87abbb",
        "indexType": "fulltext-index",
        "indexName": "b1",
        "indexUUID": "1ba9d4c221b75612",
        "sourceType": "couchbase",
        "sourceName": "beer-sample",
        "sourceUUID": "480ba695f6dc667f61d54027769185d1",
        "sourcePartitions": "1020,1021,1022,1023",
        "nodes": {
          "uuidSelf": {
            "canRead": true,
            "canWrite": true,
            "priority": 0
          }
        }}`)

	planPIndex := &cbgt.PlanPIndex{}
	err := UnmarshalJSON(value, planPIndex)
	if err != nil {
		return nil
	}
	return planPIndex
}

func TestGetStatsUrls(t *testing.T) {
	nodeDefs := mockNodeDefs()
	uuids := []string{"pi1", "pi2", "uuidSelf"}
	urlMap := getStatsUrls(uuids, "cbauthtest", nodeDefs)
	// as there is only 2 nodes in NodeDefs
	if len(urlMap) != 2 {
		t.Errorf("expecting two stats url entries in urlMap")
	}
}

func TestSrcPartitionSorter(t *testing.T) {
	rack1Node1 := &cbgt.NodeDef{UUID: "rack1node1", Container: "sg/rack1"}
	rack1Node2 := &cbgt.NodeDef{UUID: "rack1node1", Container: "sg/rack1"}
	rack1Node3 := &cbgt.NodeDef{UUID: "rack1node1", Container: "sg/rack1"}
	rack2Node1 := &cbgt.NodeDef{UUID: "rack1node1", Container: "sg/rack2"}
	rack2Node2 := &cbgt.NodeDef{UUID: "rack1node2", Container: "sg/rack2"}

	tests := []struct {
		sps    []*srcPartition
		state  string
		rack   string
		target *srcPartition
	}{
		// prefer primary partition on a node within the same rack.
		{sps: []*srcPartition{
			{nodeDef: rack1Node1, partitionCountOnNode: 13, state: "primary"},
			{nodeDef: rack1Node2, partitionCountOnNode: 5, state: "replica"},
			{nodeDef: rack2Node1, partitionCountOnNode: 5, state: "replica"}},
			state:  "primary",
			rack:   "sg/rack1",
			target: &srcPartition{nodeDef: rack1Node1, partitionCountOnNode: 13, state: "primary"},
		},
		// prefer replica partition on a node within the same rack.
		{sps: []*srcPartition{
			{nodeDef: rack1Node1, partitionCountOnNode: 13, state: "primary"},
			{nodeDef: rack1Node2, partitionCountOnNode: 5, state: "replica"},
			{nodeDef: rack2Node1, partitionCountOnNode: 5, state: "replica"}},
			state:  "replica",
			rack:   "sg/rack1",
			target: &srcPartition{nodeDef: rack1Node2, partitionCountOnNode: 5, state: "replica"},
		},
		// prefer replica partition on a node within the same rack
		// which has lowest partition load.
		{sps: []*srcPartition{
			{nodeDef: rack1Node1, partitionCountOnNode: 1, state: "primary"},
			{nodeDef: rack1Node2, partitionCountOnNode: 5, state: "replica"},
			{nodeDef: rack1Node3, partitionCountOnNode: 4, state: "replica"}},
			state:  "replica",
			rack:   "sg/rack1",
			target: &srcPartition{nodeDef: rack1Node3, partitionCountOnNode: 4, state: "replica"},
		},
		// prefer replica partition on a node with a remote rack
		// which has lowest partition load.
		{sps: []*srcPartition{
			{nodeDef: rack1Node1, partitionCountOnNode: 1, state: "primary"},
			{nodeDef: rack2Node1, partitionCountOnNode: 3, state: "replica"},
			{nodeDef: rack2Node2, partitionCountOnNode: 4, state: "replica"}},
			state:  "replica",
			rack:   "sg/rack1",
			target: &srcPartition{nodeDef: rack2Node1, partitionCountOnNode: 3, state: "replica"},
		},
		// prefer partition on a node with lowest load on local rack with no state override.
		{sps: []*srcPartition{
			{nodeDef: rack1Node1, partitionCountOnNode: 8, state: "primary"},
			{nodeDef: rack2Node2, partitionCountOnNode: 1, state: "replica"},
			{nodeDef: rack1Node2, partitionCountOnNode: 4, state: "replica"},
			{nodeDef: rack1Node3, partitionCountOnNode: 5, state: "replica"}},
			rack:   "sg/rack1",
			target: &srcPartition{nodeDef: rack1Node2, partitionCountOnNode: 4, state: "replica"},
		},
	}

	for _, test := range tests {
		srcSorter := &srcPartitionSorter{
			sps: test.sps, rack: test.rack, state: test.state,
		}

		sort.Sort(srcSorter)
		if !reflect.DeepEqual(test.target, srcSorter.sps[0]) {
			t.Errorf("expected target: %+v mismatched with the actual"+
				" : %+v", *test.target, *srcSorter.sps[0])
		}
	}

}

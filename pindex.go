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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// A PIndex represents a "physical" index or a index "partition".

const PINDEX_META_FILENAME string = "PINDEX_META"
const pindexPathSuffix string = ".pindex"

type PIndex struct {
	Name             string     `json:"name"`
	UUID             string     `json:"uuid"`
	IndexType        string     `json:"indexType"`
	IndexName        string     `json:"indexName"`
	IndexUUID        string     `json:"indexUUID"`
	IndexParams      string     `json:"indexParams"`
	SourceType       string     `json:"sourceType"`
	SourceName       string     `json:"sourceName"`
	SourceUUID       string     `json:"sourceUUID"`
	SourceParams     string     `json:"sourceParams"`
	SourcePartitions string     `json:"sourcePartitions"`
	Path             string     `json:"-"` // Transient, not persisted.
	Impl             PIndexImpl `json:"-"` // Transient, not persisted.
	Dest             Dest       `json:"-"` // Transient, not persisted.

	sourcePartitionsArr []string // Non-persisted memoization.
}

func (p *PIndex) Close(remove bool) error {
	// TODO: Need a close/cleanup protocol for p.Dest's, as some Dest
	// implementations have resources or goroutines that need cleanup.

	err := p.Impl.Close()
	if err != nil {
		return err
	}

	if remove {
		os.RemoveAll(p.Path)
	}

	return nil
}

func NewPIndex(mgr *Manager, name, uuid,
	indexType, indexName, indexUUID, indexParams,
	sourceType, sourceName, sourceUUID, sourceParams, sourcePartitions string,
	path string) (*PIndex, error) {
	var pindex *PIndex

	restart := func() {
		go func() {
			mgr.ClosePIndex(pindex)
			mgr.Kick("restart-pindex")
		}()
	}

	impl, dest, err := NewPIndexImpl(indexType, indexParams, path, restart)
	if err != nil {
		os.RemoveAll(path)
		return nil, fmt.Errorf("error: new indexType: %s, indexParams: %s,"+
			" path: %s, err: %s", indexType, indexParams, path, err)
	}

	pindex = &PIndex{
		Name:             name,
		UUID:             uuid,
		IndexType:        indexType,
		IndexName:        indexName,
		IndexUUID:        indexUUID,
		IndexParams:      indexParams,
		SourceType:       sourceType,
		SourceName:       sourceName,
		SourceUUID:       sourceUUID,
		SourceParams:     sourceParams,
		SourcePartitions: sourcePartitions,
		Path:             path,
		Impl:             impl,
		Dest:             dest,

		sourcePartitionsArr: strings.Split(sourcePartitions, ","),
	}
	buf, err := json.Marshal(pindex)
	if err != nil {
		impl.Close()
		os.RemoveAll(path)
		return nil, err
	}

	err = ioutil.WriteFile(path+string(os.PathSeparator)+PINDEX_META_FILENAME,
		buf, 0600)
	if err != nil {
		impl.Close()
		os.RemoveAll(path)
		return nil, fmt.Errorf("error: could not save PINDEX_META_FILENAME,"+
			" path: %s, err: %v", path, err)
	}

	return pindex, nil
}

// NOTE: Path argument must be a directory.
func OpenPIndex(mgr *Manager, path string) (*PIndex, error) {
	buf, err := ioutil.ReadFile(path + string(os.PathSeparator) + PINDEX_META_FILENAME)
	if err != nil {
		return nil, fmt.Errorf("error: could not load PINDEX_META_FILENAME,"+
			" path: %s, err: %v", path, err)
	}

	pindex := &PIndex{}
	err = json.Unmarshal(buf, pindex)
	if err != nil {
		return nil, fmt.Errorf("error: could not parse pindex json,"+
			" path: %s, err: %v", path, err)
	}

	restart := func() {
		go func() {
			mgr.ClosePIndex(pindex)
			mgr.Kick("restart-pindex")
		}()
	}

	impl, dest, err := OpenPIndexImpl(pindex.IndexType, path, restart)
	if err != nil {
		return nil, fmt.Errorf("error: could not open indexType: %s, path: %s, err: %v",
			pindex.IndexType, path, err)
	}

	pindex.Path = path
	pindex.Impl = impl
	pindex.Dest = dest

	return pindex, nil
}

func PIndexPath(dataDir, pindexName string) string {
	// TODO: path security checks / mapping here; ex: "../etc/pswd"
	return dataDir + string(os.PathSeparator) + pindexName + pindexPathSuffix
}

func ParsePIndexPath(dataDir, pindexPath string) (string, bool) {
	if !strings.HasSuffix(pindexPath, pindexPathSuffix) {
		return "", false
	}
	prefix := dataDir + string(os.PathSeparator)
	if !strings.HasPrefix(pindexPath, prefix) {
		return "", false
	}
	pindexName := pindexPath[len(prefix):]
	pindexName = pindexName[0 : len(pindexName)-len(pindexPathSuffix)]
	return pindexName, true
}

// ---------------------------------------------------------

type RemotePlanPIndex struct {
	PlanPIndex *PlanPIndex
	NodeDef    *NodeDef
}

// Returns a non-overlapping, disjoint set (or cut) of PIndexes
// (either local or remote) that cover all the partitons of an index
// so that the caller can perform scatter/gather queries, etc.  Only
// PlanPIndexes on wanted nodes that pass the wantNode filter will be
// returned.
//
// TODO: Perhaps need a tighter check around indexUUID, as the current
// implementation might have a race where old pindexes with a matching
// (but outdated) indexUUID might be chosen.
//
// TODO: This implementation currently always favors the local node's
// pindex, but should it?  Perhaps a remote node is more up-to-date
// than the local pindex?
//
// TODO: We should favor the most up-to-date node rather than
// the first one that we run into here?  But, perhaps the most
// up-to-date node is also the most overloaded?  Or, perhaps
// the planner may be trying to rebalance away the most
// up-to-date node and hitting it with load just makes the
// rebalance take longer?
func (mgr *Manager) CoveringPIndexes(indexName, indexUUID string,
	wantNode func(*PlanPIndexNode) bool) (
	localPIndexes []*PIndex, remotePlanPIndexes []*RemotePlanPIndex, err error) {
	nodeDefs, _, err := CfgGetNodeDefs(mgr.Cfg(), NODE_DEFS_WANTED)
	if err != nil {
		return nil, nil, fmt.Errorf("could not retrieve wanted nodeDefs, err: %v", err)
	}

	// Returns true if the node has the "pindex" tag.
	nodeDoesPIndexes := func(nodeUUID string) (*NodeDef, bool) {
		for _, nodeDef := range nodeDefs.NodeDefs {
			if nodeDef.UUID == nodeUUID {
				if len(nodeDef.Tags) <= 0 {
					return nodeDef, true
				}
				for _, tag := range nodeDef.Tags {
					if tag == "pindex" {
						return nodeDef, true
					}
				}
			}
		}
		return nil, false
	}

	_, allPlanPIndexes, err := mgr.GetPlanPIndexes(false)
	if err != nil {
		return nil, nil, fmt.Errorf("could not retrieve allPlanPIndexes, err: %v", err)
	}

	planPIndexes, exists := allPlanPIndexes[indexName]
	if !exists || len(planPIndexes) <= 0 {
		return nil, nil, fmt.Errorf("no planPIndexes for indexName: %s", indexName)
	}

	localPIndexes = make([]*PIndex, 0)
	remotePlanPIndexes = make([]*RemotePlanPIndex, 0)

	_, pindexes := mgr.CurrentMaps()

	selfUUID := mgr.UUID()
	_, selfDoesPIndexes := nodeDoesPIndexes(selfUUID)

build_alias_loop:
	for _, planPIndex := range planPIndexes {
		// First check whether this local node serves that planPIndex.
		if selfDoesPIndexes &&
			wantNode(planPIndex.Nodes[selfUUID]) {
			localPIndex, exists := pindexes[planPIndex.Name]
			if exists &&
				localPIndex != nil &&
				localPIndex.Name == planPIndex.Name &&
				localPIndex.IndexName == indexName &&
				(indexUUID == "" || localPIndex.IndexUUID == indexUUID) {
				localPIndexes = append(localPIndexes, localPIndex)
				continue build_alias_loop
			}
		}

		// Otherwise, look for a remote node that serves that planPIndex.
		for nodeUUID, planPIndexNode := range planPIndex.Nodes {
			if nodeUUID != selfUUID {
				nodeDef, ok := nodeDoesPIndexes(nodeUUID)
				if ok && wantNode(planPIndexNode) {
					remotePlanPIndexes = append(remotePlanPIndexes, &RemotePlanPIndex{
						PlanPIndex: planPIndex,
						NodeDef:    nodeDef,
					})
					continue build_alias_loop
				}
			}
		}

		return nil, nil, fmt.Errorf("no node covers planPIndex: %#v", planPIndex)
	}

	return localPIndexes, remotePlanPIndexes, nil
}

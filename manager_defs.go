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
)

// JSON/struct definitions of what the Manager stores in the Cfg.
// NOTE: You *must* update VERSION if you change these
// definitions or the planning algorithms change.

type IndexDefs struct {
	UUID          string               `json:"uuid"`
	IndexDefs     map[string]*IndexDef `json:"indexDefs"` // From Name -> IndexDef.
	CompatVersion string               `json:"compatVersion"`
}

type IndexDef struct {
	SourceType string `json:"sourceType"`
	SourceName string `json:"sourceName"`
	SourceUUID string `json:"sourceUUID"`
	Name       string `json:"name"`
	UUID       string `json:"uuid"`
	Mapping    string `json:"mapping"`

	// TODO: auth to access datasource?
	// TODO: hints at creation time for planner?
}

type NodeDefs struct {
	UUID          string              `json:"string"`
	NodeDefs      map[string]*NodeDef `json:"nodeDefs"` // From HostPort -> NodeDef.
	CompatVersion string              `json:"compatVersion"`
}

type NodeDef struct {
	HostPort string `json:"string"`
	UUID     string `json:"string"`

	// TODO: declared ability; not all indexers equal (cpu, ram, disk, etc)
}

type Plan struct {
}

// ------------------------------------------------------------------------

const INDEX_DEFS_KEY = "indexDefs"

func NewIndexDefs(version string) *IndexDefs {
	return &IndexDefs{
		UUID:          NewUUID(),
		IndexDefs:     make(map[string]*IndexDef),
		CompatVersion: version,
	}
}

func UnmarshalIndexDefs(jsonBytes []byte) (*IndexDefs, error) {
	rv := &IndexDefs{}
	if err := json.Unmarshal(jsonBytes, rv); err != nil {
		return nil, err
	}
	return rv, nil
}

func CfgGetIndexDefs(cfg Cfg) (*IndexDefs, uint64, error) {
	v, cas, err := cfg.Get(INDEX_DEFS_KEY, 0)
	if err != nil {
		return nil, 0, err
	}
	if v == nil {
		return nil, 0, nil
	}
	rv, err := UnmarshalIndexDefs(v)
	if err != nil {
		return nil, 0, err
	}
	return rv, cas, nil
}

func CfgSetIndexDefs(cfg Cfg, indexDefs *IndexDefs, cas uint64) (uint64, error) {
	buf, err := json.Marshal(indexDefs)
	if err != nil {
		return 0, err
	}
	return cfg.Set(INDEX_DEFS_KEY, buf, cas)
}

// ------------------------------------------------------------------------

const NODE_DEFS_KEY = "nodeDefs"

func NewNodeDefs(version string) *NodeDefs {
	return &NodeDefs{
		UUID:          NewUUID(),
		NodeDefs:      make(map[string]*NodeDef),
		CompatVersion: version,
	}
}

func UnmarshalNodeDefs(jsonBytes []byte) (*NodeDefs, error) {
	rv := &NodeDefs{}
	if err := json.Unmarshal(jsonBytes, rv); err != nil {
		return nil, err
	}
	return rv, nil
}

func CfgGetNodeDefs(cfg Cfg) (*NodeDefs, uint64, error) {
	v, cas, err := cfg.Get(NODE_DEFS_KEY, 0)
	if err != nil {
		return nil, 0, err
	}
	if v == nil {
		return nil, 0, nil
	}
	rv, err := UnmarshalNodeDefs(v)
	if err != nil {
		return nil, 0, err
	}
	return rv, cas, nil
}

func CfgSetNodeDefs(cfg Cfg, nodeDefs *NodeDefs, cas uint64) (uint64, error) {
	buf, err := json.Marshal(nodeDefs)
	if err != nil {
		return 0, err
	}
	return cfg.Set(NODE_DEFS_KEY, buf, cas)
}

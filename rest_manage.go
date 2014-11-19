//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"fmt"
	"net/http"
	"sort"
)

type FeedStatsHandler struct {
	mgr *Manager
}

func NewFeedStatsHandler(mgr *Manager) *FeedStatsHandler {
	return &FeedStatsHandler{mgr: mgr}
}

func (h *FeedStatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	feeds, _ := h.mgr.CurrentMaps()
	w.Write([]byte("[\n"))
	first := true
	feedNames := make([]string, 0, len(feeds))
	for feedName := range feeds {
		feedNames = append(feedNames, feedName)
	}
	sort.Strings(feedNames)
	for _, feedName := range feedNames {
		if !first {
			w.Write([]byte(",\n"))
		}
		first = false
		w.Write([]byte(fmt.Sprintf("  {\"feedName\":\"%s\",\"stats\":", feedName)))
		feeds[feedName].Stats(w)
		w.Write([]byte("}\n"))
	}
	w.Write([]byte("]\n"))
}

// ---------------------------------------------------

type ManagerKickHandler struct {
	mgr *Manager
}

func NewManagerKickHandler(mgr *Manager) *ManagerKickHandler {
	return &ManagerKickHandler{mgr: mgr}
}

func (h *ManagerKickHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.mgr.Kick(req.FormValue("msg"))
	mustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

// ---------------------------------------------------

type CfgGetHandler struct {
	mgr *Manager
}

func NewCfgGetHandler(mgr *Manager) *CfgGetHandler {
	return &CfgGetHandler{mgr: mgr}
}

func (h *CfgGetHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// TODO: Might need to scrub auth passwords from this output.
	cfg := h.mgr.Cfg()
	indexDefs, indexDefsCAS, indexDefsErr :=
		CfgGetIndexDefs(cfg)
	nodeDefsWanted, nodeDefsWantedCAS, nodeDefsWantedErr :=
		CfgGetNodeDefs(cfg, NODE_DEFS_WANTED)
	nodeDefsKnown, nodeDefsKnownCAS, nodeDefsKnownErr :=
		CfgGetNodeDefs(cfg, NODE_DEFS_KNOWN)
	planPIndexes, planPIndexesCAS, planPIndexesErr :=
		CfgGetPlanPIndexes(cfg)
	mustEncode(w, struct {
		Status            string        `json:"status"`
		IndexDefs         *IndexDefs    `json:"indexDefs"`
		IndexDefsCAS      uint64        `json:"indexDefsCAS"`
		IndexDefsErr      error         `json:"indexDefsErr"`
		NodeDefsWanted    *NodeDefs     `json:"nodeDefsWanted"`
		NodeDefsWantedCAS uint64        `json:"nodeDefsWantedCAS"`
		NodeDefsWantedErr error         `json:"nodeDefsWantedErr"`
		NodeDefsKnown     *NodeDefs     `json:"nodeDefsKnown"`
		NodeDefsKnownCAS  uint64        `json:"nodeDefsKnownCAS"`
		NodeDefsKnownErr  error         `json:"nodeDefsKnownErr"`
		PlanPIndexes      *PlanPIndexes `json:"planPIndexes"`
		PlanPIndexesCAS   uint64        `json:"planPIndexesCAS"`
		PlanPIndexesErr   error         `json:"planPIndexesErr"`
	}{
		Status:            "ok",
		IndexDefs:         indexDefs,
		IndexDefsCAS:      indexDefsCAS,
		IndexDefsErr:      indexDefsErr,
		NodeDefsWanted:    nodeDefsWanted,
		NodeDefsWantedCAS: nodeDefsWantedCAS,
		NodeDefsWantedErr: nodeDefsWantedErr,
		NodeDefsKnown:     nodeDefsKnown,
		NodeDefsKnownCAS:  nodeDefsKnownCAS,
		NodeDefsKnownErr:  nodeDefsKnownErr,
		PlanPIndexes:      planPIndexes,
		PlanPIndexesCAS:   planPIndexesCAS,
		PlanPIndexesErr:   planPIndexesErr,
	})
}

// ---------------------------------------------------

type CfgRefreshHandler struct {
	mgr *Manager
}

func NewCfgRefreshHandler(mgr *Manager) *CfgRefreshHandler {
	return &CfgRefreshHandler{mgr: mgr}
}

func (h *CfgRefreshHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.mgr.Cfg().Refresh()
	mustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

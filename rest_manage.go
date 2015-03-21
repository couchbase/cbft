//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"fmt"
	"net/http"
	"sort"

	bleveHttp "github.com/blevesearch/bleve/http"
)

type DiagGetHandler struct {
	versionMain string
	mgr         *Manager
	mr          *MsgRing
}

func NewDiagGetHandler(versionMain string,
	mgr *Manager, mr *MsgRing) *DiagGetHandler {
	return &DiagGetHandler{versionMain: versionMain, mgr: mgr, mr: mr}
}

func (h *DiagGetHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	handlers := []struct {
		Name        string
		Handler     http.Handler
		HandlerFunc http.HandlerFunc
	}{
		{"/api/cfg", NewCfgGetHandler(h.mgr), nil},
		{"/api/index", NewListIndexHandler(h.mgr), nil},
		{"/api/log", NewGetLogHandler(h.mr), nil},
		{"/api/managerMeta", NewManagerMetaHandler(h.mgr), nil},
		{"/api/pindex", NewListPIndexHandler(h.mgr), nil},
		{"/api/pindex-bleve", bleveHttp.NewListIndexesHandler(), nil},
		{"/api/runtime", NewRuntimeGetHandler(h.versionMain, h.mgr), nil},
		{"/api/runtime/flags", nil, restGetRuntimeFlags},
		{"/api/runtime/memStats", nil, restGetRuntimeMemStats},
		{"/api/runtime/stats", nil, restGetRuntimeStats},
		{"/api/stats", NewStatsHandler(h.mgr), nil},
	}

	w.Write(jsonOpenBrace)
	for i, handler := range handlers {
		if i > 0 {
			w.Write(jsonComma)
		}
		w.Write([]byte(fmt.Sprintf(`"%s":`, handler.Name)))
		if handler.Handler != nil {
			handler.Handler.ServeHTTP(w, req)
		}
		if handler.HandlerFunc != nil {
			handler.HandlerFunc.ServeHTTP(w, req)
		}
	}
	w.Write(jsonCloseBrace)
}

// ---------------------------------------------------

type StatsHandler struct {
	mgr *Manager
}

func NewStatsHandler(mgr *Manager) *StatsHandler {
	return &StatsHandler{mgr: mgr}
}

var statsFeedsPrefix = []byte("{\"feeds\":{")
var statsPIndexesPrefix = []byte("},\"pindexes\":{")
var statsNamePrefix = []byte("\"")
var statsStatsPrefix = []byte("\":")
var statsSuffix = []byte("}}")
var statsSep = []byte(",")

func (h *StatsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	feeds, pindexes := h.mgr.CurrentMaps()
	feedNames := make([]string, 0, len(feeds))
	for feedName := range feeds {
		feedNames = append(feedNames, feedName)
	}
	sort.Strings(feedNames)

	pindexNames := make([]string, 0, len(pindexes))
	for pindexName := range pindexes {
		pindexNames = append(pindexNames, pindexName)
	}
	sort.Strings(pindexNames)

	first := true
	w.Write(statsFeedsPrefix)
	for _, feedName := range feedNames {
		if !first {
			w.Write(statsSep)
		}
		first = false
		w.Write(statsNamePrefix)
		w.Write([]byte(feedName))
		w.Write(statsStatsPrefix)
		feeds[feedName].Stats(w)
	}

	first = true
	w.Write(statsPIndexesPrefix)
	for _, pindexName := range pindexNames {
		if !first {
			w.Write(statsSep)
		}
		first = false
		w.Write(statsNamePrefix)
		w.Write([]byte(pindexName))
		w.Write(statsStatsPrefix)
		pindexes[pindexName].Dest.Stats(w)
	}

	w.Write(statsSuffix)
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
	h.mgr.GetIndexDefs(true)
	h.mgr.GetPlanPIndexes(true)
	mustEncode(w, struct {
		Status string `json:"status"`
	}{Status: "ok"})
}

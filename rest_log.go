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

package cbft

import (
	"encoding/json"
	"net/http"
)

// TODO: Need to give the entire cbft codebase a scrub of its log
// messages and fmt.Errorf()'s.

type LogGetHandler struct {
	mgr *Manager
	mr  *MsgRing
}

func NewLogGetHandler(mgr *Manager, mr *MsgRing) *LogGetHandler {
	return &LogGetHandler{mgr: mgr, mr: mr}
}

func (h *LogGetHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte(`{"messages":[`))
	for i, message := range h.mr.Messages() {
		buf, err := json.Marshal(string(message))
		if err == nil {
			if i > 0 {
				w.Write(jsonComma)
			}
			w.Write(buf)
		}
	}
	w.Write([]byte(`],"events":[`))
	if h.mgr != nil {
		first := true
		h.mgr.m.Lock()
		p := h.mgr.events.Front()
		for p != nil {
			if !first {
				w.Write(jsonComma)
			}
			first = false
			w.Write(p.Value.([]byte))
			p = p.Next()
		}
		h.mgr.m.Unlock()
	}
	w.Write([]byte(`]}`))
}

//  Copyright (c) 2016 Couchbase, Inc.
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
	"github.com/couchbase/cbgt/rest"
	"github.com/couchbase/goutils/go-cbaudit"
	"net/http"
)

const (
	AuditDeleteIndexEvent       = 24576 // 0x6000
	AuditCreateUpdateIndexEvent = 24577 // 0x6001
	AuditControlEvent           = 24579 // 0x6003
	AuditConfigRefreshEvent     = 24580 // 0x6004
	AuditConfigReplanEvent      = 24581 // 0x6005
	AuditRunGCEvent             = 24582 // 0x6006
	AuditProfileCPUEvent        = 24583 // 0x6007
	AuditProfileMemoryEvent     = 24584 // 0x6008
)

type IndexControlAuditLog struct {
	audit.GenericFields
	IndexName string `json:"index_name"`
	Control   string `json:"control_name"`
}

type IndexAuditLog struct {
	audit.GenericFields
	IndexName string `json:"index_name"`
}

func GetAuditEventData(eventId uint32, req *http.Request) interface{} {
	switch eventId {
	case AuditDeleteIndexEvent, AuditCreateUpdateIndexEvent:
		indexName := rest.IndexNameLookup(req)
		d := IndexAuditLog{
			GenericFields: audit.GetAuditBasicFields(req),
			IndexName:     indexName,
		}
		return d
	case AuditConfigReplanEvent, AuditRunGCEvent,
		AuditProfileCPUEvent, AuditProfileMemoryEvent:
		return audit.GetAuditBasicFields(req)
	case AuditControlEvent:
		indexName := rest.IndexNameLookup(req)
		d := IndexControlAuditLog{
			GenericFields: audit.GetAuditBasicFields(req),
			IndexName:     indexName,
			Control:       rest.MuxVariableLookup(req, "op"),
		}
		return d
	}
	return nil
}

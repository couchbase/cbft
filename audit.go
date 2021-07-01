//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
	audit.CommonAuditFields
	IndexName string `json:"index_name"`
	Control   string `json:"control_name"`
}

type IndexAuditLog struct {
	audit.CommonAuditFields
	IndexName string `json:"index_name"`
}

func GetAuditEventData(eventId uint32, req *http.Request) interface{} {
	switch eventId {
	case AuditDeleteIndexEvent, AuditCreateUpdateIndexEvent:
		indexName := rest.IndexNameLookup(req)
		d := IndexAuditLog{
			CommonAuditFields: audit.GetCommonAuditFields(req),
			IndexName:         indexName,
		}
		return d
	case AuditConfigReplanEvent, AuditRunGCEvent,
		AuditProfileCPUEvent, AuditProfileMemoryEvent:
		return audit.GetCommonAuditFields(req)
	case AuditControlEvent:
		indexName := rest.IndexNameLookup(req)
		d := IndexControlAuditLog{
			CommonAuditFields: audit.GetCommonAuditFields(req),
			IndexName:         indexName,
			Control:           rest.RequestVariableLookup(req, "op"),
		}
		return d
	}
	return nil
}

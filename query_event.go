//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import "time"

// RegistryQueryEventCallbacks should be treated as read-only after
// process init()'ialization.
var RegistryQueryEventCallback func(int, QueryEvent, uint64) error

type QueryEvent struct {
	Kind     QueryEventKind
	Duration time.Duration
}

// QueryEventKind represents an event code for OnEvent() callbacks.
type QueryEventKind int

// EventQueryStart is fired before a query begins the search.
var EventQueryStart = QueryEventKind(1)

// EventQueryEnd is fired upon the completion of a query.
var EventQueryEnd = QueryEventKind(2)

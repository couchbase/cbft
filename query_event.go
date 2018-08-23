//  Copyright (c) 2018 Couchbase, Inc.
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

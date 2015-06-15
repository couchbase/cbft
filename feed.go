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
	"fmt"
	"io"
)

// A Feed interface represents an abstract data source.  A Feed
// instance is hooked up to one-or-more Dest instances.  When incoming
// data is received by a Feed, the Feed will invoke relvate methods on
// the relevant Dest instances.
//
// In this codebase, the words "index source", "source" and "data
// source" are often associated with and used roughly as synonyms with
// "feed".
type Feed interface {
	Name() string
	IndexName() string
	Start() error
	Close() error
	Dests() map[string]Dest // Key is partition identifier.

	// Writes stats as JSON to the given writer.
	Stats(io.Writer) error
}

// Default values for feed parameters.
const FEED_SLEEP_MAX_MS = 10000
const FEED_SLEEP_INIT_MS = 100
const FEED_BACKOFF_FACTOR = 1.5

// FeedTypes is a global registry of available feed types and is
// initialized on startup.  It should be immutable after startup time.
var FeedTypes = make(map[string]*FeedType) // Key is sourceType.

// A FeedType represents an immutable registration of a single feed
// type or data source type.
type FeedType struct {
	Start           FeedStartFunc
	Partitions      FeedPartitionsFunc
	Public          bool
	Description     string
	StartSample     interface{}
	StartSampleDocs map[string]string
}

// A FeedStartFunc is part of a FeedType registration as is invoked by
// a Manager when a new feed instance needs to be started.
type FeedStartFunc func(mgr *Manager, feedName, indexName, indexUUID string,
	sourceType, sourceName, sourceUUID, sourceParams string,
	dests map[string]Dest) error

// Each Feed or data-source type knows of the data partitions for a
// data source.
type FeedPartitionsFunc func(sourceType, sourceName, sourceUUID, sourceParams,
	server string) ([]string, error)

// RegisterFeedType is invoked at init/startup time to register a
// FeedType.
func RegisterFeedType(sourceType string, f *FeedType) {
	FeedTypes[sourceType] = f
}

// DataSourcePartitions is a helper function that returns the data
// source partitions for a named data source or feed type.
func DataSourcePartitions(sourceType, sourceName, sourceUUID, sourceParams,
	server string) ([]string, error) {
	feedType, exists := FeedTypes[sourceType]
	if !exists || feedType == nil {
		return nil, fmt.Errorf("feed: unknown sourceType: %s", sourceType)
	}

	return feedType.Partitions(sourceType, sourceName, sourceUUID,
		sourceParams, server)
}

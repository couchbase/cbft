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
	"io"
)

func init() {
	RegisterFeedType("nil", &FeedType{
		Start: func(mgr *Manager, feedName, indexName, indexUUID,
			sourceType, sourceName, sourceUUID, params string,
			dests map[string]Dest) error {
			return mgr.registerFeed(NewNILFeed(feedName, dests))
		},
		Partitions: func(sourceType, sourceName, sourceUUID, sourceParams,
			server string) ([]string, error) {
			return nil, nil
		},
	})
}

// A NILFeed never feeds any data to its dests.  It's useful for
// testing and for pindexes that are actually primary data sources.
type NILFeed struct {
	name  string
	dests map[string]Dest
}

func NewNILFeed(name string, dests map[string]Dest) *NILFeed {
	return &NILFeed{name: name, dests: dests}
}

func (t *NILFeed) Name() string {
	return t.name
}

func (t *NILFeed) Start() error {
	return nil
}

func (t *NILFeed) Close() error {
	return nil
}

func (t *NILFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *NILFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}

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
	"fmt"
)

// TODO: need way to track dead cows (non-beef)
// TODO: need a way to pass runtime config parameters to the different
// feed types, such as to configure cbdatasource options for DCP feed.
// TODO: need a way to collect these errors so REST api
// can show them to user ("hey, perhaps you deleted a bucket
// and should delete these related full-text indexes?
// or the couchbase cluster is just down.");
// perhaps as specialized clog writer?

func (mgr *Manager) startFeedByType(feedName, indexName, indexUUID,
	sourceType, sourceName, sourceUUID, sourceParams string,
	dests map[string]Dest) error {
	if sourceType == "couchbase" ||
		sourceType == "couchbase-dcp" {
		return mgr.startDCPFeed(feedName, indexName, indexUUID,
			sourceName, sourceUUID, sourceParams, dests)
	}

	if sourceType == "couchbase-tap" {
		return mgr.startTAPFeed(feedName, indexName, indexUUID,
			sourceName, sourceUUID, sourceParams, dests)
	}

	if sourceType == "dest" {
		return mgr.startDestFeed(feedName, dests)
	}

	if sourceType == "nil" {
		return mgr.registerFeed(NewNILFeed(feedName, dests))
	}

	return fmt.Errorf("error: startFeed() got unknown source type: %s", sourceType)
}

func (mgr *Manager) startDCPFeed(feedName, indexName, indexUUID,
	bucketName, bucketUUID, params string, dests map[string]Dest) error {
	feed, err := NewDCPFeed(feedName, mgr.server, "default",
		bucketName, bucketUUID, BasicPartitionFunc, dests)
	if err != nil {
		return fmt.Errorf("error: could not prepare DCP stream to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	if err = feed.Start(); err != nil {
		return fmt.Errorf("error: could not start dcp feed, server: %s, err: %v",
			mgr.server, err)
	}
	if err = mgr.registerFeed(feed); err != nil {
		feed.Close()
		return err
	}
	return nil
}

func (mgr *Manager) startTAPFeed(feedName, indexName, indexUUID,
	bucketName, bucketUUID, params string, dests map[string]Dest) error {
	feed, err := NewTAPFeed(feedName, mgr.server, "default",
		bucketName, bucketUUID, BasicPartitionFunc, dests)
	if err != nil {
		return fmt.Errorf("error: could not prepare TAP stream to server: %s,"+
			" bucketName: %s, indexName: %s, err: %v",
			mgr.server, bucketName, indexName, err)
	}
	if err = feed.Start(); err != nil {
		return fmt.Errorf("error: could not start tap feed, server: %s, err: %v",
			mgr.server, err)
	}
	if err = mgr.registerFeed(feed); err != nil {
		feed.Close()
		return err
	}
	return nil
}

func (mgr *Manager) startDestFeed(feedName string,
	dests map[string]Dest) error {
	feed, err := NewDestFeed(feedName, BasicPartitionFunc, dests)
	if err != nil {
		return err
	}
	if err = feed.Start(); err != nil {
		return fmt.Errorf("error: could not start dest feed, server: %s, err: %v",
			mgr.server, err)
	}
	if err = mgr.registerFeed(feed); err != nil {
		feed.Close()
		return err
	}
	return nil
}

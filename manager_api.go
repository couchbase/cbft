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

	log "github.com/couchbaselabs/clog"
)

// Creates a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) CreateIndex(sourceType, sourceName, sourceUUID,
	// TODO: what about auth info to be able to access bucket?
	// TODO: what if user changes pswd to bucket, but it's the same bucket & uuid?
	// TODO: what about hints for # of partitions, etc?
	indexName, indexMapping string) error {
	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return err
	}
	if indexDefs == nil {
		indexDefs = NewIndexDefs(mgr.version)
	}

	if _, exists := indexDefs.IndexDefs[indexName]; exists {
		return fmt.Errorf("error: index exists, indexName: %s", indexName)
	}

	uuid := NewUUID()

	indexDef := &IndexDef{
		Name:       indexName,
		UUID:       uuid,
		Mapping:    indexMapping,
		SourceType: sourceType,
		SourceName: sourceName,
		SourceUUID: sourceUUID,
	}

	indexDefs.UUID = uuid
	indexDefs.IndexDefs[indexName] = indexDef
	indexDefs.ImplVersion = mgr.version

	// TODO: check the ImplVersion to see if our version is too old.

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	if err != nil {
		return fmt.Errorf("error: could not save indexDefs, err: %v", err)
	}

	mgr.plannerCh <- ("api/CreateIndex, indexName: " + indexName)

	return nil
}

// Deletes a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) DeleteIndex(indexName string) error {
	// TODO - rewrite all this to update the Cfg and kick the planner.
	// Later, we need the Janitor to do all the below work to avoid a
	// concurrency race where it recreates feeds right after we
	// unregister them.  mgr.janitorCh <- true
	//
	// try to stop the feed
	// TODO: should be unregistering all feeds (multiple).
	feed := mgr.UnregisterFeed(indexName)
	if feed != nil {
		// TODO: This needs to be synchronous so that we know that
		// feeds have stopped sending to their Streams.
		err := feed.Close()
		if err != nil {
			log.Printf("error closing stream: %v", err)
		}
		// Not returning error here because we still want to try and
		// close the Stream and PIndex.
	}

	pindex := mgr.UnregisterPIndex(indexName)
	if pindex != nil {
		// TODO: what about any inflight queries or ops?
		close(pindex.Stream)
	}

	return nil
}

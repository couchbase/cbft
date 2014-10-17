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
	"os"

	log "github.com/couchbaselabs/clog"
)

// Creates a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) CreateIndex(bucketName, bucketUUID,
	indexName string, indexMappingBytes []byte) error {
	// TODO: a logical index might map to multiple PIndexes, not the current 1-to-1.
	indexPath := mgr.PIndexPath(indexName)

	// TODO: need to check if this pindex already exists?
	// TODO: need to alloc a version/uuid for the pindex?
	// TODO: need to save feed reconstruction info (bucketName, bucketUUID, etc)
	//   with the pindex
	pindex, err := NewPIndex(indexName, indexPath, indexMappingBytes)
	if err != nil {
		return fmt.Errorf("error running pindex: %v", err)
	}

	mgr.RegisterPIndex(pindex)
	mgr.janitorCh <- true

	// TODO: Create a uuid for the index?

	return nil
}

// Deletes a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) DeleteIndex(indexName string) error {
	// try to stop the feed
	// TODO: should be future, multiple feeds
	feed := mgr.UnregisterFeed(indexName)
	if feed != nil {
		err := feed.Close()
		if err != nil {
			log.Printf("error closing stream: %v", err)
		}
		// not returning error here
		// because we still want to try and delete it
	}

	pindex := mgr.UnregisterPIndex(indexName)
	if pindex != nil {
		// TODO: if we closed the stream right now, then the feed might
		// incorrectly try writing to a closed channel.
		// If there is multiple feeds going into one stream (fan-in)
		// then need to know how to count down to the final Close().
		// err := stream.Close()
		// if err != nil {
		// 	log.Printf("error closing pindex: %v", err)
		// }
		// not returning error here
		// because we still want to try and delete it
	}

	// TODO: what about any inflight queries or ops?

	// close the index
	// TODO: looks like the pindex should be responsible
	// for the final bleve.Close()
	// and actual subdirectory deletes?
	// indexToDelete := bleveHttp.UnregisterIndexByName(indexName)
	// if indexToDelete == nil {
	// 	log.Printf("no such index '%s'", indexName)
	//  }
	// indexToDelete.Close()
	pindex.BIndex().Close()

	mgr.janitorCh <- true

	// now delete it
	// TODO: should really send a msg to PIndex who's responsible for
	// actual file / subdir to do the deletion rather than here.

	return os.RemoveAll(mgr.PIndexPath(indexName))
}

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
	"strconv"

	"github.com/couchbase/gomemcached"
	"github.com/couchbaselabs/go-couchbase"
)

func init() {
	if gomemcached.MaxBodyLen < int(3e7) { // 30,000,000.
		gomemcached.MaxBodyLen = int(3e7)
	}
}

func ParsePartitionsToVBucketIds(dests map[string]Dest) ([]uint16, error) {
	vbuckets := make([]uint16, 0, len(dests))
	for partition, _ := range dests {
		if partition != "" {
			vbId, err := strconv.Atoi(partition)
			if err != nil {
				return nil, fmt.Errorf("feed_cb:"+
					" could not parse partition: %s, err: %v", partition, err)
			}
			vbuckets = append(vbuckets, uint16(vbId))
		}
	}
	return vbuckets, nil
}

func VBucketIdToPartitionDest(pf DestPartitionFunc,
	dests map[string]Dest, vbucketId uint16, key []byte) (
	partition string, dest Dest, err error) {
	if vbucketId < uint16(len(vbucketIdStrings)) {
		partition = vbucketIdStrings[vbucketId]
	}
	if partition == "" {
		partition = fmt.Sprintf("%d", vbucketId)
	}
	dest, err = pf(partition, key, dests)
	if err != nil {
		return "", nil, fmt.Errorf("feed_cb: VBucketIdToPartitionDest,"+
			" partition func, vbucketId: %d, err: %v", vbucketId, err)
	}
	return partition, dest, err
}

var vbucketIdStrings []string

func init() {
	vbucketIdStrings = make([]string, 1024)
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
	}
}

// ----------------------------------------------------------------

func CouchbasePartitions(sourceType, sourceName, sourceUUID, sourceParams,
	server string) ([]string, error) {
	poolName := "default" // TODO: Parameterize poolName.
	bucketName := sourceName

	// TODO: how the halloween does GetBucket() api work without explicit auth?
	bucket, err := couchbase.GetBucket(server, poolName, bucketName)
	if err != nil {
		return nil, fmt.Errorf("feed_cb: DataSourcePartitions/couchbase"+
			" failed GetBucket, server: %s, poolName: %s, bucketName: %s, err: %v",
			server, poolName, bucketName, err)
	}
	defer bucket.Close()

	vbm := bucket.VBServerMap()
	if vbm == nil {
		return nil, fmt.Errorf("feed_cb: DataSourcePartitions/couchbase"+
			" no VBServerMap, server: %s, poolName: %s, bucketName: %s, err: %v",
			server, poolName, bucketName, err)
	}

	// NOTE: We assume that vbucket numbers are continuous
	// integers starting from 0.
	numVBuckets := len(vbm.VBucketMap)
	rv := make([]string, numVBuckets)
	for i := 0; i < numVBuckets; i++ {
		rv[i] = strconv.Itoa(i)
	}
	return rv, nil
}

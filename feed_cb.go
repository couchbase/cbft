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
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/gomemcached"
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

type CBFeedParams struct {
	AuthUser     string `json:"authUser"` // May be "" for no auth.
	AuthPassword string `json:"authPassword"`
}

func CouchbasePartitions(sourceType, sourceName, sourceUUID, sourceParams,
	serverIn string) (partitions []string, err error) {
	server, poolName, bucketName :=
		CouchbaseParseSourceName(serverIn, "default", sourceName)

	params := CBFeedParams{}
	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), &params)
		if err != nil {
			return nil, fmt.Errorf("feed_cb: DataSourcePartitions/couchbase"+
				" failed sourceParams JSON to CBFeedParams,"+
				" server: %s, poolName: %s, bucketName: %s,"+
				" sourceType: %s, sourceParams: %q, err: %v",
				server, poolName, bucketName, sourceType, sourceParams, err)
		}
	}

	var client couchbase.Client

	if params.AuthUser != "" || bucketName != "default" {
		client, err = couchbase.ConnectWithAuthCreds(server,
			params.AuthUser, params.AuthPassword)
	} else {
		client, err = couchbase.Connect(server)
	}
	if err != nil {
		return nil, fmt.Errorf("feed_cb: DataSourcePartitions/couchbase"+
			" connection failed, server: %s, poolName: %s,"+
			" bucketName: %s, sourceType: %s, sourceParams: %q, err: %v,"+
			" please check that your authUser and authPassword are correct"+
			" and that your couchbase server (%q) is available",
			server, poolName, bucketName, sourceType, sourceParams, err, server)
	}

	pool, err := client.GetPool(poolName)
	if err != nil {
		return nil, fmt.Errorf("feed_cb: DataSourcePartitions/couchbase"+
			" failed GetPool, server: %s, poolName: %s,"+
			" bucketName: %s, sourceType: %s, sourceParams: %q, err: %v",
			server, poolName, bucketName, sourceType, sourceParams, err)
	}

	bucket, err := pool.GetBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("feed_cb: DataSourcePartitions/couchbase"+
			" failed GetBucket, server: %s, poolName: %s,"+
			" bucketName: %s, err: %v, please check that your"+
			" bucketName/sourceName (%q) is correct",
			server, poolName, bucketName, err, bucketName)
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

// CouchbaseParseSourceName parses a sourceName, if it's a couchbase
// REST/HTTP URL, into a server URL, poolName and bucketName.
// Otherwise, returns the serverURLDefault, poolNameDefault, and treat
// the sourceName as a bucketName.
func CouchbaseParseSourceName(
	serverURLDefault, poolNameDefault, sourceName string) (
	string, string, string) {
	if !strings.HasPrefix(sourceName, "http://") &&
		!strings.HasPrefix(sourceName, "https://") {
		return serverURLDefault, poolNameDefault, sourceName
	}

	u, err := url.Parse(sourceName)
	if err != nil {
		return serverURLDefault, poolNameDefault, sourceName
	}

	a := strings.Split(u.Path, "/")
	if len(a) != 5 ||
		a[0] != "" ||
		a[1] != "pools" ||
		a[2] == "" ||
		a[3] != "buckets" ||
		a[4] == "" {
		return serverURLDefault, poolNameDefault, sourceName
	}

	v := url.URL{
		Scheme: u.Scheme,
		User:   u.User,
		Host:   u.Host,
	}

	server := v.String()
	poolName := a[2]
	bucketName := a[4]

	return server, poolName, bucketName
}

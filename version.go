//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

// cbft product version
const VERSION = "v0.5.5"

// cbft api versions, helps in controlling the
// service expectations during upgrades.
// See: MB-17990 on cbft API versioning.
const API_MAX_VERSION = "2.0.0"
const API_MIN_VERSION = "0.0.0"

var API_MAX_VERSION_JSON = WithJSONVersion(API_MAX_VERSION)

const VersionTag = "version="

// FeatureGeoSpatialVersion contains the spatial
// indexing/query capabilities based on s2.
var FeatureGeoSpatialVersion = "7.1.1"

var FeatureCollectionVersion = "7.0.0"
var FeatureGrpcVersion = "6.5.0"
var FeatureIndexNameDecor = "7.2.0"

func HandleAPIVersion(h string) (string, error) {
	if len(h) <= 0 || h == "*/*" {
		return API_MAX_VERSION, nil
	}

	foundRequestVersion := false

	for _, val := range strings.Split(h, ",") {
		versionIndex := strings.Index(val, VersionTag)
		if versionIndex < 0 {
			continue
		}

		foundRequestVersion = true

		requestVersion := val[versionIndex+len(VersionTag):]
		if cbgt.VersionGTE(API_MAX_VERSION, requestVersion) &&
			cbgt.VersionGTE(requestVersion, API_MIN_VERSION) {
			return requestVersion, nil
		}
	}

	if !foundRequestVersion {
		return API_MAX_VERSION, nil
	}

	return "", fmt.Errorf("version: unsupported version")
}

func WithJSONVersion(v string) string {
	return "application/json;" + VersionTag + v
}

func CheckAPIVersion(w http.ResponseWriter, req *http.Request) (err error) {
	if req.Header != nil {
		accept := req.Header["Accept"]
		if len(accept) > 0 {
			version, err := HandleAPIVersion(accept[0])
			if err != nil {
				w.WriteHeader(406)

				versionList := []string{
					WithJSONVersion(API_MAX_VERSION),
					WithJSONVersion(API_MIN_VERSION),
				}

				rest.MustEncode(w, versionList)
				return err
			}

			w.Header().Set("Content-type", WithJSONVersion(version))
			return nil
		}
	}

	w.Header().Set("Content-type", API_MAX_VERSION_JSON)
	return nil
}

// versionTracker checks the cluster compatibilty version
// with the ns-server and tracks the cluster version until
// the effective cluster version matches the given application version.
var versionTracker *clusterVersionTracker

type clusterVersionTracker struct {
	server             string
	version            uint64 // current cbft version.
	clusterVersion     uint64 // current compatibility version found on cluster.
	compatibleFeatures map[string]struct{}
}

func StartClusterVersionTracker(version string, server string) {
	versionTracker = &clusterVersionTracker{server: server + "/pools/default"}
	versionTracker.version, _ = cbgt.CompatibilityVersion(version)
	versionTracker.compatibleFeatures = make(map[string]struct{})
	go versionTracker.run()
}

func (vt *clusterVersionTracker) run() {
	ev, err := getEffectiveClusterVersion(vt.server)
	if err != nil {
		log.Printf("version: getEffectiveClusterVersion, err: %v", err)
	}

	vt.clusterVersion = ev

	if vt.version == ev {
		log.Printf("version: matching clusterCompatibility"+
			" version: %d found", ev)
		return
	}

	// monitor the effective cluster version until it matches the app version.
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ev, err := getEffectiveClusterVersion(vt.server)
			if err != nil {
				log.Printf("version: getEffectiveClusterVersion, err: %v", err)
				continue
			}

			vt.clusterVersion = ev

			if vt.version != ev {
				continue
			}

			log.Printf("version: matching clusterCompatibility"+
				" version: %d found", ev)

			return
		}
	}
}

// clusterCompatibleForVersion checks whether a compatible cluster found
// If not, then it checks whether the current cluster compatibility
// version is greater than or equal the given app version.
func (vt *clusterVersionTracker) clusterCompatibleForVersion(
	version string) bool {
	givenVersion, err := cbgt.CompatibilityVersion(version)
	if err == nil && vt.clusterVersion != 0 &&
		vt.clusterVersion >= givenVersion {
		return true
	}

	return false
}

func getEffectiveClusterVersion(server string) (uint64, error) {
	if len(server) < 2 {
		return 1, fmt.Errorf("version: no server URL configured")
	}

	u, err := cbgt.CBAuthURL(server)
	if err != nil {
		return 0, fmt.Errorf("version: auth for ns_server,"+
			" server: %s, authType: %s, err: %v",
			server, "cbauth", err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, err
	}

	req.Header.Add("Content-Type", "application/json")

	resp, err := cbgt.HttpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	respBuf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("version: error reading resp.Body,"+
			" nsServerURL: %s, resp: %#v, err: %v", server, resp, err)
	}

	rv := &cbgt.NsServerResponse{}
	err = json.Unmarshal(respBuf, rv)
	if err != nil {
		return 0, fmt.Errorf("version: error parsing respBuf: %s,"+
			" server: %s, err: %v", respBuf, server, err)

	}

	return uint64(rv.Nodes[0].ClusterCompatibility), nil
}

func isClusterCompatibleFor(feature string) bool {
	if versionTracker == nil {
		return false
	}

	if _, ok := versionTracker.compatibleFeatures[feature]; ok {
		return true
	}

	isCompatible := versionTracker.clusterCompatibleForVersion(feature)
	if isCompatible {
		versionTracker.compatibleFeatures[feature] = struct{}{}
	}

	return isCompatible
}

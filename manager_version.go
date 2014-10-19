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
	log "github.com/couchbaselabs/clog"
)

// NOTE: You *must* update VERSION if you change what's stored in the
// Cfg (such as the JSON/struct definitions or planning algorithms).
const VERSION = "0.0.0"
const VERSION_KEY = "version"

// Returns true if a given version is modern enough to modify the Cfg.
// Older versions (which are running with older JSON/struct defintions
// or planning algorithms) will see false from their CheckVersion()'s.
func CheckVersion(cfg Cfg, myVersion string) (bool, error) {
	for cfg != nil {
		clusterVersion, cas, err := cfg.Get(VERSION_KEY, 0)
		if err != nil {
			return false, err
		}
		if clusterVersion == nil {
			// First time initialization, so save myVersion to cfg and
			// retry in case there was a race.
			_, err = cfg.Set(VERSION_KEY, []byte(myVersion), cas)
			if err != nil {
				log.Printf("error: could not save VERSION to cfg, err: %v", err)
				return false, err
			}
			continue
		}
		if VersionGTE(myVersion, string(clusterVersion)) == false {
			return false, nil
		}
		if myVersion != string(clusterVersion) {
			// Found myVersion is higher than clusterVersion so save
			// myVersion to cfg and retry in case there was a race.
			_, err = cfg.Set(VERSION_KEY, []byte(myVersion), cas)
			if err != nil {
				log.Printf("error: could not save VERSION to cfg, err: %v", err)
				return false, err
			}
			continue
		}
		return true, nil
	}

	return false, nil
}

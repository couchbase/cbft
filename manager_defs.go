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

// JSON/struct definitions of what the Manager stores in the Cfg.
// NOTE: You *must* update VERSION if you change these
// definitions or the planning algorithms change.
const VERSION = "0.0.0"
const VERSION_KEY = "version"

type Plan struct {
}

type LogicalIndex struct {
}

type LogicalIndexes []*LogicalIndex

type Indexer struct {
}

type Indexers []*Indexer

// Returns ok if our version is ok to write to the Cfg.
func CheckVersion(cfg Cfg, myVersion string) (bool, error) {
	for cfg != nil {
		clusterVersion, cas, err := cfg.Get(VERSION_KEY, 0)
		if err != nil {
			return false, err
		}
		if clusterVersion == nil || clusterVersion == "" {
			// First time initialization, so save myVersion to cfg and
			// retry in case there was a race.
			_, err = cfg.Set(VERSION_KEY, myVersion, cas)
			if err != nil {
				log.Printf("error: could not save VERSION to cfg, err: %v", err)
				return false, err
			}
			continue
		}
		if VersionGTE(myVersion, clusterVersion.(string)) == false {
			return false, nil
		}
		if myVersion != clusterVersion {
			// Found myVersion is higher than clusterVersion so save
			// myVersion to cfg and retry in case there was a race.
			_, err = cfg.Set(VERSION_KEY, myVersion, cas)
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

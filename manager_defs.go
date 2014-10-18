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
func (mgr *Manager) CheckVersion() bool {
	for mgr.cfg != nil {
		version, cas, err := mgr.cfg.Get(VERSION_KEY, 0)
		if version == nil || version == "" || err != nil {
			version = VERSION
		}
		if !VersionGTE(VERSION, version.(string)) {
			return false
		}
		if VERSION == version {
			return true
		}
		// We have a higher VERSION than the version just read from
		// cfg, so save our VERSION and retry.
		_, err = mgr.cfg.Set(VERSION_KEY, VERSION, cas)
		if err != nil {
			log.Printf("error: could not save VERSION to cfg, err: %v", err)
			return false
		}
	}

	return false
}

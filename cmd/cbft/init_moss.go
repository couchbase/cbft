//  Copyright (c) 2016 Couchbase, Inc.
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
	"strconv"

	log "github.com/couchbase/clog"

	bleveMoss "github.com/blevesearch/bleve/index/store/moss"

	"github.com/couchbase/cbft"
	"github.com/couchbase/moss"
)

func init() {
	cbft.BlevePIndexAllowMoss = true
}

func InitMossOptions(options map[string]string) (err error) {
	if options == nil {
		return nil
	}

	if bpamv, exists := options["blevePIndexAllowMoss"]; exists {
		bpam, err := strconv.ParseBool(bpamv)
		if err != nil {
			return fmt.Errorf("init_moss:"+
				" parsing blevePIndexAllowMoss: %q, err: %v", bpamv, err)
		}

		cbft.BlevePIndexAllowMoss = bpam
	}

	if !cbft.BlevePIndexAllowMoss {
		return nil
	}

	var memQuota uint64
	v, exists := options["ftsMossMemoryQuota"] // In bytes.
	if !exists {
		v, exists = options["ftsMemoryQuota"] // In bytes.
	}
	if exists {
		fmq, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("init_moss:"+
				" parsing ftsMemoryQuota: %q, err: %v", v, err)
		}
		memQuota = uint64(fmq)
	}

	// TODO: Need to split memory quota between moss and any
	// lower-level storage (e.g., forestdb).

	bleveMoss.RegistryCollectionOptions["fts"] = moss.CollectionOptions{
		Log:     log.Printf,
		OnError: func(err error) { log.Printf("moss OnError, err: %v", err) },
		OnEvent: NewMossHerderOnEvent(memQuota),
	}

	return nil
}

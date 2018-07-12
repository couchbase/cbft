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

func initMossOptions(options map[string]string) (err error) {
	if options == nil {
		return nil
	}

	if bpamv, exists := options["blevePIndexAllowMoss"]; exists {
		var bpam bool
		bpam, err = strconv.ParseBool(bpamv)
		if err != nil {
			return fmt.Errorf("init_moss:"+
				" parsing blevePIndexAllowMoss: %q, err: %v", bpamv, err)
		}

		cbft.BlevePIndexAllowMoss = bpam
	}

	if !cbft.BlevePIndexAllowMoss {
		return nil
	}

	var mossDebug int
	mossDebugV, exists := options["ftsMossDebug"] // Higher means more debug info.
	if exists {
		mossDebug, err = strconv.Atoi(mossDebugV)
		if err != nil {
			return fmt.Errorf("init_moss:"+
				" parsing ftsMossDebug: %q, err: %v", mossDebugV, err)
		}
	}

	bleveMoss.RegistryCollectionOptions["fts"] = moss.CollectionOptions{
		Debug: mossDebug,
		Log:   log.Printf,
		OnError: func(err error) {
			var stackDump string
			if flags.DataDir != "" {
				stackDump = DumpStack(flags.DataDir,
					fmt.Sprintf("moss OnError, treating this as fatal, err: %v", err))
			}
			log.Fatalf("moss OnError, treating this as fatal, err: %v,"+
				" stack dump: %s", err, stackDump)
		},
		OnEvent: ftsHerder.MossHerderOnEvent(),
	}

	return nil
}

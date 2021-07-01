//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"fmt"
	"strconv"

	log "github.com/couchbase/clog"

	bleveMoss "github.com/blevesearch/bleve/v2/index/upsidedown/store/moss"

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

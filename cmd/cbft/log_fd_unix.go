//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// +build linux darwin

package main

import (
	"golang.org/x/sys/unix"

	log "github.com/couchbase/clog"
)

func logFileDescriptorLimit() error {
	var rLimit unix.Rlimit
	err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		return err
	}
	log.Printf("main: file descriptor limit current: %d max: %d", rLimit.Cur, rLimit.Max)
	return nil
}

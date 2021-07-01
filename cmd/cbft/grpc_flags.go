//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"time"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
)

func initGRPCOptions(options map[string]string) error {
	s := options["grpcConnectionIdleTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbft.DefaultGrpcConnectionIdleTimeout = v
	}

	s = options["grpcConnectionHeartBeatInterval"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbft.DefaultGrpcConnectionHeartBeatInterval = v
	}

	s = options["grpcMaxBackOffDelay"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbft.DefaultGrpcMaxBackOffDelay = v
	}

	v, found := cbgt.ParseOptionsInt(options, "grpcMaxRecvMsgSize")
	if found {
		cbft.DefaultGrpcMaxRecvMsgSize = v
	}

	v, found = cbgt.ParseOptionsInt(options, "grpcMaxSendMsgSize")
	if found {
		cbft.DefaultGrpcMaxSendMsgSize = v
	}

	v, found = cbgt.ParseOptionsInt(options, "grpcMaxConcurrentStreams")
	if found {
		cbft.DefaultGrpcMaxConcurrentStreams = uint32(v)
	}

	return nil
}

//  Copyright (c) 2019 Couchbase, Inc.
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

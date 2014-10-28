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
	"fmt"
)

type Stream chan StreamRequest

type StreamPartitionFunc func(StreamRequest, map[string]Stream) (Stream, error)

type StreamRequest interface{}

// ----------------------------------------------

type StreamEnd struct {
	DoneCh chan error
}

// ----------------------------------------------

type StreamFlush struct {
	DoneCh chan error
}

// ----------------------------------------------

type StreamRollback struct {
	DoneCh chan error
}

// ----------------------------------------------

type StreamSnapshot struct {
	DoneCh chan error
}

// ----------------------------------------------

type StreamUpdate struct {
	Id   []byte
	Body []byte
}

// ----------------------------------------------

type StreamDelete struct {
	Id []byte
}

// ----------------------------------------------

// This partition func sends all stream requests to the "" partition.
func EmptyPartitionFunc(req StreamRequest, streams map[string]Stream) (Stream, error) {
	stream, exists := streams[""]
	if !exists || stream == nil {
		return nil, fmt.Errorf("error: no empty/all partition in streams: %#v", streams)
	}
	return stream, nil
}

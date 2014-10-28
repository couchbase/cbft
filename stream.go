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

type Stream chan *StreamRequest

type StreamPartitionFunc func(*StreamRequest, map[string]Stream) (Stream, error)

type StreamRequest struct {
	Op                  int
	DoneCh              chan error
	SourcePartition     string
	SourcePartitionUUID string
	SeqNo               uint64
	Key, Val            []byte
	Misc                interface{}
}

const (
	STREAM_OP_NOOP = iota
	STREAM_OP_UPDATE
	STREAM_OP_DELETE
	STREAM_OP_END
	STREAM_OP_FLUSH
	STREAM_OP_ROLLBACK
	STREAM_OP_SNAPSHOT
)

var StreamOpNames map[int]string

func init() {
	StreamOpNames = map[int]string{
		STREAM_OP_NOOP:     "noop",
		STREAM_OP_UPDATE:   "update",
		STREAM_OP_DELETE:   "delete",
		STREAM_OP_END:      "end",
		STREAM_OP_FLUSH:    "flush",
		STREAM_OP_ROLLBACK: "rollback",
		STREAM_OP_SNAPSHOT: "snapshot",
	}
}

// This partition func sends all stream requests to the "" partition.
func EmptyPartitionFunc(req *StreamRequest, streams map[string]Stream) (Stream, error) {
	stream, exists := streams[""]
	if !exists || stream == nil {
		return nil, fmt.Errorf("error: no empty/all partition in streams: %#v", streams)
	}
	return stream, nil
}

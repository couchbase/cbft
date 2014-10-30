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

type StreamPartitionFunc func(key []byte, partition string,
	streams map[string]Stream) (Stream, error)

type StreamRequest struct {
	Op        int
	DoneCh    chan error
	Partition string
	SeqNo     uint64
	Key, Val  []byte
	Misc      interface{}
}

const (
	STREAM_OP_NOOP = iota
	STREAM_OP_UPDATE
	STREAM_OP_DELETE
	STREAM_OP_FLUSH
	STREAM_OP_ROLLBACK
	STREAM_OP_SNAPSHOT
	STREAM_OP_GET_META // StreamRequest.Misc will be a chan []byte.
	STREAM_OP_SET_META
)

var StreamOpNames map[int]string

func init() {
	StreamOpNames = map[int]string{
		STREAM_OP_NOOP:     "noop",
		STREAM_OP_UPDATE:   "update",
		STREAM_OP_DELETE:   "delete",
		STREAM_OP_FLUSH:    "flush",
		STREAM_OP_ROLLBACK: "rollback",
		STREAM_OP_SNAPSHOT: "snapshot",
	}
}

// This basic partition func first tries a direct lookup by partition
// string, else it tries the "" partition.
func BasicPartitionFunc(key []byte, partition string,
	streams map[string]Stream) (Stream, error) {
	stream, exists := streams[partition]
	if exists {
		return stream, nil
	}
	stream, exists = streams[""]
	if exists {
		return stream, nil
	}
	return nil, fmt.Errorf("error: no stream for key: %s,"+
		" partition: %s, streams: %#v", key, partition, streams)
}

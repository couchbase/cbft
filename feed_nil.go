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

type NILFeed struct {
	name    string
	streams map[string]Stream
}

// A NILFeed never feeds any data to its streams.
func NewNILFeed(name string, streams map[string]Stream) *NILFeed {
	return &NILFeed{
		name:    name,
		streams: streams,
	}
}

func (t *NILFeed) Name() string {
	return t.name
}

func (t *NILFeed) Start() error {
	return nil
}

func (t *NILFeed) Close() error {
	return nil
}

func (t *NILFeed) Streams() map[string]Stream {
	return t.streams
}

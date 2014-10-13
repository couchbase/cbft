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
	"github.com/blevesearch/bleve"
	"sync"
)

type StreamMutation interface {
	Id() []byte
	Body() []byte
}

type StreamUpdate struct {
	id   []byte
	body []byte
}

func (s *StreamUpdate) Id() []byte {
	return s.id
}

func (s *StreamUpdate) Body() []byte {
	return s.body
}

type StreamDelete struct {
	id []byte
}

func (s *StreamDelete) Id() []byte {
	return s.id
}

type StreamMutations chan StreamMutation

func (s *StreamDelete) Body() []byte {
	return nil
}

type Stream interface {
	Channel() StreamMutations
	Start() error
	Close() error
}

var streamRegistry = map[string]Stream{}
var streamRegistryMutex sync.Mutex

func RegisterStream(name string, stream Stream) {
	streamRegistryMutex.Lock()
	defer streamRegistryMutex.Unlock()
	streamRegistry[name] = stream
}

func UnregisterStream(name string) Stream {
	streamRegistryMutex.Lock()
	defer streamRegistryMutex.Unlock()
	rv, ok := streamRegistry[name]
	if ok {
		delete(streamRegistry, name)
		return rv
	}
	return nil
}

func HandleStream(stream Stream, index bleve.Index) {
	ch := stream.Channel()
	for m := range ch {
		switch m := m.(type) {
		case *StreamUpdate:
			index.Index(string(m.Id()), m.Body())
		case *StreamDelete:
			index.Delete(string(m.Id()))
		}
	}
}

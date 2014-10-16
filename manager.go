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
	"sync"

	"github.com/blevesearch/bleve"

	// TODO: manager shouldn't know of bleve/http
	bleveHttp "github.com/blevesearch/bleve/http"

	log "github.com/couchbaselabs/clog"
)

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

func StartRegisteredStream(stream Stream, indexName string, index bleve.Index) error {
	// now start the stream
	go HandleStream(stream, index)
	err := stream.Start()
	if err != nil {
		return err
	}
	// now register the index
	RegisterStream(indexName, stream)
	bleveHttp.RegisterIndexName(indexName, index)
	log.Printf("registered index: %s", indexName)
	return nil
}

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
	"io/ioutil"
	"os"
	"sync"

	"github.com/blevesearch/bleve"

	// TODO: manager shouldn't know of bleve/http?
	bleveHttp "github.com/blevesearch/bleve/http"

	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

type Manager struct {
	dataDir string
	server  string
	m       sync.Mutex
	feeds   map[string]Feed
	streams map[string]Stream
}

func NewManager(dataDir, server string) *Manager {
	return &Manager{
		dataDir: dataDir,
		server:  server,
		streams: make(map[string]Stream),
		feeds:   make(map[string]Feed),
	}
}

func (mgr *Manager) DataDir() string {
	return mgr.dataDir
}

func (mgr *Manager) Start() error {
	// TODO: need different approach that keeps trying even if the
	// server(s) are down, so that we could at least serve queries
	// to old, existing indexes.
	//
	// connect to couchbase, make sure the address is valid

	// TODO: if we handle multiple "seed" servers, what if those
	// seeds actually come from different clusters?  Can we have
	// multiple clusters fan-in to a single cbft?
	_, err := couchbase.Connect(mgr.server)
	if err != nil {
		return fmt.Errorf("error: could not connect to couchbase server URL: %v, err: %v",
			mgr.server, err)
	}

	// walk the data dir and register index names
	dirEntries, err := ioutil.ReadDir(mgr.dataDir)
	if err != nil {
		return fmt.Errorf("error: could not read dataDir: %v, err: %v",
			mgr.dataDir, err)
	}

	for _, dirInfo := range dirEntries {
		indexPath := mgr.dataDir + string(os.PathSeparator) + dirInfo.Name()
		i, err := bleve.Open(indexPath)
		if err != nil {
			log.Printf("error: could not open indexPath: %v, err: %v", indexPath, err)
		} else {
			// make sure there is a bucket with this name
			uuid := "" // TODO: read bucket UUID and vbucket list out of bleve storage.
			stream, err := NewTAPFeed(mgr.server, "default", dirInfo.Name(), uuid)
			if err != nil {
				log.Printf("error: could not prepare TAP stream to server: %v, err: %v",
					server, err)
				// TODO: need a way to collect these errors so REST api
				// can show them to user ("hey, perhaps you deleted a bucket
				// and should delete these related full-text indexes?
				// or the couchbase cluster is just down.")
				continue
			}
			err = mgr.StartRegisteredStream(stream, dirInfo.Name(), i)
			if err != nil {
				log.Printf("error: could not start registered stream, err: %v", err)
				continue
			}
		}
	}

	return nil
}

func (mgr *Manager) RegisterStream(name string, stream Stream) {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	mgr.streams[name] = stream
}

func (mgr *Manager) UnregisterStream(name string) Stream {
	mgr.m.Lock()
	defer mgr.m.Unlock()
	rv, ok := mgr.streams[name]
	if ok {
		delete(mgr.streams, name)
		return rv
	}
	return nil
}

func (mgr *Manager) StartRegisteredStream(stream Stream,
	indexName string, index bleve.Index) error {
	// now start the stream
	err := stream.Start()
	if err != nil {
		return err
	}
	// now register the index
	mgr.RegisterStream(indexName, stream)
	bleveHttp.RegisterIndexName(indexName, index)
	log.Printf("registered index: %s", indexName)
	go HandleStream(stream, index)
	return nil
}

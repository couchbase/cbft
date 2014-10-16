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
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"

	"github.com/gorilla/mux"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"

	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

var bindAddr = flag.String("addr", ":8095",
	"http listen [address]:port")
var dataDir = flag.String("dataDir", "data",
	"directory for index data")
var logFlags = flag.String("logFlags", "",
	"comma-separated clog flags, to control logging")
var staticDir = flag.String("staticDir", "static",
	"directory for static web UI content")
var staticEtag = flag.String("staticEtag", "",
	"static etag value")
var server = flag.String("server", "",
	"url to couchbase server, example: http://localhost:8091")

var expvars = expvar.NewMap("stats")

func init() {
	expvar.Publish("cbft", expvars)
	expvars.Set("indexes", bleveHttp.IndexStats())
}

func main() {
	flag.Parse()

	go dumpOnSignalForPlatform()

	log.Printf("%s started", os.Args[0])
	if *logFlags != "" {
		log.ParseLogFlag(*logFlags)
	}
	flag.VisitAll(func(f *flag.Flag) { log.Printf("  -%s=%s\n", f.Name, f.Value) })
	log.Printf("  GOMAXPROCS=%d", runtime.GOMAXPROCS(-1))

	router, err := mainStart(*dataDir, *staticDir, *server)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", router)
	log.Printf("listening on: %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))
}

func mainStart(dataDir, staticDir, server string) (*mux.Router, error) {
	err := startStreams(dataDir, server)
	if err != nil {
		return nil, err
	}

	return makeRouter(dataDir, staticDir)
}

func startStreams(dataDir, server string) error {
	// TODO: need different approach that keeps trying even if the
	// server(s) are down, so that we could at least serve queries
	// to old, existing indexes.
	//
	// connect to couchbase, make sure the address is valid
	if server == "" {
		return fmt.Errorf("error: couchbase server URL required (-server)")
	}

	// TODO: if we handle multiple "seed" servers, what if those
	// seeds actually come from different clusters?  Can we have
	// multiple clusters fan-in to a single cbft?
	_, err := couchbase.Connect(server)
	if err != nil {
		return fmt.Errorf("error: could not connect to couchbase server URL: %v, err: %v",
			server, err)
	}

	// walk the data dir and register index names
	dirEntries, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("error: could not read dataDir: %v, err: %v", dataDir, err)
	}

	for _, dirInfo := range dirEntries {
		indexPath := dataDir + string(os.PathSeparator) + dirInfo.Name()
		i, err := bleve.Open(indexPath)
		if err != nil {
			log.Printf("error: could not open indexPath: %v, err: %v", indexPath, err)
		} else {
			// make sure there is a bucket with this name
			uuid := "" // TODO: read bucket UUID and vbucket list out of bleve storage.
			stream, err := NewTAPFeed(server, "default", dirInfo.Name(), uuid)
			if err != nil {
				log.Printf("error: could not prepare TAP stream to server: %v, err: %v",
					server, err)
				// TODO: need a way to collect these errors so REST api
				// can show them to user ("hey, perhaps you deleted a bucket
				// and should delete these related full-text indexes?
				// or the couchbase cluster is just down.")
				continue
			}
			err = StartRegisteredStream(stream, dirInfo.Name(), i)
			if err != nil {
				log.Printf("error: could not start registered stream, err: %v", err)
				continue
			}
		}
	}

	return nil
}

func makeRouter(dataDir, staticDir string) (*mux.Router, error) {
	// create a router to serve static files
	router := staticFileRouter(staticDir)

	// add the API

	// these are custom handlers for cbft
	createIndexHandler := NewCreateIndexHander(dataDir)
	router.Handle("/api/{indexName}", createIndexHandler).Methods("PUT")

	deleteIndexHandler := NewDeleteIndexHandler(dataDir)
	router.Handle("/api/{indexName}", deleteIndexHandler).Methods("DELETE")

	// the rest are standard bleveHttp handlers
	getIndexHandler := bleveHttp.NewGetIndexHandler()
	router.Handle("/api/{indexName}", getIndexHandler).Methods("GET")

	listIndexesHandler := bleveHttp.NewListIndexesHander()
	router.Handle("/api", listIndexesHandler).Methods("GET")

	// docIndexHandler := bleveHttp.NewDocIndexHandler("")
	// router.Handle("/api/{indexName}/{docID}", docIndexHandler).Methods("PUT")

	docCountHandler := bleveHttp.NewDocCountHandler("")
	router.Handle("/api/{indexName}/_count", docCountHandler).Methods("GET")

	docGetHandler := bleveHttp.NewDocGetHandler("")
	router.Handle("/api/{indexName}/{docID}", docGetHandler).Methods("GET")

	// docDeleteHandler := bleveHttp.NewDocDeleteHandler("")
	// router.Handle("/api/{indexName}/{docID}", docDeleteHandler).Methods("DELETE")

	searchHandler := bleveHttp.NewSearchHandler("")
	router.Handle("/api/{indexName}/_search", searchHandler).Methods("POST")

	listFieldsHandler := bleveHttp.NewListFieldsHandler("")
	router.Handle("/api/{indexName}/_fields", listFieldsHandler).Methods("GET")

	debugHandler := bleveHttp.NewDebugDocumentHandler("")
	router.Handle("/api/{indexName}/{docID}/_debug", debugHandler).Methods("GET")

	return router, nil
}

func dumpOnSignal(signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for _ = range c {
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	}
}

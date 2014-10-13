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
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"
)

var bindAddr = flag.String("addr", ":8095", "http listen address")
var dataDir = flag.String("dataDir", "data", "data directory")
var staticEtag = flag.String("staticEtag", "", "A static etag value.")
var staticPath = flag.String("static", "static/", "Path to the static content")
var expvars = expvar.NewMap("stats")
var server = flag.String("server", "", "couchbase server address")

func init() {
	expvar.Publish("bleve_explorer", expvars)
}

func main() {
	flag.Parse()

	log.Printf("GOMAXPROCS: %d", runtime.GOMAXPROCS(-1))

	// connect to couchbase, make sure the address is valids
	if *server == "" {
		log.Fatalf("couchbase server address required")
	}
	_, err := couchbase.Connect(*server)
	if err != nil {
		log.Fatal("error connecting to couchbase: %v", err)
	}

	// walk the data dir and register index names
	dirEntries, err := ioutil.ReadDir(*dataDir)
	if err != nil {
		log.Fatalf("error reading data dir: %v", err)
	}

	expvars.Set("indexes", bleveHttp.IndexStats())

	for _, dirInfo := range dirEntries {
		indexPath := *dataDir + string(os.PathSeparator) + dirInfo.Name()
		i, err := bleve.Open(indexPath)
		if err != nil {
			log.Printf("error opening index: %v", err)
		} else {
			// make sure there is a bucket with this name
			stream, err := NewTAPStream(*server, dirInfo.Name())
			if err != nil {
				log.Printf("error preparing tap stream: %v", err)
				continue
			}
			// now start the stream
			go HandleStream(stream, i)
			err = stream.Start()
			if err != nil {
				log.Printf("error starting stream: %v", err)
				continue
			}
			// now register the index
			RegisterStream(dirInfo.Name(), stream)
			log.Printf("registered index: %s", dirInfo.Name())
			bleveHttp.RegisterIndexName(dirInfo.Name(), i)
		}
	}

	// create a router to serve static files
	router := staticFileRouter()

	// add the API

	// these are custom handlers for cbft
	createIndexHandler := NewCreateIndexHander(*dataDir)
	router.Handle("/api/{indexName}", createIndexHandler).Methods("PUT")

	deleteIndexHandler := NewDeleteIndexHandler(*dataDir)
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

	// start the HTTP server
	http.Handle("/", router)
	log.Printf("Listening on %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))
}

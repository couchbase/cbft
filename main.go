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
	_ "expvar"
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/blevesearch/bleve"
	// bleveHttp "github.com/blevesearch/bleve/http"
)

var indexPath = flag.String("index", "index.cbft", "path to index file")
var cpuProfile = flag.String("cpuProfile", "", "write cpu profile to file")
var memProfile = flag.String("memProfile", "", "write mem profile to file")

func main() {
	flag.Parse()

	log.Printf("GOMAXPROCS: %d", runtime.GOMAXPROCS(-1))

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		// TODO: Need to close f somewhere?
	}

	_, err := bleve.Open(*indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		log.Printf("creating new index: %s", *indexPath)
		log.Fatal("TODO: implement creating new index")
	} else if err != nil {
		log.Fatalf("error: could not open index: %s, err: %v", *indexPath, err)
	}

	// TODO: Maintain the index.
	// TODO: Spin up REST.
}

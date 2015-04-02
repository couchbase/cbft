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
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"
	bleveRegistry "github.com/blevesearch/bleve/registry"

	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbaselabs/cbft"
)

var VERSION = "0.0.0"

var bindAddr = flag.String("addr", "localhost:8095",
	"http listen address:port")
var dataDir = flag.String("dataDir", "data",
	"directory path where index data and\n"+
		"\tlocal configuration files will be stored")
var logFlags = flag.String("logFlags", "",
	"comma-separated logging control flags")
var staticDir = flag.String("staticDir", "static",
	"directory for static web UI content")
var staticETag = flag.String("staticETag", "",
	"static etag value")
var server = flag.String("server", "",
	"url to datasource server;\n"+
		"\texample for couchbase: http://localhost:8091")
var tags = flag.String("tags", "",
	"comma-separated list of tags (or roles) for this node")
var container = flag.String("container", "",
	"slash separated path of parent containers for this node,\n"+
		"\tfor shelf/rack/row/zone awareness")
var weight = flag.Int("weight", 1,
	"weight of this node (a more capable node has higher weight)")
var register = flag.String("register", "wanted",
	"register this node as wanted, wantedForce,\n\t"+
		" known, knownForce,"+
		" unwanted, unknown or unchanged")
var cfgConnect = flag.String("cfgConnect", "simple",
	"connection string/info to configuration provider")

var expvars = expvar.NewMap("stats")

func init() {
	expvars.Set("indexes", bleveHttp.IndexStats())
}

func main() {
	flag.Parse()

	log.Printf("main: %s started (%s/%s)", os.Args[0], VERSION, cbft.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go dumpOnSignalForPlatform()

	mr, err := cbft.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		log.Fatalf("main: could not create MsgRing, err: %v", err)
	}
	log.SetOutput(mr)

	MainWelcome()

	// TODO: If cfg goes down, should we stop?  How do we reconnect?
	//
	cfg, err := MainCfg(*cfgConnect, *dataDir)
	if err != nil {
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v",
			*cfgConnect, err)
		return
	}

	uuid, err := MainUUID(*dataDir)
	if err != nil {
		log.Fatalf(fmt.Sprintf("%v", err))
		return
	}

	var tagsArr []string
	if *tags != "" {
		tagsArr = strings.Split(*tags, ",")
	}

	router, err := MainStart(cfg, uuid, tagsArr, *container, *weight,
		*bindAddr, *dataDir, *staticDir, *staticETag, *server, *register, mr)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", router)
	log.Printf("main: listening on: %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))
}

func MainWelcome() {
	if *logFlags != "" {
		log.ParseLogFlag(*logFlags)
	}
	flag.VisitAll(func(f *flag.Flag) {
		log.Printf("  -%s=%s\n", f.Name, f.Value)
	})
	log.Printf("  GOMAXPROCS=%d", runtime.GOMAXPROCS(-1))

	log.Printf("main: registered bleve stores")
	types, instances := bleveRegistry.KVStoreTypesAndInstances()
	for _, s := range types {
		log.Printf("  %s", s)
	}
	for _, s := range instances {
		log.Printf("  %s", s)
	}
}

func MainUUID(dataDir string) (string, error) {
	uuid := cbft.NewUUID()
	uuidPath := dataDir + string(os.PathSeparator) + "cbft.uuid"
	uuidBuf, err := ioutil.ReadFile(uuidPath)
	if err == nil {
		uuid = strings.TrimSpace(string(uuidBuf))
		if uuid == "" {
			return "", fmt.Errorf("error: could not parse uuidPath: %s",
				uuidPath)
		}
		log.Printf("main: manager uuid: %s", uuid)
		log.Printf("main: manager uuid was reloaded")
	} else {
		log.Printf("main: manager uuid: %s", uuid)
		log.Printf("main: manager uuid was generated")
	}
	err = ioutil.WriteFile(uuidPath, []byte(uuid), 0600)
	if err != nil {
		return "", fmt.Errorf("error: could not write uuidPath: %s", uuidPath)
	}
	return uuid, nil
}

func MainStart(cfg cbft.Cfg, uuid string, tags []string, container string,
	weight int, bindAddr, dataDir, staticDir, staticETag, server string,
	register string, mr *cbft.MsgRing) (
	*mux.Router, error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	_, err := couchbase.Connect(server)
	if err != nil {
		return nil, fmt.Errorf("error: could not connect to server,"+
			" URL: %s, err: %v",
			server, err)
	}

	mgr := cbft.NewManager(cbft.VERSION, cfg, uuid, tags, container, weight,
		bindAddr, dataDir, server, &MainHandlers{})
	err = mgr.Start(register)
	if err != nil {
		return nil, err
	}

	router, _, err :=
		cbft.NewManagerRESTRouter(VERSION, mgr, staticDir, staticETag, mr)

	return router, err
}

type MainHandlers struct{}

func (meh *MainHandlers) OnRegisterPIndex(pindex *cbft.PIndex) {
	bindex, ok := pindex.Impl.(bleve.Index)
	if ok {
		bleveHttp.RegisterIndexName(pindex.Name, bindex)
	}
}

func (meh *MainHandlers) OnUnregisterPIndex(pindex *cbft.PIndex) {
	bleveHttp.UnregisterIndexByName(pindex.Name)
}

func dumpOnSignal(signals ...os.Signal) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, signals...)
	for _ = range c {
		log.Printf("dump: goroutine...")
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		log.Printf("dump: heap...")
		pprof.Lookup("heap").WriteTo(os.Stderr, 1)
	}
}

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

	log "github.com/couchbaselabs/clog"
	"github.com/couchbaselabs/go-couchbase"
)

var bindAddr = flag.String("addr", "localhost:8095",
	"http listen address:port")
var dataDir = flag.String("dataDir", "data",
	"directory for local configuration and index data")
var logFlags = flag.String("logFlags", "",
	"comma-separated clog flags, to control logging")
var staticDir = flag.String("staticDir", "static",
	"directory for static web UI content")
var staticEtag = flag.String("staticEtag", "",
	"static etag value")
var server = flag.String("server", "",
	"url to datasource server; couchbase example: http://localhost:8091")
var tags = flag.String("tags", "",
	"comma-separated list of tags (or roles) for this node")
var container = flag.String("container", "",
	"slash separated path of parent containers for this node,"+
		" for shelf/rack/row/zone awareness")
var weight = flag.Int("weight", 1,
	"weight of this node (a more capable node has higher weight)")
var register = flag.String("register", "wanted",
	"register this node as wanted, wantedForce, known, knownForce or notRegistered")
var cfgConnect = flag.String("cfgConnect", "simple",
	"connection string/info to configuration provider")

var expvars = expvar.NewMap("stats")

func init() {
	expvar.Publish("cbft", expvars)
	expvars.Set("indexes", bleveHttp.IndexStats())
}

func main() {
	flag.Parse()

	rand.Seed(time.Now().UTC().UnixNano())

	go dumpOnSignalForPlatform()

	mr, err := NewMsgRing(os.Stderr, 1000)
	if err != nil {
		log.Fatalf("error: could not create MsgRing, err: %v", err)
	}
	log.SetOutput(mr)

	MainWelcome()

	// TODO: If cfg goes down, should we stop?  How do we reconnect?
	//
	cfg, err := MainCfg(*cfgConnect, *dataDir)
	if err != nil {
		log.Fatalf("error: could not start cfg, cfgConnect: %s, err: %v",
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
		*bindAddr, *dataDir, *staticDir, *server, *register, mr)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", router)
	log.Printf("listening on: %v", *bindAddr)
	log.Fatal(http.ListenAndServe(*bindAddr, nil))
}

func MainWelcome() {
	log.Printf("%s started", os.Args[0])
	if *logFlags != "" {
		log.ParseLogFlag(*logFlags)
	}
	flag.VisitAll(func(f *flag.Flag) { log.Printf("  -%s=%s\n", f.Name, f.Value) })
	log.Printf("  GOMAXPROCS=%d", runtime.GOMAXPROCS(-1))
}

func MainUUID(dataDir string) (string, error) {
	uuid := NewUUID()
	uuidPath := dataDir + string(os.PathSeparator) + "cbft.uuid"
	uuidBuf, err := ioutil.ReadFile(uuidPath)
	if err == nil {
		uuid = strings.TrimSpace(string(uuidBuf))
		if uuid == "" {
			return "", fmt.Errorf("error: could not parse uuidPath: %s", uuidPath)
		}
		log.Printf("manager uuid: %s", uuid)
		log.Printf("manager uuid was reloaded")
	} else {
		log.Printf("manager uuid: %s", uuid)
		log.Printf("manager uuid was generated")
	}
	err = ioutil.WriteFile(uuidPath, []byte(uuid), 0600)
	if err != nil {
		return "", fmt.Errorf("error: could not write uuidPath: %s", uuidPath)
	}
	return uuid, nil
}

func MainStart(cfg Cfg, uuid string, tags []string, container string,
	weight int, bindAddr, dataDir, staticDir, server string,
	register string, mr *MsgRing) (
	*mux.Router, error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	_, err := couchbase.Connect(server)
	if err != nil {
		return nil, fmt.Errorf("error: could not connect to server URL: %s, err: %v",
			server, err)
	}

	mgr := NewManager(VERSION, cfg, uuid, tags, container, weight,
		bindAddr, dataDir, server, &MainHandlers{})
	if err = mgr.Start(register); err != nil {
		return nil, err
	}

	return NewManagerRESTRouter(mgr, staticDir, mr)
}

type MainHandlers struct{}

func (meh *MainHandlers) OnRegisterPIndex(pindex *PIndex) {
	bindex, ok := pindex.Impl.(bleve.Index)
	if ok {
		bleveHttp.RegisterIndexName(pindex.Name, bindex)
	}
}

func (meh *MainHandlers) OnUnregisterPIndex(pindex *PIndex) {
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

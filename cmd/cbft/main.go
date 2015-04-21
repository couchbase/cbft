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
	"path"
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

var VERSION = "v0.0.3"

var expvars = expvar.NewMap("stats")

func main() {
	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	if flags.Version {
		fmt.Printf("%s main: %s, data: %s\n", path.Base(os.Args[0]),
			VERSION, cbft.VERSION)
		os.Exit(0)
	}

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	mr, err := cbft.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		log.Fatalf("main: could not create MsgRing, err: %v", err)
	}
	log.SetOutput(mr)

	log.Printf("main: %s started (%s/%s)",
		os.Args[0], VERSION, cbft.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go dumpOnSignalForPlatform()

	MainWelcome(flagAliases)

	// If cfg is down, we error, leaving it to some user-supplied
	// outside watchdog to backoff and restart/retry.
	cfg, err := MainCfg(flags.CfgConnect, flags.DataDir)
	if err != nil {
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%q)\n"+
			"  is correct and/or that your configuration provider\n"+
			"  is available.",
			flags.CfgConnect, err, flags.CfgConnect)
		return
	}

	uuid, err := MainUUID(flags.DataDir)
	if err != nil {
		log.Fatalf(fmt.Sprintf("%v", err))
		return
	}

	var tagsArr []string
	if flags.Tags != "" {
		tagsArr = strings.Split(flags.Tags, ",")
	}

	expvars.Set("indexes", bleveHttp.IndexStats())

	router, err := MainStart(cfg, uuid, tagsArr,
		flags.Container, flags.Weight,
		flags.BindHttp, flags.DataDir,
		flags.StaticDir, flags.StaticETag,
		flags.Server, flags.Register, mr)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", router)

	log.Printf("main: listening on: %s", flags.BindHttp)
	u := flags.BindHttp
	if u[0] == ':' {
		u = "localhost" + u
	}
	if strings.HasPrefix(u, "0.0.0.0:") {
		u = "localhost" + u[len("0.0.0.0"):]
	}
	log.Printf("------------------------------------------------------------")
	log.Printf("web UI / REST API is available: http://%s", u)
	log.Printf("------------------------------------------------------------")
	err = http.ListenAndServe(flags.BindHttp, nil)
	if err != nil {
		log.Fatalf("main: listen, err: %v\n"+
			"  Please check that your -bindHttp parameter (%q)\n"+
			"  is correct and available.", err, flags.BindHttp)
	}
}

func MainWelcome(flagAliases map[string][]string) {
	flag.VisitAll(func(f *flag.Flag) {
		if flagAliases[f.Name] != nil {
			log.Printf("  -%s=%q\n", f.Name, f.Value)
		}
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
		return "", fmt.Errorf("error: could not write uuidPath: %s\n"+
			"  Please check that your -data/-dataDir parameter (%q)\n"+
			"  is to a writable directory where cbft can store\n"+
			"  index data.",
			uuidPath, dataDir)
	}
	return uuid, nil
}

func MainStart(cfg cbft.Cfg, uuid string, tags []string, container string,
	weight int, bindHttp, dataDir, staticDir, staticETag, server string,
	register string, mr *cbft.MsgRing) (
	*mux.Router, error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	if server != "." {
		_, err := couchbase.Connect(server)
		if err != nil {
			return nil, fmt.Errorf("error: could not connect to server,"+
				" URL: %s, err: %v\n"+
				"  Please check that your -server parameter (%q)\n"+
				"  is correct and the server is available.",
				server, err, server)
		}
	}

	mgr := cbft.NewManager(cbft.VERSION, cfg, uuid, tags, container, weight,
		bindHttp, dataDir, server, &MainHandlers{})
	err := mgr.Start(register)
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

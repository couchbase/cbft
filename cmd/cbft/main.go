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
	"sort"
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

var expvars = expvar.NewMap("stats")

type Flags struct {
	BindAddr   string
	DataDir    string
	Help       bool
	LogFlags   string
	StaticDir  string
	StaticETag string
	Server     string
	Tags       string
	Container  string
	Version    bool
	Weight     int
	Register   string
	CfgConnect string
}

var flags Flags
var flagAliases map[string][]string

func init() {
	flagAliases = initFlags(&flags)
}

func initFlags(flags *Flags) map[string][]string {
	flagAliases := map[string][]string{} // main flag name => all aliases.

	s := func(v *string, names []string,
		defaultVal, usage string) { // String cmd-line param.
		for _, name := range names {
			flag.StringVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
	}

	i := func(v *int, names []string,
		defaultVal int, usage string) { // Integer cmd-line param.
		for _, name := range names {
			flag.IntVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
	}

	b := func(v *bool, names []string,
		defaultVal bool, usage string) { // Bool cmd-line param.
		for _, name := range names {
			flag.BoolVar(v, name, defaultVal, usage)
		}
		flagAliases[names[0]] = names
	}

	s(&flags.BindAddr,
		[]string{"bindAddr"}, "localhost:8095",
		"http listen address:port")
	s(&flags.DataDir,
		[]string{"dataDir", "data"}, "data",
		"directory path where index data and"+
			"\nlocal configuration files will be stored")
	b(&flags.Help,
		[]string{"help", "?", "H", "h"}, false,
		"print this usage message and exit")
	s(&flags.LogFlags,
		[]string{"logFlags"}, "",
		"comma-separated logging control flags")
	s(&flags.StaticDir,
		[]string{"staticDir"}, "static",
		"directory for static web UI content")
	s(&flags.StaticETag,
		[]string{"staticETag"}, "",
		"static etag value")
	s(&flags.Server,
		[]string{"server"}, "",
		"url to datasource server;"+
			"\nexample for couchbase: http://localhost:8091")
	s(&flags.Tags,
		[]string{"tags"}, "",
		"comma-separated list of tags (or roles) for this node")
	s(&flags.Container,
		[]string{"container"}, "",
		"slash separated path of parent containers for this node,"+
			"\nfor shelf/rack/row/zone awareness")
	b(&flags.Version,
		[]string{"version", "v"}, false,
		"print version string and exit")
	i(&flags.Weight,
		[]string{"weight"}, 1,
		"weight of this node (a more capable node has higher weight)")
	s(&flags.Register,
		[]string{"register"}, "wanted",
		"register this node as wanted, wantedForce,"+
			"\nknown, knownForce, unwanted, unknown or unchanged")
	s(&flags.CfgConnect,
		[]string{"cfgConnect", "cfg"}, "simple",
		"connection string/info to configuration provider")

	flag.Usage = func() {
		if !flags.Help {
			return
		}

		base := path.Base(os.Args[0])

		fmt.Fprintf(os.Stderr,
			"%s: couchbase full-text server\n\n", base)
		fmt.Fprintf(os.Stderr, "more information is available at:\n"+
			"  http://github.com/couchbaselabs/cbft\n\n")
		fmt.Fprintf(os.Stderr, "usage:\n  %s [flags]\n\n", base)
		fmt.Fprintf(os.Stderr, "flags:\n")

		flagsByName := map[string]*flag.Flag{}
		flag.VisitAll(func(f *flag.Flag) {
			flagsByName[f.Name] = f
		})

		flags := []string(nil)
		for name := range flagAliases {
			flags = append(flags, name)
		}
		sort.Strings(flags)

		for _, name := range flags {
			aliases := flagAliases[name]
			a := []string(nil)
			for i := len(aliases) - 1; i >= 0; i-- {
				a = append(a, aliases[i])
			}
			f := flagsByName[name]
			fmt.Fprintf(os.Stderr, "  -%s=%q\n",
				strings.Join(a, ", -"), f.Value)
			fmt.Fprintf(os.Stderr, "      %s\n",
				strings.Join(strings.Split(f.Usage, "\n"),
					"\n      "))
		}
	}

	return flagAliases
}

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

	log.Printf("main: %s started (%s/%s)",
		os.Args[0], VERSION, cbft.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go dumpOnSignalForPlatform()

	mr, err := cbft.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		log.Fatalf("main: could not create MsgRing, err: %v", err)
	}
	log.SetOutput(mr)

	if flags.LogFlags != "" {
		log.ParseLogFlag(flags.LogFlags)
	}

	MainWelcome(flagAliases)

	// If cfg is down, we error, leaving it to some user-supplied
	// outside watchdog to backoff and restart/retry.
	cfg, err := MainCfg(flags.CfgConnect, flags.DataDir)
	if err != nil {
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%s)\n"+
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
		flags.BindAddr, flags.DataDir,
		flags.StaticDir, flags.StaticETag,
		flags.Server, flags.Register, mr)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", router)

	log.Printf("main: listening on: %v", flags.BindAddr)
	err = http.ListenAndServe(flags.BindAddr, nil)
	if err != nil {
		log.Fatalf("main: listen, err: %v\n"+
			"  Please check that your -bindAddr parameter (%s)\n"+
			"  is correct and available.", err, flags.BindAddr)
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
			"  Please check that your -data/-dataDir parameter (%s)\n"+
			"  is to a writable directory.", uuidPath, dataDir)
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

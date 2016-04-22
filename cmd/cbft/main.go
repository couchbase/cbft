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
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"
	bleveRegistry "github.com/blevesearch/bleve/registry"
	bleveSearchers "github.com/blevesearch/bleve/search/searchers"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/ctl"
	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/goutils/go-cbaudit"
	"github.com/couchbase/goutils/platform"
)

var cmdName = "cbft"

var VERSION = "v0.3.1"

var DefaultCtlVerbose = 3

var expvars = expvar.NewMap("stats")

func init() {
	cbgt.DCPFeedPrefix = "fts:"

	cbgt.CfgMetaKvPrefix = "/fts/cbgt/cfg/"
}

func main() {
	platform.HideConsole(true)
	defer platform.HideConsole(false)

	flag.Parse()

	if flags.Help {
		flag.Usage()
		os.Exit(2)
	}

	if flags.Version {
		fmt.Printf("%s main: %s, data: %s\n", path.Base(os.Args[0]),
			VERSION, cbgt.VERSION)
		os.Exit(0)
	}

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}

	mr, err := cbgt.NewMsgRing(os.Stderr, 1000)
	if err != nil {
		log.Fatalf("main: could not create MsgRing, err: %v", err)
	}
	log.SetOutput(mr)
	log.SetLoggerCallback(LoggerFunc)

	log.Printf("main: %s started (%s/%s)",
		os.Args[0], VERSION, cbgt.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go cmd.DumpOnSignalForPlatform()

	bleve.StoreDynamic = false
	bleve.MappingJSONStrict = true
	bleveSearchers.DisjunctionMaxClauseCount = 1024

	MainWelcome(flagAliases)

	s, err := os.Stat(flags.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			if flags.DataDir == DEFAULT_DATA_DIR {
				log.Printf("main: creating data directory, dataDir: %s",
					flags.DataDir)
				err = os.Mkdir(flags.DataDir, 0700)
				if err != nil {
					log.Fatalf("main: could not create data directory,"+
						" dataDir: %s, err: %v", flags.DataDir, err)
				}
			} else {
				log.Fatalf("main: data directory does not exist,"+
					" dataDir: %s", flags.DataDir)
				return
			}
		} else {
			log.Fatalf("main: could not access data directory,"+
				" dataDir: %s, err: %v", flags.DataDir, err)
			return
		}
	} else {
		if !s.IsDir() {
			log.Fatalf("main: not a directory, dataDir: %s", flags.DataDir)
			return
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("main: os.Getwd, err: %#v", err)
		return
	}
	log.Printf("main: curr dir: %q", wd)

	dataDirAbs, err := filepath.Abs(flags.DataDir)
	if err != nil {
		log.Fatalf("main: filepath.Abs, err: %#v", err)
		return
	}
	log.Printf("main: data dir: %q", dataDirAbs)

	uuid := flags.UUID
	if uuid != "" {
		uuidPath :=
			flags.DataDir + string(os.PathSeparator) + cmdName + ".uuid"

		err = ioutil.WriteFile(uuidPath, []byte(uuid), 0600)
		if err != nil {
			log.Fatalf("main: could not write uuidPath: %s\n"+
				"  Please check that your -data/-dataDir parameter (%q)\n"+
				"  is to a writable directory where %s can persist data.",
				uuidPath, flags.DataDir, cmdName)
			return
		}
	}

	if uuid == "" {
		uuid, err = cmd.MainUUID(cmdName, flags.DataDir)
		if err != nil {
			log.Fatalf("%v", err)
			return
		}
	}

	options := cmd.ParseOptions(flags.Options, "CBFT_ENV_OPTIONS",
		map[string]string{
			"managerLoadDataDir": "async",
			"authType":           flags.AuthType,
		})

	// User may supply a comma-separated list of HOST:PORT values for
	// http addresss/port listening, but only the first entry is used
	// for cbgt node and Cfg registration.
	bindHttps := strings.Split(flags.BindHttp, ",")

	// If cfg is down, we error, leaving it to some user-supplied
	// outside watchdog to backoff and restart/retry.
	cfg, err := cmd.MainCfgEx(cmdName, flags.CfgConnect,
		bindHttps[0], flags.Register, flags.DataDir, uuid, options)
	if err != nil {
		if err == cmd.ErrorBindHttp {
			log.Fatalf("%v", err)
			return
		}
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%q)\n"+
			"  is correct and/or that your configuration provider\n"+
			"  is available.",
			flags.CfgConnect, err, flags.CfgConnect)
		return
	}

	var tagsArr []string
	if flags.Tags != "" {
		tagsArr = strings.Split(flags.Tags, ",")
	}

	router, err := MainStart(cfg, uuid, tagsArr,
		flags.Container, flags.Weight, flags.Extras,
		bindHttps[0], flags.DataDir,
		flags.StaticDir, flags.StaticETag,
		flags.Server, flags.Register, mr, options)
	if err != nil {
		log.Fatal(err)
	}

	if flags.Register == "unknown" {
		log.Printf("main: unregistered node; now exiting")
		os.Exit(0)
	}

	http.Handle("/", router)

	anyHostPorts := map[string]bool{}

	// Bind to 0.0.0.0's first.
	for _, bindHttp := range bindHttps {
		if strings.HasPrefix(bindHttp, "0.0.0.0:") {
			go MainServeHttp(bindHttp, nil)

			anyHostPorts[bindHttp] = true
		}
	}

	for i := len(bindHttps) - 1; i >= 1; i-- {
		go MainServeHttp(bindHttps[i], anyHostPorts)
	}

	MainServeHttp(bindHttps[0], anyHostPorts)

	<-(make(chan struct{})) // Block forever.
}

func MainServeHttp(bindHttp string, anyHostPorts map[string]bool) {
	if bindHttp[0] == ':' {
		bindHttp = "localhost" + bindHttp
	}

	bar := "------------------------------------------------------------"

	if anyHostPorts != nil {
		// If we've already bound to 0.0.0.0 on the same port, then
		// skip this hostPort.
		hostPort := strings.Split(bindHttp, ":")
		if len(hostPort) >= 2 {
			anyHostPort := "0.0.0.0:" + hostPort[1]
			if anyHostPorts[anyHostPort] {
				if anyHostPort != bindHttp {
					log.Printf(bar)
					log.Printf("web UI / REST API is available"+
						" (via 0.0.0.0): http://%s", bindHttp)
					log.Printf(bar)
				}
				return
			}
		}
	}

	log.Printf(bar)
	log.Printf("web UI / REST API is available: http://%s", bindHttp)
	log.Printf(bar)

	err := http.ListenAndServe(bindHttp, nil) // Blocks.
	if err != nil {
		log.Fatalf("main: listen, err: %v\n"+
			"  Please check that your -bindHttp parameter (%q)\n"+
			"  is correct and available.", err, bindHttp)
	}
}

func LoggerFunc(level, format string, args ...interface{}) string {
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	prefix := ts + " [" + level + "] "
	if format != "" {
		return prefix + fmt.Sprintf(format, args...)
	}
	return prefix + fmt.Sprint(args...)
}

func MainWelcome(flagAliases map[string][]string) {
	cmd.LogFlags(flagAliases)

	log.Printf("main: registered bleve stores")
	types, instances := bleveRegistry.KVStoreTypesAndInstances()
	for _, s := range types {
		log.Printf("  %s", s)
	}
	for _, s := range instances {
		log.Printf("  %s", s)
	}
}

func MainStart(cfg cbgt.Cfg, uuid string, tags []string, container string,
	weight int, extras, bindHttp, dataDir, staticDir, staticETag, server string,
	register string, mr *cbgt.MsgRing, options map[string]string) (
	*mux.Router, error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	err := InitOptions(options)
	if err != nil {
		return nil, err
	}

	err = InitMossOptions(options)
	if err != nil {
		return nil, err
	}

	if options["bleveMaxOpsPerBatch"] != "" {
		bleveMaxOpsPerBatch, err := strconv.Atoi(options["bleveMaxOpsPerBatch"])
		if err != nil {
			return nil, err
		}
		cbft.BleveMaxOpsPerBatch = bleveMaxOpsPerBatch
	}

	if options["logStatsEvery"] != "" {
		logStatsEvery, err := strconv.Atoi(options["logStatsEvery"])
		if err != nil {
			return nil, err
		}
		if logStatsEvery >= 0 {
			cbft.LogEveryNStats = logStatsEvery
		}
	}

	exitCode := MainTool(cfg, uuid, tags, flags, options)
	if exitCode >= 0 {
		os.Exit(exitCode)
	}

	if server != "." && options["startCheckServer"] != "skip" {
		_, err := couchbase.Connect(server)
		if err != nil {
			if !strings.HasPrefix(server, "http://") &&
				!strings.HasPrefix(server, "https://") {
				return nil, fmt.Errorf("error: not a URL, server: %q\n"+
					"  Please check that your -server parameter"+
					" is a valid URL\n"+
					"  (http://HOST:PORT),"+
					" such as \"http://localhost:8091\",\n"+
					"  to a couchbase server",
					server)
			}

			return nil, fmt.Errorf("error: could not connect"+
				" to server (%q), err: %v\n"+
				"  Please check that your -server parameter (%q)\n"+
				"  is correct, the couchbase server is accessible,\n"+
				"  and auth is correct (e.g., http://USER:PSWD@HOST:PORT)",
				server, err, server)
		}
	}

	meh := &MainHandlers{}
	mgr := cbgt.NewManagerEx(cbgt.VERSION, cfg,
		uuid, tags, container, weight,
		extras, bindHttp, dataDir, server, meh, options)
	meh.mgr = mgr

	err = mgr.Start(register)
	if err != nil {
		return nil, err
	}

	var adtSvc *audit.AuditSvc
	if options["cbaudit"] == "true" {
		adtSvc, _ = audit.NewAuditSvc(server)
	}

	router, _, err :=
		cbft.NewRESTRouter(VERSION, mgr, staticDir, staticETag, mr,
			adtSvc)

	// register handlers needed by ns_server
	prefix := mgr.Options()["urlPrefix"]

	router.Handle(prefix+"/api/nsstats", cbft.NewNsStatsHandler(mgr))

	nsStatusHandler, err := cbft.NewNsStatusHandler(mgr, server)
	if err != nil {
		return nil, err
	}
	router.Handle(prefix+"/api/nsstatus", nsStatusHandler)

	nsSearchResultRedirectHandler, err := cbft.NsSearchResultRedirctHandler(mgr)
	if err != nil {
		return nil, err
	}
	router.Handle(prefix+"/api/nsSearchResultRedirect/{pIndexName}/{docID}", nsSearchResultRedirectHandler)

	cbAuthBasicLoginHadler, err := cbft.CBAuthBasicLoginHandler(mgr)
	if err != nil {
		return nil, err
	}
	router.Handle(prefix+"/login", cbAuthBasicLoginHadler)

	// ------------------------------------------------

	tagsMap := mgr.TagsMap()
	if tagsMap != nil && tagsMap["cbauth_service"] {
		dryRun := false
		dryRunV := mgr.Options()["cbauth_service.dryRun"]
		if dryRunV != "" {
			dryRun, err = strconv.ParseBool(dryRunV)
			if err != nil {
				return nil, err
			}
		}

		waitForMemberNodes := 30 // In seconds.
		waitForMemberNodesV := mgr.Options()["cbauth_service.waitForMemberNodes"]
		if waitForMemberNodesV != "" {
			waitForMemberNodes, err = strconv.Atoi(waitForMemberNodesV)
			if err != nil {
				return nil, err
			}
		}

		verbose := DefaultCtlVerbose
		verboseV := mgr.Options()["cbauth_service.verbose"]
		if verboseV != "" {
			verbose, err = strconv.Atoi(verboseV)
			if err != nil {
				return nil, err
			}
		}

		log.Printf("main: cbauth_service ctl starting,"+
			" dryRun: %v, waitForMemberNodes: %d, verbose: %d",
			dryRun, waitForMemberNodes, verbose)

		c, err := ctl.StartCtl(cfg, server, mgr.Options(), ctl.CtlOptions{
			DryRun:             dryRun,
			Verbose:            verbose,
			FavorMinNodes:      false,
			WaitForMemberNodes: waitForMemberNodes,
		})
		if err != nil {
			return nil, fmt.Errorf("main: ctl.StartCtl, err: %v", err)
		}

		nodeInfo := &service.NodeInfo{
			NodeID: service.NodeID(uuid),
		}

		err = cfg.Refresh()
		if err != nil {
			return nil, err
		}

		ctlMgr := ctl.NewCtlMgr(nodeInfo, c)
		if ctlMgr != nil {
			go func() {
				log.Printf("main: cbauth_service registering...")

				err := service.RegisterManager(ctlMgr, nil)
				if err != nil {
					log.Printf("main: cbauth_service register err: %v", err)
					return
				}

				log.Printf("main: cbauth_service registering... done")
			}()
		}
	}

	// ------------------------------------------------

	return router, err
}

// -------------------------------------------------------

type MainHandlers struct {
	mgr *cbgt.Manager
}

func (meh *MainHandlers) OnRegisterPIndex(pindex *cbgt.PIndex) {
	bindex, ok := pindex.Impl.(bleve.Index)
	if ok {
		bleveHttp.RegisterIndexName(pindex.Name, bindex)
		bindex.SetName(pindex.Name)
	}
}

func (meh *MainHandlers) OnUnregisterPIndex(pindex *cbgt.PIndex) {
	bleveHttp.UnregisterIndexByName(pindex.Name)
}

func (meh *MainHandlers) OnFeedError(srcType string, r cbgt.Feed, err error) {
	log.Printf("main: meh.OnFeedError, srcType: %s, err: %v", srcType, err)

	if _, ok := err.(*couchbase.BucketNotFoundError); !ok ||
		srcType != "couchbase" || r == nil {
		return
	}

	dcpFeed, ok := r.(*cbgt.DCPFeed)
	if !ok {
		return
	}

	gone, err := dcpFeed.VerifyBucketNotExists()
	log.Printf("main: meh.OnFeedError, VerifyBucketNotExists,"+
		" srcType: %s, gone: %t, err: %v", srcType, gone, err)
	if !gone {
		return
	}

	bucketName, bucketUUID := dcpFeed.GetBucketDetails()
	if bucketName == "" {
		return
	}

	log.Printf("main: meh.OnFeedError, DeleteAllIndexFromSource,"+
		" srcType: %s, bucketName: %s, bucketUUID: %s",
		srcType, bucketName, bucketUUID)

	meh.mgr.DeleteAllIndexFromSource(srcType, bucketName, bucketUUID)
}

//  Copyright (c) 2017 Couchbase, Inc.
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
	"encoding/json"
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
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/blevesearch/bleve"
	bleveHttp "github.com/blevesearch/bleve/http"
	bleveRegistry "github.com/blevesearch/bleve/registry"

	"github.com/couchbase/cbauth"
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

var version = "v0.5.0"

var defaultCtlVerbose = 3

var expvars = expvar.NewMap("stats")

// List of active https servers
var httpsServers []*http.Server
var httpsServersMutex sync.Mutex

// http router in use
var routerInUse *mux.Router

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
			version, cbgt.VERSION)
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
	log.SetLoggerCallback(loggerFunc)

	log.Printf("main: %s started (%s/%s)",
		os.Args[0], version, cbgt.VERSION)

	rand.Seed(time.Now().UTC().UnixNano())

	go cmd.DumpOnSignalForPlatform()

	mainWelcome(flagAliases)

	s, err := os.Stat(flags.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			if flags.DataDir == defaultDataDir {
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
			}
		} else {
			log.Fatalf("main: could not access data directory,"+
				" dataDir: %s, err: %v", flags.DataDir, err)
		}
	} else {
		if !s.IsDir() {
			log.Fatalf("main: not a directory, dataDir: %s", flags.DataDir)
		}
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("main: os.Getwd, err: %#v", err)
	}
	log.Printf("main: curr dir: %q", wd)

	dataDirAbs, err := filepath.Abs(flags.DataDir)
	if err != nil {
		log.Fatalf("main: filepath.Abs, err: %#v", err)
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
		}
	}

	if uuid == "" {
		uuid, err = cmd.MainUUID(cmdName, flags.DataDir)
		if err != nil {
			log.Fatalf("%v", err)
		}
	}

	options := cmd.ParseOptions(flags.Options, "CBFT_ENV_OPTIONS",
		map[string]string{
			cbgt.FeedAllotmentOption: cbgt.FeedAllotmentOnePerPIndex,
			"managerLoadDataDir":     "async",
			"authType":               flags.AuthType,
		})

	err = initHTTPOptions(options)
	if err != nil {
		log.Fatalf("main: InitHttpOptions, err: %v", err)
	}

	// User may supply a comma-separated list of HOST:PORT values for
	// http addresss/port listening, but only the first http entry
	// is used for cbgt node and Cfg registration.
	bindHTTPList := strings.Split(flags.BindHTTP, ",")

	// If cfg is down, we error, leaving it to some user-supplied
	// outside watchdog to backoff and restart/retry.
	cfg, err := cmd.MainCfgEx(cmdName, flags.CfgConnect,
		bindHTTPList[0], flags.Register, flags.DataDir, uuid, options)
	if err != nil {
		if err == cmd.ErrorBindHttp {
			log.Fatalf("%v", err)
		}
		log.Fatalf("main: could not start cfg, cfgConnect: %s, err: %v\n"+
			"  Please check that your -cfg/-cfgConnect parameter (%q)\n"+
			"  is correct and/or that your configuration provider\n"+
			"  is available.",
			flags.CfgConnect, err, flags.CfgConnect)
	}

	var tagsArr []string
	if flags.Tags != "" {
		tagsArr = strings.Split(flags.Tags, ",")
	}

	routerInUse, err = mainStart(cfg, uuid, tagsArr,
		flags.Container, flags.Weight, flags.Extras,
		bindHTTPList[0], flags.DataDir,
		flags.StaticDir, flags.StaticETag,
		flags.Server, flags.Register, mr, options)
	if err != nil {
		log.Fatal(err)
	}

	if flags.Register == "unknown" {
		log.Printf("main: unregistered node; now exiting")
		os.Exit(0)
	}

	http.Handle("/", routerInUse)

	anyHostPorts := map[string]bool{}

	// Bind to 0.0.0.0's first for http listening.
	for _, bindHTTP := range bindHTTPList {
		if strings.HasPrefix(bindHTTP, "0.0.0.0:") {
			go mainServeHTTP("http", bindHTTP, nil, "", "")

			anyHostPorts[bindHTTP] = true
		}
	}

	for i := len(bindHTTPList) - 1; i >= 1; i-- {
		go mainServeHTTP("http", bindHTTPList[i], anyHostPorts, "", "")
	}

	if options["authType"] == "cbauth" {
		// Registering a certificate refresh callback with cbauth,
		// which will be responsible for updating https listeners,
		// whenever ssl certificates are changed.
		cbauth.RegisterCertRefreshCallback(setupHTTPSListeners)
	} else {
		setupHTTPSListeners()
	}

	mainServeHTTP("http", bindHTTPList[0], anyHostPorts, "", "")

	<-(make(chan struct{})) // Block forever.
}

// Add to HTTPS Server list serially
func addToHTTPSServerList(entry *http.Server) {
	httpsServersMutex.Lock()
	httpsServers = append(httpsServers, entry)
	httpsServersMutex.Unlock()
}

// Close all HTTPS Servers and clear HTTPS Server list
func closeAndClearHTTPSServerList() {
	httpsServersMutex.Lock()
	defer httpsServersMutex.Unlock()

	for _, server := range httpsServers {
		server.Close()
	}
	httpsServers = nil
}

func setupHTTPSListeners() error {
	// Close any previously open https servers
	closeAndClearHTTPSServerList()

	anyHostPorts := map[string]bool{}

	if flags.BindHTTPS != "" {
		bindHTTPSList := strings.Split(flags.BindHTTPS, ",")

		// Bind to 0.0.0.0's first for https listening.
		for _, bindHTTPS := range bindHTTPSList {
			if strings.HasPrefix(bindHTTPS, "0.0.0.0:") {
				go mainServeHTTP("https", bindHTTPS, nil,
					flags.TLSCertFile, flags.TLSKeyFile)

				anyHostPorts[bindHTTPS] = true
			}
		}

		for _, bindHTTPS := range bindHTTPSList {
			go mainServeHTTP("https", bindHTTPS, anyHostPorts,
				flags.TLSCertFile, flags.TLSKeyFile)
		}
	}

	return nil
}

// mainServeHTTP starts the http/https servers for cbft.
// The proto may be "http" or "https".
func mainServeHTTP(proto, bindHTTP string, anyHostPorts map[string]bool,
	certFile, keyFile string) {
	if bindHTTP[0] == ':' && proto == "http" {
		bindHTTP = "localhost" + bindHTTP
	}

	bar := "main: ------------------------------------------------------"

	if anyHostPorts != nil {
		// If we've already bound to 0.0.0.0 on the same port, then
		// skip this hostPort.
		hostPort := strings.Split(bindHTTP, ":")
		if len(hostPort) >= 2 {
			anyHostPort := "0.0.0.0:" + hostPort[1]
			if anyHostPorts[anyHostPort] {
				if anyHostPort != bindHTTP {
					log.Printf(bar)
					log.Printf("main: web UI / REST API is available"+
						" (via 0.0.0.0): %s://%s", proto, bindHTTP)
					log.Printf(bar)
				}
				return
			}
		}
	}

	log.Printf(bar)
	log.Printf("main: web UI / REST API is available: %s://%s", proto, bindHTTP)
	log.Printf(bar)

	server := &http.Server{Addr: bindHTTP, Handler: routerInUse}

	if proto == "http" {
		err := server.ListenAndServe() // Blocks on success.
		if err != nil {
			log.Fatalf("main: listen, err: %v;\n"+
				"  Please check that your -bindHttp(s) parameter (%q)\n"+
				"  is correct and available.", err, bindHTTP)
		}
	} else {
		addToHTTPSServerList(server)
		err := server.ListenAndServeTLS(certFile, keyFile)
		if err != nil {
			log.Printf("main: listen, err: %v;\n"+
				" HTTP listeners closed, likely to be re-initialized, "+
				" -bindHttp(s) (%q)\n", err, bindHTTP)
		}
	}
}

func loggerFunc(level, format string, args ...interface{}) string {
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	prefix := ts + " " + level + " "
	if format != "" {
		return prefix + fmt.Sprintf(format, args...)
	}
	return prefix + fmt.Sprint(args...)
}

func mainWelcome(flagAliases map[string][]string) {
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

func mainStart(cfg cbgt.Cfg, uuid string, tags []string, container string,
	weight int, extras, bindHTTP, dataDir, staticDir, staticETag, server string,
	register string, mr *cbgt.MsgRing, options map[string]string) (
	*mux.Router, error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	extrasMap, err := cbft.ParseExtras(extras)
	if err != nil {
		return nil, err
	}

	extrasMap["version-cbft.app"] = version
	extrasMap["version-cbft.lib"] = cbft.VERSION

	extrasJSON, err := json.Marshal(extrasMap)
	if err != nil {
		return nil, err
	}

	extras = string(extrasJSON)

	err = initMossOptions(options)
	if err != nil {
		return nil, err
	}

	err = initBleveOptions(options)
	if err != nil {
		return nil, err
	}

	if options["logStatsEvery"] != "" {
		var logStatsEvery int
		logStatsEvery, err = strconv.Atoi(options["logStatsEvery"])
		if err != nil {
			return nil, err
		}
		if logStatsEvery >= 0 {
			cbft.LogEveryNStats = logStatsEvery
		}
	}

	err = cbft.InitResultCacheOptions(options)
	if err != nil {
		return nil, err
	}

	err = cbft.InitBleveResultCacheOptions(options)
	if err != nil {
		return nil, err
	}

	exitCode := mainTool(cfg, uuid, tags, flags, options)
	if exitCode >= 0 {
		os.Exit(exitCode)
	}

	if server != "." && options["startCheckServer"] != "skip" {
		_, err = couchbase.Connect(server)
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

	meh := &mainHandlers{}
	mgr := cbgt.NewManagerEx(cbgt.VERSION, cfg,
		uuid, tags, container, weight,
		extras, bindHTTP, dataDir, server, meh, options)
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
		cbft.NewRESTRouter(version, mgr, staticDir, staticETag, mr,
			adtSvc)
	if err != nil {
		return nil, err
	}

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
	router.Handle(prefix+"/api/nsSearchResultRedirect/{pIndexName}/{docID}",
		nsSearchResultRedirectHandler)

	cbAuthBasicLoginHadler, err := cbft.CBAuthBasicLoginHandler(mgr)
	if err != nil {
		return nil, err
	}
	router.Handle(prefix+"/login", cbAuthBasicLoginHadler)

	router.Handle(prefix+"/api/managerOptions",
		cbft.NewManagerOptionsExt(mgr)).
		Methods("PUT").Name(prefix + "/api/managerOptions")

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

		verbose := defaultCtlVerbose
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

		var c *ctl.Ctl
		c, err = ctl.StartCtl(cfg, server, mgr.Options(), ctl.CtlOptions{
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

				err = service.RegisterManager(ctlMgr, nil)
				if err != nil {
					log.Errorf("main: cbauth_service register, err: %v", err)
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

type mainHandlers struct {
	mgr *cbgt.Manager
}

func (meh *mainHandlers) OnRegisterPIndex(pindex *cbgt.PIndex) {
	bindex, ok := pindex.Impl.(bleve.Index)
	if ok {
		bleveHttp.RegisterIndexName(pindex.Name, bindex)
		bindex.SetName(pindex.Name)
	}
}

func (meh *mainHandlers) OnUnregisterPIndex(pindex *cbgt.PIndex) {
	bleveHttp.UnregisterIndexByName(pindex.Name)
}

func (meh *mainHandlers) OnFeedError(srcType string, r cbgt.Feed, err error) {
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

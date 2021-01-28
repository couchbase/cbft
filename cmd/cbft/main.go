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
	"context"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/julienschmidt/httprouter"

	"github.com/blevesearch/bleve/v2"
	bleveRegistry "github.com/blevesearch/bleve/v2/registry"
	ftsHttp "github.com/couchbase/cbft/http"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/ctl"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
	"github.com/couchbase/go-couchbase"
	audit "github.com/couchbase/goutils/go-cbaudit"
	"github.com/couchbase/goutils/platform"
)

var cmdName = "cbft"

var version = "v0.6.0"

var defaultCtlVerbose = 3

var expvars = expvar.NewMap("stats")

// http router in use
var routerInUse http.Handler

func init() {
	cbgt.DCPFeedPrefix = "fts:"

	cbgt.CfgMetaKvPrefix = "/fts/cbgt/cfg/"

	cbgt.CfgAppVersion = "7.0.0"
}

func main() {
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

	logFileDescriptorLimit()

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
			log.Fatalf("main: could not write uuidPath: %s,\n"+
				"  err: %#v,\n"+
				"  Please check that your -data/-dataDir parameter (%q)\n"+
				"  is to a writable directory where %s can persist data.",
				uuidPath, err, flags.DataDir, cmdName)
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

	if purgeTimeOut, ok := cbgt.ParseOptionsInt(options, "cfgPlanPurgeTimeout"); ok {
		cbgt.PlanPurgeTimeout = int64(purgeTimeOut)
	}

	if v, ok := options["cbftVersion"]; ok {
		cbgt.CfgAppVersion = v
	}

	if _, ok := options["nsServerURL"]; !ok {
		options["nsServerURL"] = flags.Server
	}

	// Update the cached CertFile and KeyFile for TLS.
	cbgt.TLSCertFile = flags.TLSCertFile
	cbgt.TLSKeyFile = flags.TLSKeyFile

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

	if flags.BindGRPC != "" {
		bindGRPCList := strings.Split(flags.BindGRPC, ",")
		options["bindGRPC"] = bindGRPCList[0]
	}

	if flags.BindGRPCSSL != "" {
		bindGRPCList := strings.Split(flags.BindGRPCSSL, ",")
		options["bindGRPCSSL"] = bindGRPCList[0]
	}

	// register for the cbauth's security callbacks
	cbgt.RegisterSecurityNotifications()

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

	if os.Getenv("CBFT_CONSOLE") != "show" {
		platform.HideConsole(true)
		defer platform.HideConsole(false)
	}

	setupHTTPListenersAndServ(routerInUse, bindHTTPList, options)

	<-(make(chan struct{})) // Block forever.
}

func loggerFunc(level, format string, args ...interface{}) string {
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	prefix := ts + " [" + level + "] "
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
	http.Handler, error) {
	if server == "" {
		return nil, fmt.Errorf("error: server URL required (-server)")
	}

	// register remote http/gRPC clients for cbauth security notifications
	cbft.RegisterRemoteClientsForSecurity()

	extrasMap, err := cbft.ParseExtras(extras)
	if err != nil {
		return nil, err
	}

	extrasMap["features"] = cbgt.NodeFeatureLeanPlan +
		"," + cbft.FeatureScorchIndex + "," + cbft.FeatureUpsidedownIndex +
		"," + cbft.FeatureGRPC + "," + cbft.FeatureCollections +
		"," + cbft.FeatureBlevePreferredSegmentVersion

	extrasMap["version-cbft.app"] = version
	extrasMap["version-cbft.lib"] = cbft.VERSION

	s := options["http2"]
	if s == "true" && flags.TLSCertFile != "" && flags.TLSKeyFile != "" {
		extrasMap["bindHTTPS"] = flags.BindHTTPS
	}

	if _, ok := options["bindGRPC"]; ok {
		extrasMap["bindGRPC"] = options["bindGRPC"]
	}

	if _, ok := options["bindGRPCSSL"]; ok {
		extrasMap["bindGRPCSSL"] = options["bindGRPCSSL"]
	}

	extrasJSON, err := json.Marshal(extrasMap)
	if err != nil {
		return nil, err
	}

	extras = string(extrasJSON)

	err = initMemOptions(options)
	if err != nil {
		return nil, err
	}

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

	// Set logLevel if requested, after ensuring that it is a valid value.
	logLevelStr := options["logLevel"]
	if logLevelStr != "" {
		logLevel, exists := cbft.LogLevels[logLevelStr]
		if !exists {
			return nil, fmt.Errorf("error: invalid entry for"+
				" logLevel: %v", logLevelStr)
		}
		log.SetLevel(log.LogLevel(logLevel))
	} else {
		log.SetLevel(log.LevelNormal)
	}

	// If maxReplicasAllowed is among options provided, ensure that it
	// holds a valid value.
	if options["maxReplicasAllowed"] != "" {
		_, err = strconv.Atoi(options["maxReplicasAllowed"])
		if err != nil {
			return nil, fmt.Errorf("error: invalid entry for"+
				"maxReplicasAllowed: %v", options["maxReplicasAllowed"])
		}
	}

	// If gcMinThreshold is among options provided, ensure that it
	// holds a valid value.
	if options["gcMinThreshold"] != "" {
		var gcMinThreshold int
		gcMinThreshold, err = strconv.Atoi(options["gcMinThreshold"])
		if err != nil || gcMinThreshold < 0 {
			return nil, fmt.Errorf("error: invalid entry for"+
				"gcMinThreshold: %v", options["gcMinThreshold"])
		}
	}

	// If gcTriggerPct is among options provided, ensure that it
	// holds a valid value.
	if options["gcTriggerPct"] != "" {
		var gcTriggerPct int
		gcTriggerPct, err = strconv.Atoi(options["gcTriggerPct"])
		if err != nil || gcTriggerPct < 0 {
			return nil, fmt.Errorf("error: invalid entry for"+
				"gcTriggerPct: %v", options["gcTriggerPct"])
		}
	}

	// If memStatsLoggingInterval is among options provided, ensure that it
	// holds a valid value, defaults to 0 => disabled.
	if options["memStatsLoggingInterval"] != "" {
		var memStatsLoggingInterval int
		memStatsLoggingInterval, err = strconv.Atoi(options["memStatsLoggingInterval"])
		if err != nil || memStatsLoggingInterval < 0 {
			return nil, fmt.Errorf("error: invalid entry for"+
				"memStatsLoggingInterval: %v", options["memStatsLoggingInterval"])
		}
	}

	// If vbuckets is among the options provided, ensure that it holds a valid
	// value.
	if options["vbuckets"] != "" {
		var vbuckets int
		vbuckets, err = strconv.Atoi(options["vbuckets"])
		if err != nil || vbuckets < 0 {
			return nil, fmt.Errorf("error: invalid entry for"+
				"vbuckets: %v", options["vbuckets"])
		}

		// Add an entry for sourcePartitions if vbuckets is provided for
		// cbgt to read, this setting will be used to make a decision on
		// the number of vbuckets to allocate per index partition.
		options["sourcePartitions"] = options["vbuckets"]
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

	// enabled by default, runtime controllable through manager options
	log.Printf("main: custom jsoniter json implementation enabled")
	cbft.JSONImpl = &cbft.CustomJSONImpl{CustomJSONImplType: "jsoniter"}
	cbft.JSONImpl.SetManager(mgr)

	// set mgr for the NodeDefsFetcher, which is invoked for index type
	// validation during creation
	cbft.CurrentNodeDefsFetcher = &cbft.NodeDefsFetcher{}
	cbft.CurrentNodeDefsFetcher.SetManager(mgr)

	var adtSvc *audit.AuditSvc
	if options["cbaudit"] == "true" {
		adtSvc, err = audit.NewAuditSvc(server)
		if err != nil {
			log.Warnf("main: failed to start audit service with err: %v", err)
		}
	}

	setupGRPCListenersAndServ(mgr, options)

	muxrouter, _, err :=
		cbft.NewRESTRouter(version, mgr, staticDir, staticETag, mr, adtSvc)
	if err != nil {
		return nil, err
	}

	// register handlers needed by ns_server
	prefix := mgr.Options()["urlPrefix"]

	// no auth for /api/nsstats endpoint so search_admin and search_reader
	// can access these stats
	muxrouter.Handle(prefix+"/api/nsstats", cbft.NewNsStatsHandler(mgr))

	handle := func(path string, method string, h http.Handler) {
		dh := cbft.NewAuthVersionHandler(mgr, adtSvc,
			rest.NewHandlerWithRESTMeta(h, &rest.RESTMeta{path, method,
				map[string]string{"_path": path}},
				nil, path))

		muxrouter.Handle(path, dh).Methods(method).Name(path)
	}

	handle(prefix+"/_prometheusMetricsHigh", "GET",
		cbft.NewPrometheusHighMetricsHandler(mgr))

	handle(prefix+"/_prometheusMetrics", "GET",
		cbft.NewPrometheusMetricsHandler(mgr))

	nsStatusHandler, err := cbft.NewNsStatusHandler(mgr, server)
	if err != nil {
		return nil, err
	}
	handle(prefix+"/api/nsstatus", "GET", nsStatusHandler)

	nsSearchResultRedirectHandler, err := cbft.NsSearchResultRedirctHandler(mgr)
	if err != nil {
		return nil, err
	}
	handle(prefix+"/api/nsSearchResultRedirect/{pindexName}/{docID}",
		"GET",
		nsSearchResultRedirectHandler)

	cbAuthBasicLoginHadler, err := cbft.CBAuthBasicLoginHandler(mgr)
	if err != nil {
		return nil, err
	}
	muxrouter.Handle(prefix+"/login", cbAuthBasicLoginHadler)

	handle(prefix+"/api/index/{indexName}/analyzeDoc", "POST",
		cbft.NewAnalyzeDocHandler(mgr))

	handle(prefix+"/api/v1/backup", "GET",
		cbft.NewBackupIndexHandler(mgr))

	handle(prefix+"/api/v1/backup", "POST",
		cbft.NewRestoreIndexHandler(mgr))

	handle(prefix+"/api/v1/bucket/{bucketName}/backup", "GET",
		cbft.NewBucketBackupIndexHandler(mgr))

	handle(prefix+"/api/v1/bucket/{bucketName}/backup", "POST",
		cbft.NewBucketRestoreIndexHandler(mgr))

	handle(prefix+"/api/query/index/{indexName}", "GET",
		cbft.NewQuerySupervisorDetails())

	handle(prefix+"/api/conciseOptions", "GET", cbft.NewConciseOptions(mgr))

	router := exportMuxRoutesToHttprouter(muxrouter)

	router.Handler("PUT", prefix+"/api/managerOptions",
		cbft.NewAuthVersionHandler(mgr, nil, cbft.NewManagerOptionsExt(mgr)))

	router.Handler("GET", prefix+"/api/query",
		cbft.NewAuthVersionHandler(mgr, nil, cbft.NewQuerySupervisorDetails()))

	router.Handle("POST", prefix+"/api/query/:queryID/cancel",
		func(w http.ResponseWriter, req *http.Request, p httprouter.Params) {
			req = ContextSet(req, p)
			handler := cbft.NewAuthVersionHandler(mgr, nil, cbft.NewQueryKiller())
			handler.ServeHTTP(w, req)
		})

	// Setup all debug/pprof routes
	router.Handler("GET", "/debug/pprof/",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Index)))
	router.Handler("GET", "/debug/pprof/block",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Index)))
	router.Handler("GET", "/debug/pprof/goroutine",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Index)))
	router.Handler("GET", "/debug/pprof/heap",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Index)))
	router.Handler("GET", "/debug/pprof/mutex",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Index)))
	router.Handler("GET", "/debug/pprof/threadcreate",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Index)))
	router.Handler("GET", "/debug/pprof/cmdline",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Cmdline)))
	router.Handler("GET", "/debug/pprof/profile",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Profile)))
	router.Handler("GET", "/debug/pprof/symbol",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Symbol)))
	router.Handler("GET", "/debug/pprof/trace",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(pprof.Trace)))

	// Handle expvar route(s)
	router.Handler("GET", "/debug/vars",
		cbft.NewAuthVersionHandler(mgr, adtSvc, expvar.Handler()))

	// Handle untracked route(s)
	router.NotFound = &untrackedRouteHandler{}

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

		maxConcurrentPartitionMovesPerNode := 1
		v, found := cbgt.ParseOptionsInt(mgr.Options(), "maxConcurrentPartitionMovesPerNode")
		if found {
			maxConcurrentPartitionMovesPerNode = v
		}

		log.Printf("main: ctl starting,"+
			" dryRun: %v, waitForMemberNodes: %d, verbose: %d,"+
			" maxConcurrentPartitionMovesPerNode: %d", dryRun, waitForMemberNodes,
			verbose, maxConcurrentPartitionMovesPerNode)

		var c *ctl.Ctl
		c, err = ctl.StartCtl(cfg, server, mgr.Options(), ctl.CtlOptions{
			DryRun:                             dryRun,
			Verbose:                            verbose,
			FavorMinNodes:                      false,
			WaitForMemberNodes:                 waitForMemberNodes,
			MaxConcurrentPartitionMovesPerNode: maxConcurrentPartitionMovesPerNode,
			Manager:                            mgr,
			HttpClient:                         cbft.FetchHttp2Client(),
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

	go runBleveExpvarsCooker(mgr)

	return router, err
}

// -------------------------------------------------------

const (
	ctxKey = iota
)

func ContextSet(req *http.Request, val interface{}) *http.Request {
	if val == "" {
		return req
	}

	return req.WithContext(context.WithValue(req.Context(), ctxKey, val))
}

func ContextGet(req *http.Request, name string) string {
	if rv := req.Context().Value(ctxKey); rv != nil {
		for _, entry := range rv.(httprouter.Params) {
			if entry.Key == name {
				return entry.Value
			}
		}
	}
	return ""
}

// Fetches all routes, their methods and handlers from gorilla/mux router
// and initializes the julienschmidt/httprouter router with them.
func exportMuxRoutesToHttprouter(router *mux.Router) *httprouter.Router {
	hr := httprouter.New()

	re := regexp.MustCompile("{([^/]*)}")

	routesHandled := map[string]bool{}

	handleRoute := func(method, path string, handler http.Handler) {
		if _, handled := routesHandled[method+path]; !handled {
			httpRouterHandler := func(w http.ResponseWriter, req *http.Request,
				p httprouter.Params) {
				req = ContextSet(req, p)
				handler.ServeHTTP(w, req)
			}

			hr.Handle(method, path, httpRouterHandler)
			routesHandled[method+path] = true
		}
	}

	err := router.Walk(func(route *mux.Route, router *mux.Router,
		ancestors []*mux.Route) error {
		path, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		adjustedPath := re.ReplaceAllString(path, ":$1")

		if adjustedPath == "/api/managerOptions" {
			// This path is set with an extended handler as part of the
			// router setup within mainStart().
			return nil
		}

		handler := route.GetHandler()

		avh, ok := handler.(*cbft.AuthVersionHandler)
		if ok {
			hwrm, ok := avh.H.(*rest.HandlerWithRESTMeta)
			if ok && hwrm.RESTMeta != nil {
				handleRoute(hwrm.RESTMeta.Method, adjustedPath, handler)
			} else {
				log.Errorf("Failed to type assert auth version handler for "+
					"path: %v", adjustedPath)
			}
		} else {
			// Fetch the methods if these are bleve pindex routes.
			if method, ok := cbft.BleveRouteMethods[path]; ok {
				handleRoute(method, adjustedPath, handler)
			} else {
				for _, method := range []string{"GET", "PUT", "POST"} {
					handleRoute(method, adjustedPath, handler)
				}
			}
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Error walking gorilla/mux to fetch registered routes!")
	}

	// Set Request Variable Lookup
	rest.RequestVariableLookup = ContextGet

	return hr
}

// Custom handler for handling untracked route(s)
type untrackedRouteHandler struct{}

func (h *untrackedRouteHandler) ServeHTTP(w http.ResponseWriter,
	req *http.Request) {
	rest.PropagateError(w, nil, fmt.Sprintf("Page not found"), http.StatusNotFound)
}

// -------------------------------------------------------

type mainHandlers struct {
	mgr *cbgt.Manager
}

func (meh *mainHandlers) OnRegisterPIndex(pindex *cbgt.PIndex) {
	bindex, ok := pindex.Impl.(bleve.Index)
	if ok {
		ftsHttp.RegisterIndexName(pindex.Name, bindex)
		bindex.SetName(pindex.Name)
	}
}

func (meh *mainHandlers) OnUnregisterPIndex(pindex *cbgt.PIndex) {
	ftsHttp.UnregisterIndexByName(pindex.Name)
}

func (meh *mainHandlers) OnFeedError(srcType string, r cbgt.Feed, err error) {
	log.Printf("main: meh.OnFeedError, srcType: %s, err: %v", srcType, err)

	if r == nil ||
		(srcType != cbgt.SOURCE_GOCOUCHBASE && srcType != cbgt.SOURCE_GOCBCORE) {
		return
	}

	if srcType == cbgt.SOURCE_GOCOUCHBASE {
		if _, ok := err.(*couchbase.BucketNotFoundError); !ok {
			return
		}
	}

	dcpFeed, ok := r.(cbgt.FeedEx)
	if !ok {
		return
	}

	gone, indexUUID, er := dcpFeed.VerifySourceNotExists()
	log.Printf("main: meh.OnFeedError, VerifySourceNotExists,"+
		" srcType: %s, gone: %t, indexUUID: %s",
		srcType, gone, indexUUID)
	if !gone {
		if er != nil {
			log.Warnf("main: meh.OnFeedError, VerifySourceNotExists err: %v", er)
		}

		// If we get an EOF error from the feeds and the bucket is still alive,
		// then there could at the least two potential error scenarios.
		//
		// 1. Faulty kv node is failed over.
		// 2. Ephemeral network connection issues with the host.
		//
		// In either case, the current feed instance turns dangling.
		// Hence we can close the feeds so that they get refreshed to fix
		// the connectivity problems either during the next rebalance
		// (new kv node after failover-recovery rebalance) or
		// on the next janitor work cycle(ephemeral network issue to the same node).
		if strings.Contains(err.Error(), "EOF") {
			dcpFeed.NotifyMgrOnClose()
		}

		return
	}

	if len(indexUUID) == 0 {
		bucketName, bucketUUID := dcpFeed.GetBucketDetails()
		if bucketName == "" {
			return
		}

		log.Printf("main: meh.OnFeedError, DeleteAllIndexFromSource,"+
			" srcType: %s, bucketName: %s, bucketUUID: %s",
			srcType, bucketName, bucketUUID)

		meh.mgr.DeleteAllIndexFromSource(srcType, bucketName, bucketUUID)
	} else {
		log.Printf("main: meh.OnFeedError, DeleteIndex,"+
			" indexName: %s, indexUUID: %s",
			r.IndexName(), indexUUID)

		meh.mgr.DeleteIndexEx(r.IndexName(), indexUUID)
	}
}

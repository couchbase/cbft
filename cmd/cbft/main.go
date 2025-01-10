//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"context"
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/julienschmidt/httprouter"

	"github.com/blevesearch/bleve/v2"
	ftsHttp "github.com/couchbase/cbft/http"
	"github.com/couchbase/go-couchbase"

	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/cbgt/ctl"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
	audit "github.com/couchbase/goutils/go-cbaudit"
	"github.com/couchbase/goutils/platform"
)

var cmdName = "cbft"

var version = "v0.6.0"

var defaultCtlVerbose = 3

var expvars = expvar.NewMap("stats")

// AuthType used for SSL servers/listeners
var authType string

func init() {
	cbgt.DCPFeedPrefix = "fts:"

	cbgt.CfgMetaKvPrefix = "/fts/cbgt/cfg/"

	// Set this to 7.6.4 since that's the version Cfg VERSION was last changed in
	cbgt.CfgAppVersion = "7.6.4"
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

	// start the system event listener
	err := cbgt.StartSystemEventListener(flags.Server, "search", "cbft",
		os.Getpid())
	if err != nil {
		log.Fatal(err)
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

		err = os.WriteFile(uuidPath, []byte(uuid), 0600)
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

	if _, ok := options["serverSslPort"]; !ok {
		if flags.ServerSslPort > 0 {
			options["serverSslPort"] = strconv.Itoa(flags.ServerSslPort)
		}
	}

	// Update the cached TLSCAFile, TLSCertFile and TLSKeyFile for external TLS.
	// All 3 flags are paths to PEM encoded data.
	//   - TLSCAFile - holds multiple CAs
	//   - TLSCertFile - holds a certificate chain
	//   - TLSKeyFile - holds the private key
	cbgt.TLSCAFile = flags.TLSCAFile
	cbgt.TLSCertFile = flags.TLSCertFile
	cbgt.TLSKeyFile = flags.TLSKeyFile

	// Update the cached ClientCertFile and ClientKeyFile for internal TLS.
	// All 3 flags are paths to PEM encoded data.
	cbgt.ClientCertFile = flags.ClientCertFile
	cbgt.ClientKeyFile = flags.ClientKeyFile

	// Update and validate the the ipv4 and ipv6 settings
	ipv4 = options["ipv4"]
	ipv6 = options["ipv6"]
	validateIPFlags()

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

	authType = options["authType"]

	if flags.BindGRPC != "" {
		bindGRPCList := strings.Split(flags.BindGRPC, ",")
		options["bindGRPC"] = bindGRPCList[0]
	}

	if flags.BindGRPCSSL != "" {
		bindGRPCList := strings.Split(flags.BindGRPCSSL, ",")
		options["bindGRPCSSL"] = bindGRPCList[0]
	}

	if flags.RegulatorSettingsFile != "" {
		options["regulatorSettingsFile"] = flags.RegulatorSettingsFile
	}

	// register for the cbauth's security callbacks
	cbgt.RegisterSecurityNotifications()

	if err = mainStart(cfg, uuid, tagsArr,
		flags.Container, flags.Weight, flags.Extras,
		bindHTTPList[0], flags.DataDir,
		flags.StaticDir, flags.StaticETag,
		flags.Server, flags.Register, mr, options); err != nil {
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

	err = cbgt.PublishSystemEvent(cbgt.NewSystemEvent(
		cbgt.ServiceStartEventID,
		"info",
		"Search Service started",
		map[string]interface{}{
			"SearchNodeUUID":  uuid,
			"SearchIPAddress": bindHTTPList[0],
			"SearchDataDir":   flags.DataDir,
			"CBFTVersion":     cbft.VERSION,
			"CBGTVersion":     cbgt.VERSION,
		}))
	if err != nil {
		log.Errorf("main: unexpected system_event error: %v", err)
	}
	<-(make(chan struct{})) // Block forever.
}

func loggerFunc(level, format string, args ...interface{}) string {
	defer func() {
		if level == "FATA" {
			cbgt.PublishCrashEvent(fmt.Sprintf(format, args...))
		}
	}()
	ts := time.Now().Format("2006-01-02T15:04:05.000-07:00")
	prefix := ts + " [" + level + "] "
	if format != "" {
		return prefix + fmt.Sprintf(format, args...)
	}
	return prefix + fmt.Sprint(args...)
}

func initCPUOptions(options map[string]string) error {
	if options == nil {
		return nil
	}

	numCPU := cbft.GetNumCPUs()

	options["ftsCpuQuota"] = numCPU
	log.Printf("main: FTS CPU quota is: %s", numCPU)

	return nil
}

func mainWelcome(flagAliases map[string][]string) {
	cmd.LogFlags(flagAliases)
}

func mainStart(cfg cbgt.Cfg, uuid string, tags []string, container string,
	weight int, extras, bindHTTP, dataDir, staticDir, staticETag, server string,
	register string, mr *cbgt.MsgRing, options map[string]string) error {
	if server == "" {
		return fmt.Errorf("error: server URL required (-server)")
	}

	if options == nil {
		options = map[string]string{}
	}

	// register remote http/gRPC clients for cbauth security notifications
	cbft.RegisterRemoteClientsForSecurity()

	extrasMap, err := cbft.ParseExtras(extras)
	if err != nil {
		return err
	}

	extrasMap["features"] = cbgt.NodeFeatureLeanPlan +
		"," + cbgt.NodeFeatureAdvMetaEncoding +
		"," + cbft.FeatureScorchIndex +
		"," + cbft.FeatureGRPC +
		"," + cbft.FeatureCollections +
		"," + cbft.FeatureBlevePreferredSegmentVersion +
		"," + cbft.FeatureFileTransferRebalance +
		"," + cbft.FeatureGeoSpatial +
		cbft.FeatureVectorSearchSupport() +
		"," + cbft.FeatureXattrs

	extrasMap["version-cbft.app"] = version
	extrasMap["version-cbft.lib"] = cbft.VERSION

	s := options["http2"]
	if s == "true" &&
		(flags.TLSCAFile != "" ||
			(flags.TLSCertFile != "" && flags.TLSKeyFile != "")) {
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
		return err
	}

	extras = string(extrasJSON)

	if err = cbft.InitSystemStats(); err != nil {
		return err
	}

	if val, exists := options["useOSOBackfill"]; !exists || val != "false" {
		// Force OSO Backfills for ingest while using the gocbcore sourceType,
		// if option is explicitly not set to "false".
		options["useOSOBackfill"] = "true"
	}

	// Set DCP connection sharing for gocbcore feeds to 6
	options["maxFeedsPerDCPAgent"] = "6"

	options = registerServerlessHooks(options)

	if err = initMemOptions(options); err != nil {
		return err
	}

	if err = initCPUOptions(options); err != nil {
		return err
	}

	if err = initBleveOptions(options); err != nil {
		return err
	}

	if options["logStatsEvery"] != "" {
		logStatsEvery, err := strconv.Atoi(options["logStatsEvery"])
		if err != nil {
			return err
		}
		if logStatsEvery >= 0 {
			cbft.LogEveryNStats = logStatsEvery
		}
	}

	if err = cbft.InitResultCacheOptions(options); err != nil {
		return err
	}

	if err = cbft.InitBleveResultCacheOptions(options); err != nil {
		return err
	}

	if err = cbft.InitKNNQueryThrottlerOptions(options); err != nil {
		return err
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
				return fmt.Errorf("error: not a URL, server: %q\n"+
					"  Please check that your -server parameter"+
					" is a valid URL\n"+
					"  (http://HOST:PORT),"+
					" such as \"http://localhost:8091\",\n"+
					"  to a couchbase server",
					server)
			}

			return fmt.Errorf("error: could not connect"+
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
			return fmt.Errorf("error: invalid entry for"+
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
			return fmt.Errorf("error: invalid entry for"+
				"maxReplicasAllowed: %v", options["maxReplicasAllowed"])
		}
	}

	// If gcMinThreshold is among options provided, ensure that it
	// holds a valid value.
	if options["gcMinThreshold"] != "" {
		var gcMinThreshold int
		gcMinThreshold, err = strconv.Atoi(options["gcMinThreshold"])
		if err != nil || gcMinThreshold < 0 {
			return fmt.Errorf("error: invalid entry for"+
				"gcMinThreshold: %v", options["gcMinThreshold"])
		}
	}

	// If gcTriggerPct is among options provided, ensure that it
	// holds a valid value.
	if options["gcTriggerPct"] != "" {
		var gcTriggerPct int
		gcTriggerPct, err = strconv.Atoi(options["gcTriggerPct"])
		if err != nil || gcTriggerPct < 0 {
			return fmt.Errorf("error: invalid entry for"+
				"gcTriggerPct: %v", options["gcTriggerPct"])
		}
	}

	// If memStatsLoggingInterval is among options provided, ensure that it
	// holds a valid value, defaults to 0 => disabled.
	if options["memStatsLoggingInterval"] != "" {
		var memStatsLoggingInterval int
		memStatsLoggingInterval, err = strconv.Atoi(options["memStatsLoggingInterval"])
		if err != nil || memStatsLoggingInterval < 0 {
			return fmt.Errorf("error: invalid entry for"+
				"memStatsLoggingInterval: %v", options["memStatsLoggingInterval"])
		}
	}

	cbgt.StreamingEndpointListener = cbft.ListenStreamingEndpoint

	meh := &mainHandlers{}
	mgr := cbgt.NewManagerEx(cbgt.VERSION, cfg,
		uuid, tags, container, weight,
		extras, bindHTTP, dataDir, server, meh, options)
	meh.mgr = mgr

	if cbft.ServerlessMode {
		// Initialize the rate limiter, only if in serverless mode;
		// above variable set in registerServerlessHooks(..) above.
		_ = cbft.InitRateLimiter(mgr)
	}

	// start the effective cluster tracker.
	cbft.StartClusterVersionTracker(cbgt.CfgAppVersion,
		options["nsServerURL"])

	// start the server group tracker.
	cbft.StartServerGroupTracker(mgr)

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

	cbft.InitRESTPathStats()

	muxrouter, _, err :=
		cbft.NewRESTRouter(version, mgr, staticDir, staticETag, mr, adtSvc)
	if err != nil {
		return err
	}

	// register handlers needed by ns_server
	prefix := mgr.GetOption("urlPrefix")

	handle := func(path string, method string, h http.Handler) {
		dh := cbft.NewAuthVersionHandler(mgr, adtSvc,
			rest.NewHandlerWithRESTMeta(
				h, &rest.RESTMeta{
					Path:   path,
					Method: method,
					Opts:   map[string]string{"_path": path},
				}, nil, path))

		muxrouter.Handle(path, dh).Methods(method).Name(path)
	}

	handle(prefix+"/api/nsstats", "GET", cbft.NewNsStatsHandler(mgr))
	rest.RegisterDiagHandler(cbgt.DiagHandler{
		Name:        prefix + "/api/nsstats",
		Handler:     cbft.NewNsStatsHandler(mgr),
		HandlerFunc: nil,
	})

	handle(prefix+"/api/nsstats/indexSourceNames", "GET", cbft.NewBucketsNsStatsHandler(mgr))

	if cbft.ServerlessMode {
		handle(prefix+"/api/nodeUtilStats", "GET", cbft.NewNodeUtilStatsHandler(mgr))
	}

	handle(prefix+"/api/nsstats/index/{indexName}", "GET",
		cbft.NewIndexNsStatsHandler(mgr))

	handle(prefix+"/api/statsStream", "GET",
		cbft.NewStatsStreamHandler(mgr))

	handle(prefix+"/api/stats/index/{indexName}/progress", "GET",
		cbft.NewProgressStatsHandler(mgr))

	handle(prefix+"/_prometheusMetricsHigh", "GET",
		cbft.NewPrometheusHighMetricsHandler(mgr))

	handle(prefix+"/_prometheusMetrics", "GET",
		cbft.NewPrometheusMetricsHandler(mgr))

	handle(prefix+"/api/dcpAgentStats", "GET", cbft.NewDCPAgentsStatsHandler(mgr))
	rest.RegisterDiagHandler(cbgt.DiagHandler{
		Name:        prefix + "/api/dcpAgentStats",
		Handler:     cbft.NewDCPAgentsStatsHandler(mgr),
		HandlerFunc: nil,
	})

	nsStatusHandler, err := cbft.NewNsStatusHandler(mgr, server)
	if err != nil {
		return err
	}
	handle(prefix+"/api/nsstatus", "GET", nsStatusHandler)

	nsSearchResultRedirectHandler, err := cbft.NsSearchResultRedirctHandler(mgr)
	if err != nil {
		return err
	}
	handle(prefix+"/api/nsSearchResultRedirect/{pindexName}/{docID}",
		"GET",
		nsSearchResultRedirectHandler)

	cbAuthBasicLoginHadler, err := cbft.CBAuthBasicLoginHandler(mgr)
	if err != nil {
		return err
	}
	muxrouter.Handle(prefix+"/login", cbAuthBasicLoginHadler)

	handle(prefix+"/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/analyzeDoc", "POST",
		cbft.NewAnalyzeDocHandler(mgr))

	handle(prefix+"/api/index/{indexName}/analyzeDoc", "POST",
		cbft.NewAnalyzeDocHandler(mgr))

	handle(prefix+"/api/pindex/{pindexName}/contents", "GET",
		cbft.NewPIndexContentHandler(mgr))

	handle(prefix+"/api/v1/backup", "GET",
		cbft.NewBackupIndexHandler(mgr))

	handle(prefix+"/api/v1/backup", "POST",
		cbft.NewRestoreIndexHandler(mgr))

	handle(prefix+"/api/v1/bucket/{bucketName}/backup", "GET",
		cbft.NewBucketBackupIndexHandler(mgr))

	handle(prefix+"/api/v1/bucket/{bucketName}/backup", "POST",
		cbft.NewBucketRestoreIndexHandler(mgr))

	handle(prefix+"/api/query/index/{indexName}", "GET",
		cbft.NewQuerySupervisorDetails(mgr))

	handle(prefix+"/api/conciseOptions", "GET", cbft.NewConciseOptions(mgr))

	router := exportMuxRoutesToHttprouter(muxrouter, options)

	router.Handler("PUT", prefix+"/api/managerOptions",
		cbft.NewAuthVersionHandler(mgr, nil, cbft.NewManagerOptionsExt(mgr)))

	router.Handler("GET", prefix+"/api/query",
		cbft.NewAuthVersionHandler(mgr, nil, cbft.NewQuerySupervisorDetails(mgr)))

	router.Handle("POST", prefix+"/api/query/:queryID/cancel",
		func(w http.ResponseWriter, req *http.Request, p httprouter.Params) {
			req = ContextSet(req, p)
			handler := cbft.NewAuthVersionHandler(mgr, nil, cbft.NewQueryKiller(mgr))
			handler.ServeHTTP(w, req)
		})

	if cbft.ServerlessMode {
		endpoint, handler := cbft.MeteringEndpointHandler(mgr)
		if len(endpoint) > 0 && handler != nil {
			router.Handler("GET", prefix+endpoint,
				cbft.NewAuthVersionHandler(mgr, nil, handler))
		}
	}

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

	// Handle stats and profile route(s) for the C runtime associated
	// with the cbft process.
	router.Handler("GET", "/debug/jemallocStats",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(cbft.JeMallocStatsHandler)))
	router.Handler("GET", "/debug/jemallocProfiler",
		cbft.NewAuthVersionHandler(mgr, nil, http.HandlerFunc(cbft.JeMallocProfilerHandler)))

	// Handle unsupported method for route(s)
	router.MethodNotAllowed = &methodNotAllowedHandler{}

	// Handle untracked route(s)
	router.NotFound = &untrackedRouteHandler{}

	// ------------------------------------------------

	// Register gRPC and HTTP listeners
	setupGRPCListenersAndServe(mgr)
	setupHTTPListenersAndServe(router)

	// ------------------------------------------------

	tagsMap := mgr.TagsMap()
	if tagsMap != nil && tagsMap["cbauth_service"] {
		dryRun := false
		dryRunV := mgr.GetOption("cbauth_service.dryRun")
		if dryRunV != "" {
			dryRun, err = strconv.ParseBool(dryRunV)
			if err != nil {
				return err
			}
		}

		waitForMemberNodes := 30 // In seconds.
		waitForMemberNodesV := mgr.GetOption("cbauth_service.waitForMemberNodes")
		if waitForMemberNodesV != "" {
			waitForMemberNodes, err = strconv.Atoi(waitForMemberNodesV)
			if err != nil {
				return err
			}
		}

		verbose := defaultCtlVerbose
		verboseV := mgr.GetOption("cbauth_service.verbose")
		if verboseV != "" {
			verbose, err = strconv.Atoi(verboseV)
			if err != nil {
				return err
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
			EnableReplicaCatchup:               true, // enabling replica partition catchup by default
			MaxConcurrentPartitionMovesPerNode: maxConcurrentPartitionMovesPerNode,
			Manager:                            mgr,
			// Registering a callback for each node's cbft process
			SkipSeqChecksCallback: cbft.CustomSeqTimeoutCheck,
		})
		if err != nil {
			return fmt.Errorf("main: ctl.StartCtl, err: %v", err)
		}

		// deferred start of the mgr after the ctl initialisations, as
		// this helps ctl to get a glimpse of the cluster nodes before
		// self-registeration to cfg.
		err = mgr.Start(register)
		if err != nil {
			return err
		}

		priority := service.Priority(0)
		intVersion, err := cbgt.CompatibilityVersion(cbgt.CfgAppVersion)
		if err == nil {
			priority = service.Priority(intVersion)
		} else {
			log.Warnf("main: error fetching compatability version for app "+
				"version: %v \n", cbgt.CfgAppVersion)
		}
		nodeInfo := &service.NodeInfo{
			NodeID:   service.NodeID(uuid),
			Priority: priority,
		}

		err = cfg.Refresh()
		if err != nil {
			return err
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

		router.Handler("GET", prefix+"/api/ctlmanager",
			cbft.NewAuthVersionHandler(mgr, nil, ctl.NewCtlManagerStatusHandler(ctlMgr)))

		router.Handler("GET", prefix+"/api/hibernationStatus",
			cbft.NewAuthVersionHandler(mgr, nil, ctl.NewCtlHibernationStatusHandler(ctlMgr)))
	}

	// ------------------------------------------------

	go runBleveExpvarsCooker(mgr)

	return err
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
func exportMuxRoutesToHttprouter(router *mux.Router,
	options map[string]string) *httprouter.Router {
	hr := httprouter.New()

	re := regexp.MustCompile("{([^/]*)}")

	routesHandled := map[string]bool{}

	handleRoute := func(method, path string, handler http.Handler) {
		if _, handled := routesHandled[method+path]; !handled {
			handler = wrapTimeoutHandler(handler, options)
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

type methodNotAllowedHandler struct{}

func (h *methodNotAllowedHandler) ServeHTTP(w http.ResponseWriter,
	req *http.Request) {
	rest.PropagateError(w, nil, fmt.Sprintf("Method not allowed for endpoint"),
		http.StatusMethodNotAllowed)
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

func (meh *mainHandlers) OnRefreshManagerOptions(options map[string]string) {
	if meh.mgr != nil {
		err := initBleveOptions(options)
		if err != nil {
			log.Printf("main: meh.OnRefreshManagerOptions: bleve options, err: %v",
				err)
		}

		err = updateHerderOptions(options)
		if err != nil {
			log.Printf("main: meh.OnRefreshManagerOptions: herder options, err: %v",
				err)
		}
		err = cbft.InitKNNQueryThrottlerOptions(options)
		if err != nil {
			log.Printf("main: meh.OnRefreshManagerOptions, err: %v", err)
			return
		}
	}
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
	if r == nil ||
		(srcType != cbgt.SOURCE_GOCOUCHBASE && srcType != cbgt.SOURCE_GOCBCORE) {
		return
	}

	log.Printf("main: meh.OnFeedError, srcType: %s, feed name: %s, err: %v",
		srcType, r.Name(), err)

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
		" srcType: %s, gone: %t, indexUUID: %s, err: %v",
		srcType, gone, indexUUID, er)
	if !gone {
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

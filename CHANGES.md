# v0.3.0 (2015/09/18)

- bleve: changes for blevex/leveldb
- bleve: updated to new bleve.NewUsing() indexType API
- build: Makefile dist-build avoids 386 targets due to cross-build issues
- build: remove dist-meta manifest.projects gen due to awk dependency
- build: importing bleve/config package for more bleve features
- build: added tags to make coverage
- build: default to forestdb kvstore for Couchbase Server builds
- build: add forestdb build tag to Couchbase Server builds
- build: first CMakeLists.txt attempt for couchbase integration
- build: add list of thirdparty js/css components
- docs: fix example for new REST query API JSON
- docs: update docs with authSaslUser/Password REST params
- general: major refactoring into separate, reusable cbgt library
- general: use cbgt.LogFlags helper func
- general: issue 173: log current directory and abs(dataDir) during startup
- general: add support for specifying node extras from command-line
- general: merge pull request #159 from nimishzynga/changes
- general: adding metakv for cfg
- general: issue:154 added auth hint/help to connection err msg
- general: merge pull request #153 from nimishzynga/changes
- REST-API: typo in bleve registry REST metadata for FragmentFormatter
- REST-API: added partitions uuid's/seq's to /api/stats
- REST-API: add feed stats
- REST-API: add handlers for requests made by ns_server
- REST-API: diag has more /debug/pprof capture
- REST-API: diag includes /debug/pprof/goroutine?debug=2 output
- web-UI: optional checkbox shows curl cmd for index recreation

# v0.2.0 (2015/06/18)

The 0.2.0 release has an incompatible REST API change for query
requests (/api/index/{indexName}/query POST).  See the REST API
reference documentation for more information on the new query request
JSON structure and the new "ctl" sub-object.

- REST-API: query JSON timeout/consistency fields moved to "ctl" sub-object
- web UI: hide 2nd errorMessage element from bleve mapping
- web UI: for query has toggle'able cmd-line curl example
- web UI: query advanced checkbox shows JSON query request
- web UI: bleve http/mapping tree selected CSS highlight
- web UI: fix Tokenizer label alignment
- docs: added a 2nd bleve query example
- docs: pindexes can provide more than one QuerySample for auto docs
- docs: more godoc documentation for those using cbft as a library
- build: Makefile release fixes and instruction tweaks
- general: privitized workReq/syncWorkReq functionality

# v0.1.0

- web UI: fixes for angular slow retrieval of manager metadata
- web UI: use bleve/http/mapping UI for index mapping creation
- web-UI: hides doc count and query tab depending on pindex type
- web-UI: hide cfg JSON with advanced checkbox
- general: added files data source to index a local directory
- general: refactorings and more API to be more library-like
- general: propagating CAS through Dest interface
- general: propagating extras in Dest.DataUpdate/Delete() interface
- general: default bleve query timeout now 10secs
- general: query timeout works
- build: integrated travis CI
- build: generate LICESNES-thirdparty.txt from golang dependencies
- build: rename from cbft.darwin.amd64 to cbft.macos.amd64
- build: instructions on GITHUB_TOKEN/USER when releasing
- docs: various REST API and example fixes
- docs: added some windows specific getting-started docs
- docs: added section on dev preview limitations/disclaimers
- docs: added info on providing feedback
- docs: added braces to placeholder version number examples
- general: fix blackhole Count and Query tests
- general: better cmd-line error message for server param

# v0.0.5

- general: use 0700 if we mkdir the default dataDir
- general: URL's ok as index source name, so -server becomes a default
- library: added extras param to NewManager()
- library: added extras field to NodeDef JSON
- library: exposed AssetFS() and StaticFileRouter()
- library: refactored out InitManagerRESTRouter()
- library: refactored main helper funcs into cmd package
- web UI: fix, don't clear params before JSON parsing them
- web UI: remember user's last query
- web UI: hint that query timings are server-side
- web UI: retrieve managerMeta REST data only once at web UI load
- windows: disable colors on windows for clog
- docs: typo/grammar fixes

# v0.0.4

- build: releases have new naming pattern: cbft-VERSION.platform.gz
- web-UI: added confirm() to index manage buttons
- web-UI: JSON parsing error handling improved
- web-UI: redirect index.html to the web UI start page
- web-UI: store field is hidden/advanced by default
- web-UI: index definition JSON is hidden by default
- web-UI: source params is now hidden/advanced by default
- web-UI: source UUID is now hidden/advanced by default
- web-UI: prepopulates authUser for couchbase source type
- web-UI: added help links to docs and forums
- REST-API: index creation can take JSON body of index definition
- REST-API: /api/runtime/stats added currTime
- general: creates data directory if user is using "data" as default
- general: exits if we're unregistering the cbft node
- general: default for MaxPartitionsPerPIndex is now 20
- general: now errors and exits if node's uuid is already in use in cluster
- general: requires real IP for bindHttp when using couchbase bucket cfg
- general: default bindHttp address is now 0.0.0.0:8095
- general: set GOMAXPROCS to num cpu's if not already
- docs: lots more content, updates, including cmd-line usage capture
- docs: explicit mention of couchbase 3.x verison in usage string
- docs: added README-dev, including section on contributing code

# v0.0.3

- web-UI: index monitoring UI improvements, including hiding advanced.
  stats, aggregates, and refresh buttons.
- web-UI: index summary doc count refresh button.
- web-UI: index type and source type categorizations.
- web-UI: added link to query syntax help.
- general: better error messages throughout.
- general: no logging every query, only errors.
- general: cmd-line usage message improvements.
- general: more prominent logging of web-UI/REST URL on startup.
- build: release and Makefile updates, introducing cbft-full.
- build: added kagome as it's pure golang.
- docs: improved REST API docs, README and info for cbft developers.
- internals: Dest method renames.
- internals: exposed PIndexImplTypes and FeedTypes as public.

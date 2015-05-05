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

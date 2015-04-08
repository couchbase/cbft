# API Reference

---

# Indexing

## Index definition

---

GET `/api/index`

Returns all index definitions as JSON.

**version introduced**: 0.0.1

---

GET `/api/index/{indexName}`

Returns the definition of an index as JSON.

**version introduced**: 0.0.1

---

PUT `/api/index/{indexName}`

Creates/updates an index definition.

**param: indexName**: required, string, URL path parameter

the name of the created/updated index definition

**param: indexParams**: optional, string (JSON), form parameter

**param: indexType**: required, string, form parameter

supported index types:

* alias - supports fan-out of queries to multiple index targets
* blackhole - ignores all incoming data and is not queryable; used for testing
* bleve - full-text index powered by the bleve full-text-search engine
* bleve-mem - full-text index powered by bleve (in memory only)
* vlite - lightweight, view-like index
* vlite-mem - lightweight, view-like index (in memory only)

**param: planParams**: optional, string (JSON), form parameter

**param: prevIndexUUID**: optional, string, form parameter

intended for clients that want to check that they are not overwriting the index definition updates of concurrent clients

**param: sourceName**: optional, string, form parameter

**param: sourceParams**: optional, string (JSON), form parameter

**param: sourceType**: required, string, form parameter

supported source types:

* couchbase - Couchbase Server/Cluster data source
* nil - a NO-OP data source, used for index aliases and testing
* primary - a primary data source

**param: sourceUUID**: optional, string, form parameter

**result on error**: non-200 HTTP error code

**result on success**: HTTP 200 with body JSON of {"status": "ok"}

**version introduced**: 0.0.1

---

DELETE `/api/index/{indexName}`

Deletes an index definition.

**param: indexName**: required, string, URL path parameter

the name of the index definition to be deleted

**version introduced**: 0.0.1

## Index management

---

POST `/api/index/{indexName}/ingestControl/{op}`

Pause index updates and maintenance (no more
                          ingesting document mutations).

**param: op**: Allowed values for op are
                          "pause" or "resume".

**version introduced**: 0.0.1

---

POST `/api/index/{indexName}/planFreezeControl/{op}`

Freeze the assignment of index partitions to nodes.

**param: op**: Allowed values for op are
                          "freeze" or "unfreeze".

**version introduced**: 0.0.1

---

POST `/api/index/{indexName}/queryControl/{op}`

Disallow queries on an index.

**param: op**: Allowed values for op are
                          "allow" or "disallow".

**version introduced**: 0.0.1

## Index monitoring

---

GET `/api/stats`

Returns indexing and data related metrics,
                       timings and counters for the node.

**version introduced**: 0.0.1

## Index querying

---

GET `/api/index/{indexName}/count`

Returns the count of indexed documents.

**version introduced**: 0.0.1

---

POST `/api/index/{indexName}/query`

Queries an index.

**version introduced**: 0.0.1

---

# Node

## Node configuration

---

GET `/api/cfg`

Returns the node's current view
                       of the cluster's configuration.

**version introduced**: 0.0.1

---

POST `/api/cfgRefresh`

Requests the node to refresh its configuration
                       from the configuration provider.

**version introduced**: 0.0.1

---

POST `/api/managerKick`

Forces the node to replan resource assignments and
                       to update its state to reflect the latest plan.

**version introduced**: 0.0.1

---

GET `/api/managerMeta`

Returns information on the node's capabilities,
                       including available storage and bleve options.

**version introduced**: 0.0.1

## Node diagnostics

---

GET `/api/diag`

Returns full set of diagnostic information
                       from the node.

**version introduced**: 0.0.1

---

GET `/api/log`

Returns recent log messages
                       and key events for the node.

**version introduced**: 0.0.1

---

GET `/api/runtime`

Returns information on the node's software,
                       such as version strings and slow-changing
                       runtime settings.

**version introduced**: 0.0.1

---

GET `/api/runtime/args`

Returns information on the node's command-line,
                       parameters, environment variables and
                       O/S process values.

**version introduced**: 0.0.1

---

POST `/api/runtime/profile/cpu`

Requests the node to capture
                       cpu usage profiling information.

**version introduced**: 0.0.1

---

POST `/api/runtime/profile/memory`

Requests the node to capture
                       memory usage profiling information.

**version introduced**: 0.0.1

## Node management

---

POST `/api/runtime/gc`

Requests the node to perform a GC.

**version introduced**: 0.0.1

## Node monitoring

---

GET `/api/runtime/stats`

Returns information on the node's
                       low-level runtime stats.

**version introduced**: 0.0.1

---

GET `/api/runtime/statsMem`

Returns information on the node's
                       low-level GC and memory related runtime stats.

**version introduced**: 0.0.1

---

# Advanced

## Index partition definition

---

GET `/api/pindex`

**version introduced**: 0.0.1

---

GET `/api/pindex-bleve`

**version introduced**: 0.0.1

---

GET `/api/pindex-bleve/{pindexName}`

**version introduced**: 0.0.1

---

GET `/api/pindex/{pindexName}`

**version introduced**: 0.0.1

## Index partition querying

---

GET `/api/pindex-bleve/{pindexName}/count`

**version introduced**: 0.0.1

---

POST `/api/pindex-bleve/{pindexName}/query`

**version introduced**: 0.0.1

---

GET `/api/pindex/{pindexName}/count`

**version introduced**: 0.0.1

---

POST `/api/pindex/{pindexName}/query`

**version introduced**: 0.0.1

## bleve index diagnostics

---

GET `/api/pindex-bleve/{pindexName}/doc/{docID}`

**version introduced**: 0.0.1

---

GET `/api/pindex-bleve/{pindexName}/docDebug/{docID}`

**version introduced**: 0.0.1

---

GET `/api/pindex-bleve/{pindexName}/fields`

**version introduced**: 0.0.1


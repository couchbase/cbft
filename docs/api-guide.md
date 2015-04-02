# Indexing

## Index definition

---
**/api/index**

**method**: GET

**version introduced**: 0.0.0

---
**/api/index/{indexName}**

**method**: GET

**version introduced**: 0.0.0

## Index management

---
**/api/index/{indexName}/ingestControl/{op}**

**method**: POST

**param: op**: Allowed values for op are
                          "pause" or "resume".

**version introduced**: 0.0.0

---
**/api/index/{indexName}/planFreezeControl/{op}**

**method**: POST

**param: op**: Allowed values for op are
                          "freeze" or "unfreeze".

**version introduced**: 0.0.0

---
**/api/index/{indexName}/queryControl/{op}**

**method**: POST

**param: op**: Allowed values for op are
                          "allow" or "disallow".

**version introduced**: 0.0.0

## Index monitoring

---
**/api/stats**

**method**: GET

Returns indexing and data related metrics,
                       timings and counters for the node.

**version introduced**: 0.0.0

## Index querying

---
**/api/index/{indexName}/count**

**method**: GET

**version introduced**: 0.0.0

---
**/api/index/{indexName}/query**

**method**: POST

**version introduced**: 0.0.0

# Node

## Node configuration

---
**/api/cfg**

**method**: GET

Returns the node's current view
                       of the cluster's configuration.

**version introduced**: 0.0.0

---
**/api/cfgRefresh**

**method**: POST

Requests the node to refresh its configuration
                       from the configuration provider.

**version introduced**: 0.0.0

---
**/api/managerKick**

**method**: POST

Forces the node to replan resource assignments and
                       to update its state to reflect the latest plan.

**version introduced**: 0.0.0

---
**/api/managerMeta**

**method**: GET

Returns information on the node's capabilities,
                       including available storage and bleve options.

**version introduced**: 0.0.0

## Node diagnostics

---
**/api/diag**

**method**: GET

Returns full set of diagnostic information
                       from the node.

**version introduced**: 0.0.0

---
**/api/log**

**method**: GET

Returns recent log messages
                       and key events for the node.

**version introduced**: 0.0.0

---
**/api/runtime**

**method**: GET

Returns information on the node's software,
                       such as version strings and slow-changing
                       runtime settings.

**version introduced**: 0.0.0

---
**/api/runtime/args**

**method**: GET

Returns information on the node's command-line,
                       parameters, environment variables and
                       O/S process values.

**version introduced**: 0.0.0

---
**/api/runtime/profile/cpu**

**method**: POST

Requests the node to capture
                       cpu usage profiling information.

**version introduced**: 0.0.0

---
**/api/runtime/profile/memory**

**method**: POST

Requests the node to capture
                       memory usage profiling information.

**version introduced**: 0.0.0

## Node management

---
**/api/runtime/gc**

**method**: POST

Requests the node to perform a GC.

**version introduced**: 0.0.0

## Node monitoring

---
**/api/runtime/stats**

**method**: GET

Returns information on the node's
                       low-level runtime stats.

**version introduced**: 0.0.0

---
**/api/runtime/statsMem**

**method**: GET

Returns information on the node's
                       low-level GC and memory related runtime stats.

**version introduced**: 0.0.0

# Advanced

## Index partition definition

---
**/api/pindex**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex-bleve**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex/{pindexName}**

**method**: GET

**version introduced**: 0.0.0

## Index partition querying

---
**/api/pindex-bleve/{pindexName}/count**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}/query**

**method**: POST

**version introduced**: 0.0.0

---
**/api/pindex/{pindexName}/count**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex/{pindexName}/query**

**method**: POST

**version introduced**: 0.0.0

## bleve index diagnostics

---
**/api/pindex-bleve/{pindexName}/doc/{docID}**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}/docDebug/{docID}**

**method**: GET

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}/fields**

**method**: GET

**version introduced**: 0.0.0


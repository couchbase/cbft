# Indexing

## Index definition

---
**/api/index GET**

Returns all index definitions.

**version introduced**: 0.0.0

---
**/api/index/{indexName} DELETE**

Deletes an index definition.

**version introduced**: 0.0.0

---
**/api/index/{indexName} GET**

Returns the definition of an index.

**version introduced**: 0.0.0

---
**/api/index/{indexName} PUT**

Creates/updates an index definition.

**version introduced**: 0.0.0

## Index management

---
**/api/index/{indexName}/ingestControl/{op} POST**

Pause index updates and maintenance (no more
                          ingesting document mutations).

**param: op**: Allowed values for op are
                          "pause" or "resume".

**version introduced**: 0.0.0

---
**/api/index/{indexName}/planFreezeControl/{op} POST**

Freeze the assignment of index partitions to nodes.

**param: op**: Allowed values for op are
                          "freeze" or "unfreeze".

**version introduced**: 0.0.0

---
**/api/index/{indexName}/queryControl/{op} POST**

Disallow queries on an index.

**param: op**: Allowed values for op are
                          "allow" or "disallow".

**version introduced**: 0.0.0

## Index monitoring

---
**/api/stats GET**

Returns indexing and data related metrics,
                       timings and counters for the node.

**version introduced**: 0.0.0

## Index querying

---
**/api/index/{indexName}/count GET**

Returns the count of indexed documents.

**version introduced**: 0.0.0

---
**/api/index/{indexName}/query POST**

Queries an index.

**version introduced**: 0.0.0

# Node

## Node configuration

---
**/api/cfg GET**

Returns the node's current view
                       of the cluster's configuration.

**version introduced**: 0.0.0

---
**/api/cfgRefresh POST**

Requests the node to refresh its configuration
                       from the configuration provider.

**version introduced**: 0.0.0

---
**/api/managerKick POST**

Forces the node to replan resource assignments and
                       to update its state to reflect the latest plan.

**version introduced**: 0.0.0

---
**/api/managerMeta GET**

Returns information on the node's capabilities,
                       including available storage and bleve options.

**version introduced**: 0.0.0

## Node diagnostics

---
**/api/diag GET**

Returns full set of diagnostic information
                       from the node.

**version introduced**: 0.0.0

---
**/api/log GET**

Returns recent log messages
                       and key events for the node.

**version introduced**: 0.0.0

---
**/api/runtime GET**

Returns information on the node's software,
                       such as version strings and slow-changing
                       runtime settings.

**version introduced**: 0.0.0

---
**/api/runtime/args GET**

Returns information on the node's command-line,
                       parameters, environment variables and
                       O/S process values.

**version introduced**: 0.0.0

---
**/api/runtime/profile/cpu POST**

Requests the node to capture
                       cpu usage profiling information.

**version introduced**: 0.0.0

---
**/api/runtime/profile/memory POST**

Requests the node to capture
                       memory usage profiling information.

**version introduced**: 0.0.0

## Node management

---
**/api/runtime/gc POST**

Requests the node to perform a GC.

**version introduced**: 0.0.0

## Node monitoring

---
**/api/runtime/stats GET**

Returns information on the node's
                       low-level runtime stats.

**version introduced**: 0.0.0

---
**/api/runtime/statsMem GET**

Returns information on the node's
                       low-level GC and memory related runtime stats.

**version introduced**: 0.0.0

# Advanced

## Index partition definition

---
**/api/pindex GET**

**version introduced**: 0.0.0

---
**/api/pindex-bleve GET**

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName} GET**

**version introduced**: 0.0.0

---
**/api/pindex/{pindexName} GET**

**version introduced**: 0.0.0

## Index partition querying

---
**/api/pindex-bleve/{pindexName}/count GET**

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}/query POST**

**version introduced**: 0.0.0

---
**/api/pindex/{pindexName}/count GET**

**version introduced**: 0.0.0

---
**/api/pindex/{pindexName}/query POST**

**version introduced**: 0.0.0

## bleve index diagnostics

---
**/api/pindex-bleve/{pindexName}/doc/{docID} GET**

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}/docDebug/{docID} GET**

**version introduced**: 0.0.0

---
**/api/pindex-bleve/{pindexName}/fields GET**

**version introduced**: 0.0.0


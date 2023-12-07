//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

// See: https://docs.google.com/document/d/1JXm4PXyli45KE5dyGvD5oQLb6MQPJSkubDR21JZQycQ/edit?ts=56b15d8f#
var restPermDefault = "cluster.fts!read"

var restPerms = `
GET /api/index
cluster.bucket{}.fts!read

GET /api/bucket/{bucketName}/scope/{scopeName}/index
cluster.bucket{}.fts!read

GET /api/index/{indexName}
cluster.collection[<sourceName>].fts!read

GET /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}
cluster.collection[<sourceName>].fts!read

PUT /api/index/{indexName}
cluster.collection[<sourceName>].fts!write
24577

PUT /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}
cluster.collection[<sourceName>].fts!write
24577

DELETE /api/index/{indexName}
cluster.collection[<sourceName>].fts!write
24576

DELETE /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}
cluster.collection[<sourceName>].fts!write
24576

POST /api/index/{indexName}/ingestControl/{op}
cluster.collection[<sourceName>].fts!manage
24579

POST /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/ingestControl/{op}
cluster.collection[<sourceName>].fts!manage
24579

POST /api/index/{indexName}/planFreezeControl/{op}
cluster.collection[<sourceName>].fts!manage
24579

POST /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/planFreezeControl/{op}
cluster.collection[<sourceName>].fts!manage
24579

POST /api/index/{indexName}/queryControl/{op}
cluster.collection[<sourceName>].fts!manage
24579

POST /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/queryControl/{op}
cluster.collection[<sourceName>].fts!manage
24579

GET /api/statsStream
cluster.bucket[].fts!read

GET /api/stats
cluster.bucket[].fts!read

GET /api/nsstats/buckets
cluster.bucket[].fts!read

GET /api/nsstats
cluster.bucket[].fts!read

GET /api/nodeUtilStats
cluster.bucket[].fts!read

GET /api/dcpAgentStats
cluster.bucket[].fts!read

GET /api/nsstats/index/{indexName}
cluster.collection[<sourceName>].fts!read

GET /api/stats/index/{indexName}/progress
cluster.collection[<sourceName>].fts!read

GET /api/stats/index/{indexName}
cluster.collection[<sourceName>].fts!read

GET /api/stats/sourceStats/{indexName}
cluster.collection[<sourceName>].stats.fts!read

GET /api/index/{indexName}/count
cluster.collection[<sourceName>].fts!read

GET /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/count
cluster.collection[<sourceName>].fts!read

POST /api/index/{indexName}/query
cluster.collection[<sourceName>].fts!read

POST /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/query
cluster.collection[<sourceName>].fts!read

POST /api/index/{indexName}/analyzeDoc
cluster.collection[<sourceName>].fts!read

POST /api/index/{indexName}/tasks
cluster.bucket[<sourceName>].fts!write

POST /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/tasks
cluster.bucket[<sourceName>].fts!write

GET /api/index/{indexName}/status
cluster.collection[<sourceName>].fts!read

GET /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/status
cluster.collection[<sourceName>].fts!read

POST /api/index/{indexName}/pindexLookup
cluster.collection[<sourceName>].fts!read

POST /api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/pindexLookup
cluster.collection[<sourceName>].fts!read

GET /api/cfg
cluster.settings.fts!read

POST /api/cfgRefresh
cluster.settings.fts!write
24580

POST /api/cfgNodeDefs
cluster.settings.fts!write
24586

POST /api/cfgPlanPIndexes
cluster.settings.fts!write
24587

POST /api/managerKick
cluster.settings.fts!write
24581

GET /api/managerMeta
cluster.settings.fts!read

GET /api/diag
cluster.logs.fts!read

GET /api/log
cluster.logs.fts!read

GET /api/runtime
cluster.settings.fts!read

GET /api/runtime/args
cluster.settings.fts!read

POST /api/runtime/profile/cpu
cluster.settings.fts!write
24583

POST /api/runtime/profile/memory
cluster.settings.fts!write
24584

POST /api/runtime/gc
cluster.settings.fts!write
24582

POST /api/runtime/trace
cluster.settings.fts!write
24585

GET /api/runtime/stats
cluster.stats.fts!read

GET /api/runtime/statsMem
cluster.stats.fts!read

GET /api/pindex
cluster.bucket[].fts!read

GET /api/pindex/{pindexName}
cluster.collection[<sourceName>].fts!read

GET /api/pindex/{pindexName}/count
cluster.collection[<sourceName>].fts!read

POST /api/pindex/{pindexName}/query
cluster.collection[<sourceName>].fts!read

GET /api/pindex/{pindexName}/contents
cluster.bucket[<sourceName>].fts!write

GET /api/ping
none

GET /debug/vars
cluster.settings.fts!read

GET /_prometheusMetrics
cluster.admin.internal.stats!read

GET /_prometheusMetricsHigh
cluster.admin.internal.stats!read

RPC /Search
cluster.collection[<sourceName>].fts!read

GET /api/conciseOptions
cluster.settings.fts!read

GET /api/v1/backup
cluster.bucket[].fts!read

POST /api/v1/backup
cluster.bucket[].fts!read

GET /api/v1/bucket/{bucketName}/backup
cluster.bucket[<bucketName>].fts!read

POST /api/v1/bucket/{bucketName}/backup
cluster.bucket[<bucketName>].fts!read

GET /api/indexes/source/{bucketName}
cluster.bucket[<bucketName>].fts!read
`

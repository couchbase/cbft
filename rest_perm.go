//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

// See: https://docs.google.com/document/d/1JXm4PXyli45KE5dyGvD5oQLb6MQPJSkubDR21JZQycQ/edit?ts=56b15d8f#
//
var restPermDefault = "cluster.fts!read"

var restPerms = `
GET /api/index
cluster.bucket{}.fts!read

GET /api/index/{indexName}
cluster.bucket[<sourceName>].fts!read

PUT /api/index/{indexName}
cluster.bucket[<sourceName>].fts!write
24577

DELETE /api/index/{indexName}
cluster.bucket[<sourceName>].fts!write
24576

POST /api/index/{indexName}/ingestControl/{op}
cluster.bucket[<sourceName>].fts!manage
24579

POST /api/index/{indexName}/planFreezeControl/{op}
cluster.bucket[<sourceName>].fts!manage
24579

POST /api/index/{indexName}/queryControl/{op}
cluster.bucket[<sourceName>].fts!manage
24579

GET /api/stats
cluster.bucket[].stats.fts!read

GET /api/stats/index/{indexName}
cluster.bucket[<sourceName>].stats.fts!read

GET /api/stats/sourceStats/{indexName}
cluster.bucket[<sourceName>].stats.fts!read

GET /api/index/{indexName}/count
cluster.bucket[<sourceName>].fts!read

POST /api/index/{indexName}/query
cluster.bucket[<sourceName>].fts!read

GET /api/cfg
cluster.settings.fts!read

POST /api/cfgRefresh
cluster.settings.fts!write
24580

POST /api/cfgNodeDefs
cluster.settings.fts!write
24586

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
cluster.bucket[<sourceName>].fts!read

GET /api/pindex/{pindexName}/count
cluster.bucket[<sourceName>].fts!read

POST /api/pindex/{pindexName}/query
cluster.bucket[<sourceName>].fts!read

GET /api/ping
none
`

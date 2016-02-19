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
var restPermDefault = "cluster!read"

var restPerms = `
GET /api/index
cluster.bucket.fts!read

GET /api/index/{indexName}
cluster.bucket[<sourceName>].fts!read

PUT /api/index/{indexName}
cluster.bucket[<sourceName>].fts!write

CREATE /api/index/{indexName}
cluster.bucket.fts!write

DELETE /api/index/{indexName}
cluster.bucket[<sourceName>].fts!write

POST /api/index/{indexName}/ingestControl/{op}
cluster.bucket[<sourceName>].fts!manage

POST /api/index/{indexName}/planFreezeControl/{op}
cluster.bucket[<sourceName>].fts!manage

POST /api/index/{indexName}/queryControl/{op}
cluster.bucket[<sourceName>].fts!manage

GET /api/stats
cluster.bucket.stats!read

GET /api/stats/index/{indexName}
cluster.bucket[<sourceName>].stats!read

GET /api/index/{indexName}/count
cluster.bucket[<sourceName>].fts!read

POST /api/index/{indexName}/query
cluster.bucket[<sourceName>].fts!read

GET /api/cfg
cluster.settings!read

POST /api/cfgRefresh
cluster.settings!write

POST /api/managerKick
cluster.settings!write

GET /api/managerMeta
cluster.settings!read

GET /api/diag
cluster.logs!read

GET /api/log
cluster.logs!read

GET /api/runtime
cluster.settings!read

GET /api/runtime/args
cluster.settings!read

POST /api/runtime/profile/cpu
cluster.settings!write

POST /api/runtime/profile/memory
cluster.settings!write

POST /api/runtime/gc
cluster.settings!write

GET /api/runtime/stats
cluster.stats!read

GET /api/runtime/statsMem
cluster.stats!read

GET /api/pindex
cluster.bucket.fts!read

GET /api/pindex/{pindexName}
cluster.bucket[<sourceName>].fts!read

GET /api/pindex/{pindexName}/count
cluster.bucket[<sourceName>].fts!read

POST /api/pindex/{pindexName}/query
cluster.bucket[<sourceName>].fts!read
`

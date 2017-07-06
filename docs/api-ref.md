# API Reference

---

# Indexing

---

## Index definition

---

GET `/api/index`

Returns all index definitions as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "indexDefs": {
        "implVersion": "5.0.0",
        "indexDefs": {
          "myFirstIndex": {
            "name": "myFirstIndex",
            "params": null,
            "planParams": {
              "hierarchyRules": null,
              "maxPartitionsPerPIndex": 0,
              "nodePlanParams": null,
              "numReplicas": 0,
              "pindexWeights": null,
              "planFrozen": false
            },
            "sourceName": "",
            "sourceParams": null,
            "sourceType": "nil",
            "sourceUUID": "",
            "type": "blackhole",
            "uuid": "6cc599ab7a85bf3b"
          }
        },
        "uuid": "6cc599ab7a85bf3b"
      },
      "status": "ok"
    }

---

GET `/api/index/{indexName}`

Returns the definition of an index as JSON.

**param: indexName**: required, string, URL path parameter

The name of the index definition to be retrieved.

**version introduced**: 0.0.1

Sample response:

    {
      "indexDef": {
        "name": "myFirstIndex",
        "params": null,
        "planParams": {
          "hierarchyRules": null,
          "maxPartitionsPerPIndex": 0,
          "nodePlanParams": null,
          "numReplicas": 0,
          "pindexWeights": null,
          "planFrozen": false
        },
        "sourceName": "",
        "sourceParams": null,
        "sourceType": "nil",
        "sourceUUID": "",
        "type": "blackhole",
        "uuid": "6cc599ab7a85bf3b"
      },
      "planPIndexes": [
        {
          "indexName": "myFirstIndex",
          "indexType": "blackhole",
          "indexUUID": "6cc599ab7a85bf3b",
          "name": "myFirstIndex_6cc599ab7a85bf3b_00000000",
          "nodes": {
            "78fc2ffac2fd9401": {
              "canRead": true,
              "canWrite": true,
              "priority": 0
            }
          },
          "sourceName": "",
          "sourcePartitions": "",
          "sourceType": "nil",
          "sourceUUID": "",
          "uuid": "1ac72081ac81e0aa"
        }
      ],
      "status": "ok",
      "warnings": []
    }

---

PUT `/api/index/{indexName}`

Creates/updates an index definition.

**param: indexName**: required, string, URL path parameter

The name of the to-be-created/updated index definition,
validated with the regular expression of ```^[A-Za-z][0-9A-Za-z_\-]*$```.

**param: indexParams**: optional (depends on the value of the indexType), JSON object, form parameter

For indexType ```blackhole```, the indexParams can be null.

For indexType ```fulltext-alias```, an example indexParams JSON:

    {
      "targets": {
        "yourIndexName": {
          "indexUUID": ""
        }
      }
    }

For indexType ```fulltext-index```, an example indexParams JSON:

    {
      "mapping": {
        "default_mapping": {
          "enabled": true,
          "dynamic": true,
          "default_analyzer": ""
        },
        "type_field": "_type",
        "default_type": "_default",
        "default_analyzer": "standard",
        "default_datetime_parser": "dateTimeOptional",
        "default_field": "_all",
        "store_dynamic": true,
        "index_dynamic": true,
        "analysis": {}
      },
      "store": {
        "kvStoreName": "boltdb"
      },
      "doc_config": {
        "mode": "type_field",
        "type_field": "type",
        "docid_prefix_delim": "",
        "docid_regexp": null
      }
    }

**param: indexType**: required, string, form parameter

Supported indexType's:

* ```blackhole```: a blackhole index ignores all data and is not queryable; used for testing
* ```fulltext-alias```: a full text index alias provides a naming level of indirection to one or more actual, target full text indexes
* ```fulltext-index```: a full text index powered by the bleve engine

**param: planParams**: optional, JSON object, form parameter

**param: prevIndexUUID / indexUUID**: optional, string, form parameter

Intended for clients that want to check that they are not overwriting the index definition updates of concurrent clients.

**param: sourceName**: optional, string, form parameter

**param: sourceParams**: optional (depends on the value of the sourceType), JSON object, form parameter

For sourceType ```couchbase```, an example sourceParams JSON:

    {
      "clusterManagerBackoffFactor": 0,
      "clusterManagerSleepInitMS": 0,
      "clusterManagerSleepMaxMS": 2000,
      "dataManagerBackoffFactor": 0,
      "dataManagerSleepInitMS": 0,
      "dataManagerSleepMaxMS": 2000,
      "feedBufferSizeBytes": 0,
      "feedBufferAckThreshold": 0,
      "noopTimeIntervalSecs": 1
    }

For sourceType ```files```, an example sourceParams JSON:

    {
      "regExps": [
        ".txt$",
        ".md$"
      ],
      "maxFileSize": 0,
      "numPartitions": 0,
      "sleepStartMS": 5000,
      "backoffFactor": 1.5,
      "maxSleepMS": 300000
    }

For sourceType ```nil```, the sourceParams can be null.

**param: sourceType**: required, string, form parameter

Supported sourceType's:

* ```couchbase```: a Couchbase Server bucket will be the data source
* ```files```: files under a dataDir subdirectory tree will be the data source
* ```nil```: a nil data source has no data; used for index aliases and testing

**param: sourceUUID**: optional, string, form parameter

**result on error**: non-200 HTTP error code

**result on success**: HTTP 200 with body JSON of {"status": "ok"}

**version introduced**: 0.0.1

---

DELETE `/api/index/{indexName}`

Deletes an index definition.

**param: indexName**: required, string, URL path parameter

The name of the index definition to be deleted.

**version introduced**: 0.0.1

---

## Index management

---

POST `/api/index/{indexName}/ingestControl/{op}`

Pause index updates and maintenance (no more
                          ingesting document mutations).

**param: indexName**: required, string, URL path parameter

The name of the index whose control values will be modified.

**param: op**: required, string, URL path parameter

Allowed values for op are "pause" or "resume".

**version introduced**: 0.0.1

---

POST `/api/index/{indexName}/planFreezeControl/{op}`

Freeze the assignment of index partitions to nodes.

**param: indexName**: required, string, URL path parameter

The name of the index whose control values will be modified.

**param: op**: required, string, URL path parameter

Allowed values for op are "freeze" or "unfreeze".

**version introduced**: 0.0.1

---

POST `/api/index/{indexName}/queryControl/{op}`

Disallow queries on an index.

**param: indexName**: required, string, URL path parameter

The name of the index whose control values will be modified.

**param: op**: required, string, URL path parameter

Allowed values for op are "allow" or "disallow".

**version introduced**: 0.0.1

---

## Index monitoring

---

GET `/api/stats`

Returns indexing and data related metrics,
                       timings and counters from the node as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "feeds": {
        "myFirstIndex_6cc599ab7a85bf3b": {}
      },
      "manager": {
        "TotCreateIndex": 1,
        "TotCreateIndexOk": 1,
        "TotDeleteIndex": 0,
        "TotDeleteIndexBySource": 0,
        "TotDeleteIndexBySourceErr": 0,
        "TotDeleteIndexBySourceOk": 0,
        "TotDeleteIndexOk": 0,
        "TotIndexControl": 0,
        "TotIndexControlOk": 0,
        "TotJanitorClosePIndex": 0,
        "TotJanitorKick": 3,
        "TotJanitorKickErr": 1,
        "TotJanitorKickOk": 2,
        "TotJanitorKickStart": 3,
        "TotJanitorLoadDataDir": 0,
        "TotJanitorNOOP": 0,
        "TotJanitorNOOPOk": 0,
        "TotJanitorOpDone": 3,
        "TotJanitorOpErr": 1,
        "TotJanitorOpRes": 3,
        "TotJanitorOpStart": 3,
        "TotJanitorRemovePIndex": 0,
        "TotJanitorStop": 0,
        "TotJanitorSubscriptionEvent": 1,
        "TotJanitorUnknownErr": 0,
        "TotKick": 0,
        "TotPlannerKick": 3,
        "TotPlannerKickChanged": 1,
        "TotPlannerKickErr": 0,
        "TotPlannerKickOk": 3,
        "TotPlannerKickStart": 3,
        "TotPlannerNOOP": 0,
        "TotPlannerNOOPOk": 0,
        "TotPlannerOpDone": 3,
        "TotPlannerOpErr": 0,
        "TotPlannerOpRes": 3,
        "TotPlannerOpStart": 3,
        "TotPlannerStop": 0,
        "TotPlannerSubscriptionEvent": 1,
        "TotPlannerUnknownErr": 0,
        "TotRefreshLastIndexDefs": 2,
        "TotRefreshLastNodeDefs": 0,
        "TotRefreshLastPlanPIndexes": 1,
        "TotRegisterFeed": 1,
        "TotRegisterPIndex": 1,
        "TotSaveNodeDef": 2,
        "TotSaveNodeDefGetErr": 0,
        "TotSaveNodeDefNil": 0,
        "TotSaveNodeDefOk": 2,
        "TotSaveNodeDefRetry": 0,
        "TotSaveNodeDefSame": 0,
        "TotSaveNodeDefSetErr": 0,
        "TotSetOptions": 0,
        "TotUnregisterFeed": 0,
        "TotUnregisterPIndex": 0
      },
      "pindexes": {
        "myFirstIndex_6cc599ab7a85bf3b_00000000": null
      }
    }

---

GET `/api/stats/index/{indexName}`

Returns metrics, timings and counters
                       for a single index from the node as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "feeds": {
        "myFirstIndex_6cc599ab7a85bf3b": {}
      },
      "pindexes": {
        "myFirstIndex_6cc599ab7a85bf3b_00000000": null
      }
    }

---

GET `/api/stats/sourcePartitionSeqs/{indexName}`

Returns data source partiton seqs
                       for an index as JSON.

**param: indexName**: required, string, URL path parameter

The name of the index whose partition seqs should be retrieved.

**version introduced**: 4.2.0

Sample response:

    null

---

GET `/api/stats/sourceStats/{indexName}`

Returns data source specific stats
                       for an index as JSON.

**param: indexName**: required, string, URL path parameter

The name of the index whose partition seqs should be retrieved.

**param: statsKind**: optional, string

Optional source-specific string for kind of stats wanted.

**version introduced**: 4.2.0

Sample response:

    null

---

## Index querying

---

GET `/api/index/{indexName}/count`

Returns the count of indexed documents.

**param: indexName**: required, string, URL path parameter

The name of the index whose count is to be retrieved.

**version introduced**: 0.0.1

---

POST `/api/index/{indexName}/query`

Queries an index.

**param: indexName**: required, string, URL path parameter

The name of the index to be queried.

**version introduced**: 0.2.0

The request's POST body depends on the index type:

For index type ```fulltext-index```:

A simple bleve query POST body:

    {
      "query": {
        "query": "a sample query"
      },
      "size": 10,
      "from": 0,
      "highlight": null,
      "fields": null,
      "facets": null,
      "explain": false,
      "sort": null,
      "includeLocations": false
    }
An example POST body using from/size for results paging,
using ctl for a timeout and for "at_plus" consistency level.
On consistency, the index must have incorporated at least mutation
sequence-number 123 for partition (vbucket) 0 and mutation
sequence-number 234 for partition (vbucket) 1 (where vbucket 1
should have a vbucketUUID of a0b1c2):

    {
      "ctl": {
        "timeout": 10000,
        "consistency": {
          "level": "at_plus",
          "vectors": {
            "customerIndex": {
              "0": 123,
              "1/a0b1c2": 234
            }
          }
        }
      },
      "query": {
        "query": "alice smith"
      },
      "size": 10,
      "from": 20,
      "highlight": {
        "style": null,
        "fields": null
      },
      "fields": [
        "*"
      ],
      "facets": null,
      "explain": true,
      "sort": null,
      "includeLocations": false
    }


---

# Node

---

## Node configuration

---

GET `/api/cfg`

Returns the node's current view
                       of the cluster's configuration as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "indexDefs": {
        "implVersion": "5.0.0",
        "indexDefs": {
          "myFirstIndex": {
            "name": "myFirstIndex",
            "params": null,
            "planParams": {
              "hierarchyRules": null,
              "maxPartitionsPerPIndex": 0,
              "nodePlanParams": null,
              "numReplicas": 0,
              "pindexWeights": null,
              "planFrozen": false
            },
            "sourceName": "",
            "sourceParams": null,
            "sourceType": "nil",
            "sourceUUID": "",
            "type": "blackhole",
            "uuid": "6cc599ab7a85bf3b"
          }
        },
        "uuid": "6cc599ab7a85bf3b"
      },
      "indexDefsCAS": 4,
      "indexDefsErr": null,
      "nodeDefsKnown": {
        "implVersion": "5.0.0",
        "nodeDefs": {
          "78fc2ffac2fd9401": {
            "container": "",
            "extras": "",
            "hostPort": "0.0.0.0:8094",
            "implVersion": "5.0.0",
            "tags": null,
            "uuid": "78fc2ffac2fd9401",
            "weight": 1
          }
        },
        "uuid": "2f0d18fb750b2d4a"
      },
      "nodeDefsKnownCAS": 1,
      "nodeDefsKnownErr": null,
      "nodeDefsWanted": {
        "implVersion": "5.0.0",
        "nodeDefs": {
          "78fc2ffac2fd9401": {
            "container": "",
            "extras": "",
            "hostPort": "0.0.0.0:8094",
            "implVersion": "5.0.0",
            "tags": null,
            "uuid": "78fc2ffac2fd9401",
            "weight": 1
          }
        },
        "uuid": "72d6750878551451"
      },
      "nodeDefsWantedCAS": 2,
      "nodeDefsWantedErr": null,
      "planPIndexes": {
        "implVersion": "5.0.0",
        "planPIndexes": {
          "myFirstIndex_6cc599ab7a85bf3b_00000000": {
            "indexName": "myFirstIndex",
            "indexType": "blackhole",
            "indexUUID": "6cc599ab7a85bf3b",
            "name": "myFirstIndex_6cc599ab7a85bf3b_00000000",
            "nodes": {
              "78fc2ffac2fd9401": {
                "canRead": true,
                "canWrite": true,
                "priority": 0
              }
            },
            "sourceName": "",
            "sourcePartitions": "",
            "sourceType": "nil",
            "sourceUUID": "",
            "uuid": "1ac72081ac81e0aa"
          }
        },
        "uuid": "414f9eea6197296d",
        "warnings": {
          "myFirstIndex": []
        }
      },
      "planPIndexesCAS": 5,
      "planPIndexesErr": null,
      "status": "ok"
    }

---

POST `/api/cfgRefresh`

Requests the node to refresh its configuration
                       from the configuration provider.

**version introduced**: 0.0.1

---

GET `/api/manager`

Returns runtime config information about this node.

**version introduced**: 0.4.0

Sample response:

    {
      "mgr": {
        "bindHttp": "0.0.0.0:8094",
        "container": "",
        "dataDir": "tmp/data249764618",
        "extras": "",
        "options": {},
        "server": "http://localhost:8091",
        "startTime": "2017-03-01T13:32:10.172035612-08:00",
        "tags": null,
        "uuid": "78fc2ffac2fd9401",
        "version": "5.0.0",
        "weight": 1
      },
      "status": "ok"
    }

---

POST `/api/managerKick`

Forces the node to replan resource assignments
                       (by running the planner, if enabled) and to update
                       its runtime state to reflect the latest plan
                       (by running the janitor, if enabled).

**version introduced**: 0.0.1

---

GET `/api/managerMeta`

Returns information on the node's capabilities,
                       including available indexing and storage options as JSON,
                       and is intended to help management tools and web UI's
                       to be more dynamically metadata driven.

**version introduced**: 0.0.1

---

PUT `/api/managerOptions`

Set the options for the manager

**version introduced**: 4.2.0

---

## Node diagnostics

---

GET `/api/diag`

Returns full set of diagnostic information
                        from the node in one shot as JSON.  That is, the
                        /api/diag response will be the union of the responses
                        from the other REST API diagnostic and monitoring
                        endpoints from the node, and is intended to make
                        production support easier.

**version introduced**: 0.0.1

---

GET `/api/log`

Returns recent log messages
                       and key events for the node as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "events": [],
      "messages": []
    }

---

GET `/api/ping`

Returns an empty body as a quick aliveness check.

**version introduced**: 5.0.0

---

GET `/api/runtime`

Returns information on the node's software,
                       such as version strings and slow-changing
                       runtime settings as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "arch": "amd64",
      "go": {
        "GOMAXPROCS": 8,
        "GOROOT": "/usr/local/go",
        "compiler": "gc",
        "version": "go1.8"
      },
      "numCPU": 8,
      "os": "darwin",
      "versionData": "5.0.0",
      "versionMain": "v0.3.1-345-ga3409b2"
    }

---

GET `/api/runtime/args`

Returns information on the node's command-line,
                       parameters, environment variables and
                       O/S process values as JSON.

**version introduced**: 0.0.1

---

POST `/api/runtime/profile/cpu`

Requests the node to capture local
                       cpu usage profiling information.

**version introduced**: 0.0.1

---

POST `/api/runtime/profile/memory`

Requests the node to capture lcoal
                       memory usage profiling information.

**version introduced**: 0.0.1

---

## Node management

---

POST `/api/runtime/gc`

Requests the node to perform a GC.

**version introduced**: 0.0.1

---

## Node monitoring

---

GET `/api/runtime/stats`

Returns information on the node's
                       low-level runtime stats as JSON.

**version introduced**: 0.0.1

---

GET `/api/runtime/statsMem`

Returns information on the node's
                       low-level GC and memory related runtime stats as JSON.

**version introduced**: 0.0.1

---

POST `/api/runtime/trace`

Requests the node to trace the program

**version introduced**: 5.0.0

---

# Advanced

---

## Index partition definition

---

GET `/api/pindex`

**version introduced**: 0.0.1

Sample response:

    {
      "pindexes": {
        "myFirstIndex_6cc599ab7a85bf3b_00000000": {
          "indexName": "myFirstIndex",
          "indexParams": "",
          "indexType": "blackhole",
          "indexUUID": "6cc599ab7a85bf3b",
          "name": "myFirstIndex_6cc599ab7a85bf3b_00000000",
          "sourceName": "",
          "sourceParams": "",
          "sourcePartitions": "",
          "sourceType": "nil",
          "sourceUUID": "",
          "uuid": "337163e07b605e92"
        }
      },
      "status": "ok"
    }

---

GET `/api/pindex/{pindexName}`

**version introduced**: 0.0.1

---

## Index partition querying

---

GET `/api/pindex/{pindexName}/count`

**version introduced**: 0.0.1

---

POST `/api/pindex/{pindexName}/query`

**version introduced**: 0.2.0

---

Copyright (c) 2015 Couchbase, Inc.

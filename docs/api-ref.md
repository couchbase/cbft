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
        "implVersion": "3.1.0",
        "indexDefs": {
          "myFirstIndex": {
            "name": "myFirstIndex",
            "params": "",
            "planParams": {
              "hierarchyRules": null,
              "maxPartitionsPerPIndex": 0,
              "nodePlanParams": null,
              "numReplicas": 0,
              "planFrozen": false
            },
            "sourceName": "",
            "sourceParams": "",
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
        "params": "",
        "planParams": {
          "hierarchyRules": null,
          "maxPartitionsPerPIndex": 0,
          "nodePlanParams": null,
          "numReplicas": 0,
          "planFrozen": false
        },
        "sourceName": "",
        "sourceParams": "",
        "sourceType": "nil",
        "sourceUUID": "",
        "type": "blackhole",
        "uuid": "6cc599ab7a85bf3b"
      },
      "planPIndexes": [
        {
          "indexName": "myFirstIndex",
          "indexParams": "",
          "indexType": "blackhole",
          "indexUUID": "6cc599ab7a85bf3b",
          "name": "myFirstIndex_6cc599ab7a85bf3b_0",
          "nodes": {
            "78fc2ffac2fd9401": {
              "canRead": true,
              "canWrite": true,
              "priority": 0
            }
          },
          "sourceName": "",
          "sourceParams": "",
          "sourcePartitions": "",
          "sourceType": "nil",
          "sourceUUID": "",
          "uuid": "64bed6e2edf354c3"
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

**param: indexParams**: optional (depends on the value of the indexType), string (JSON), form parameter

For indexType ```alias```, an example indexParams JSON:

    {
      "targets": {
        "yourIndexName": {
          "indexUUID": ""
        }
      }
    }

For indexType ```blackhole```, the indexParams can be null.

For indexType ```bleve```, an example indexParams JSON:

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
        "byte_array_converter": "json",
        "analysis": {}
      },
      "store": {
        "kvStoreName": "boltdb"
      }
    }

**param: indexType**: required, string, form parameter

Supported indexType's:

* ```alias```: an alias provides a naming level of indirection to one or more actual, target indexes
* ```blackhole```: a blackhole index ignores all data and is not queryable; used for testing
* ```bleve```: a full-text index powered by the bleve engine

**param: planParams**: optional, string (JSON), form parameter

**param: prevIndexUUID**: optional, string, form parameter

Intended for clients that want to check that they are not overwriting the index definition updates of concurrent clients.

**param: sourceName**: optional, string, form parameter

**param: sourceParams**: optional (depends on the value of the sourceType), string (JSON), form parameter

For sourceType ```couchbase```, an example sourceParams JSON:

    {
      "authUser": "",
      "authPassword": "",
      "clusterManagerBackoffFactor": 0,
      "clusterManagerSleepInitMS": 0,
      "clusterManagerSleepMaxMS": 20000,
      "dataManagerBackoffFactor": 0,
      "dataManagerSleepInitMS": 0,
      "dataManagerSleepMaxMS": 20000,
      "feedBufferSizeBytes": 0,
      "feedBufferAckThreshold": 0
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
        "TotDeleteIndexOk": 0,
        "TotIndexControl": 0,
        "TotIndexControlOk": 0,
        "TotJanitorClosePIndex": 0,
        "TotJanitorKick": 2,
        "TotJanitorKickErr": 0,
        "TotJanitorKickOk": 2,
        "TotJanitorKickStart": 2,
        "TotJanitorNOOP": 0,
        "TotJanitorNOOPOk": 0,
        "TotJanitorRemovePIndex": 0,
        "TotJanitorSubscriptionEvent": 0,
        "TotJanitorUnknownErr": 0,
        "TotKick": 0,
        "TotPlannerKick": 2,
        "TotPlannerKickChanged": 1,
        "TotPlannerKickErr": 0,
        "TotPlannerKickOk": 2,
        "TotPlannerKickStart": 2,
        "TotPlannerNOOP": 0,
        "TotPlannerNOOPOk": 0,
        "TotPlannerSubscriptionEvent": 0,
        "TotPlannerUnknownErr": 0,
        "TotSaveNodeDef": 2,
        "TotSaveNodeDefGetErr": 0,
        "TotSaveNodeDefOk": 2,
        "TotSaveNodeDefSetErr": 0,
        "TotSaveNodeDefUUIDErr": 0,
        "TotSaveNodeDefUUIDTakenErr": 0
      },
      "pindexes": {
        "myFirstIndex_6cc599ab7a85bf3b_0": null
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
        "myFirstIndex_6cc599ab7a85bf3b_0": null
      }
    }

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

**version introduced**: 0.0.1

The request's POST body depends on the index type:

For index type ```bleve```, an example POST body:

    {
      "ctl": {
        "timeout": 10000,
        "consistency": {
          "level": "",
          "vectors": {}
        }
      },
      "query": null,
      "size": 0,
      "from": 0,
      "highlight": null,
      "fields": null,
      "facets": null,
      "explain": false
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
        "implVersion": "3.1.0",
        "indexDefs": {
          "myFirstIndex": {
            "name": "myFirstIndex",
            "params": "",
            "planParams": {
              "hierarchyRules": null,
              "maxPartitionsPerPIndex": 0,
              "nodePlanParams": null,
              "numReplicas": 0,
              "planFrozen": false
            },
            "sourceName": "",
            "sourceParams": "",
            "sourceType": "nil",
            "sourceUUID": "",
            "type": "blackhole",
            "uuid": "6cc599ab7a85bf3b"
          }
        },
        "uuid": "6cc599ab7a85bf3b"
      },
      "indexDefsCAS": 3,
      "indexDefsErr": null,
      "nodeDefsKnown": {
        "implVersion": "3.1.0",
        "nodeDefs": {
          "0.0.0.0:8095": {
            "container": "",
            "extras": "",
            "hostPort": "0.0.0.0:8095",
            "implVersion": "3.1.0",
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
        "implVersion": "3.1.0",
        "nodeDefs": {
          "0.0.0.0:8095": {
            "container": "",
            "extras": "",
            "hostPort": "0.0.0.0:8095",
            "implVersion": "3.1.0",
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
        "implVersion": "3.1.0",
        "planPIndexes": {
          "myFirstIndex_6cc599ab7a85bf3b_0": {
            "indexName": "myFirstIndex",
            "indexParams": "",
            "indexType": "blackhole",
            "indexUUID": "6cc599ab7a85bf3b",
            "name": "myFirstIndex_6cc599ab7a85bf3b_0",
            "nodes": {
              "78fc2ffac2fd9401": {
                "canRead": true,
                "canWrite": true,
                "priority": 0
              }
            },
            "sourceName": "",
            "sourceParams": "",
            "sourcePartitions": "",
            "sourceType": "nil",
            "sourceUUID": "",
            "uuid": "64bed6e2edf354c3"
          }
        },
        "uuid": "6327debf817a5ec7",
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

GET `/api/runtime`

Returns information on the node's software,
                       such as version strings and slow-changing
                       runtime settings as JSON.

**version introduced**: 0.0.1

Sample response:

    {
      "arch": "amd64",
      "go": {
        "GOMAXPROCS": 1,
        "GOROOT": "/usr/local/go",
        "compiler": "gc",
        "version": "go1.4"
      },
      "numCPU": 8,
      "os": "darwin",
      "versionData": "3.1.0",
      "versionMain": "v0.1.0"
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

# Advanced

---

## Index partition definition

---

GET `/api/pindex`

**version introduced**: 0.0.1

Sample response:

    {
      "pindexes": {
        "myFirstIndex_6cc599ab7a85bf3b_0": {
          "indexName": "myFirstIndex",
          "indexParams": "",
          "indexType": "blackhole",
          "indexUUID": "6cc599ab7a85bf3b",
          "name": "myFirstIndex_6cc599ab7a85bf3b_0",
          "sourceName": "",
          "sourceParams": "",
          "sourcePartitions": "",
          "sourceType": "nil",
          "sourceUUID": "",
          "uuid": "2d9ecb8b574a9f6a"
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

**version introduced**: 0.0.1

---

Copyright (c) 2015 Couchbase, Inc.

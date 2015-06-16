# Index queries

You can query indexes either using cbft's REST API or using its web
admin UI.

## Web admin UI queries

You can use cbft's web admin UI in order to query an index using your
web browser:

- Navigate to the ```Indexes``` screen.

- Click on your index's name link to navigate to your index's summary
  screen.

- Click on the ```Query``` tab for your index.

- Enter your query statement in the input field.

The kind of query statement you can enter depends on your index's
type.

Please see below in this document for more information on the query
statement requirements for the different index types.

- You can click on the ```Advanced``` checkbox in order to see
  more optional, advanced query parameters.

## REST API queries

Please see the [REST API reference](api-ref) for documentation on
programmatically POST'ing query requests to cbft.

You can POST a query request to any cbft node in a cbft cluster.

To process your query request, the cbft node that receives your REST
query will then in turn perform distributed requests against the other
nodes in the cbft cluster and gather their responses.

Your REST client will then receive a processed, merged response from
the cbft node that received the original REST query request.

Programmatically, the POST body for REST API ```bleve``` queries would
look similar to the following example JSON:

    {
      "ctl": {
        "timeout": 0,
        "consistency": {
          "level": "",
          "vectors": {}
        },
      },
      "query": {
        "boost": 1.0,
        "query": "your query string here"
      }
    }

There are several common fields:

- ```ctl``` - an optional JSON sub-object that contains generic query
  request control information, such as ```timeout``` and
  ```consistency```.

- ```timeout``` - an optional integer in the ```ctl``` JSON
  sub-object, timeout in milliseconds, 0 for no timeout.

- ```consistency``` - an optional JSON sub-object in the ```ctl```
  JSON sub-object to ensure that the index has reached a consistency
  level required by the application, where the index has incorporated
  enough required data from the data source of the index.  See below
  in this document for more information.

# Index types and queries

## Index type: bleve

When using the ```bleve``` index type, the query statement that you
use in cbft's web admin UI must follow bleve's full-text index [query
string](https://github.com/blevesearch/bleve/wiki/Query%20String%20Query)
syntax rules.

See: [https://github.com/blevesearch/bleve/wiki/Query%20String%20Query](https://github.com/blevesearch/bleve/wiki/Query%20String%20Query)

An example JSON for querying the ```bleve``` index type:

    {
      "ctl": {
        "timeout": 10000,    // Optional timeout, in milliseconds.
        "consistency": {     // Optionally wait for index consistency.
          "level": "",       // "" means wait that a stale index is ok to query.
                             // "at_plus" means index must incorporate
                             // the latest mutations up to the following optional vectors.
          "vectors": {
            "yourIndexName": { // This JSON map is keyed by strings of
                               // "partitionId" (vbucketId) or by
                               // "partitionId/partitionUUID" (vbucketId/vbucketUUID).
                               // Values are data-source partition (vbucket)
                               // sequence numbers that must incorporated
                               // into the index before the query can proceed.
              "0": 123,        // This example means the index partition for
                               // partitionId "0" (or vbucketId 0) must
                               // reach at least sequence number 123 before
                               // the query can proceeed (or timeout).
              "1": 444,        // The index partition for partitionId "1" (or vbucketId 1)
                               // must reach at least sequence number 444
                               // before the query can proceed (or timeout).
              "2/a0b1c2": 555  // The index partition for partitionId "2" (or vbucketId 2)
                               // which must have partition (or vbucket) UUID of "a0b1c2"
                               // must reach at least sequence nubmer 555
                               // before the query can proceed (or timeout).
            }
          }
        },
      },
      // The remaining top-level fields come from bleve.SearchRequest.
      "query": : {
        "boost": 1.0,
        "query": "your bleve query string here"
      },
      "size": 10,         // Limit the number of results that are returned.
      "from": 0,          // Offset position of the first result returned,
                          // starting from 0, used for resultset paging.
      "highlight": null,  // See bleve's Highlight API for result snippet highlighting.
      "fields": [ "*" ],  // Restricts the fields that bleve considers during the search.
      "facets": null,     // For bleve's faceted search features.
      "explain": false    // When true, bleve will provide scoring explaination.
    }

### Scoring

TBD

### Pagination

TBD

### Results highlighting

TBD

### Faceted searches

TBD

# Index document counts

TBD

# Index consistency

TBD

---

Copyright (c) 2015 Couchbase, Inc.

# Index queries

You can query indexes either using cbft's REST API or using its web
admin UI.

## Web admin UI queries

You can use cbft's web admin UI in order to query an index using your
web browser:

- Navigate to the ```Indexes``` screen.

- Click on your index's name link to naviate to the summary screen for
  your index.

- Click on the ```Query``` sub-tab for your index.

- Enter your query statement in the input field.

The kind of query statement you should enter depends on the index
type.

Please see below in this document for more information on the query
statement requirements for different index types.

- You can click on the ```Advanced``` checkbox in order to see
  more optional, advanced query parameters.

## REST API queries

Please see the [REST API reference](api-ref) for documentation on
programmatically POST'ing query requests to cbft.

You can POST a query request to any cbft node in a cbft cluster.

To process your query request, that cbft node will then in turn
perform distributed requests against the other nodes in the cbft
cluster and gather their responses.

Your REST client will then receive a processed, merged response from
the cbft node that received the original REST query request.

Programmatically, the POST body for REST API bleve queries would look
similar to the following example JSON:

    {
      "timeout": 0,
      "consistency": {
        "level": "",
        "vectors": {}
      },
      "query": ...
    }

There are several fields:

- ```timeout``` - integer, timeout in milliseconds, 0 for no timeout.

- ```consistency``` - a JSON sub-object to ensure that the index has
  reached a consistency level required by the application, where the
  index has incorporated enough required data from the data source of
  the index.  See below in this document for more information.

- ```query``` - a JSON sub-object or string whose interpretation
  depends on the index type of your index.

# Index types and queries

## Index type: bleve

When using the ```bleve``` index type, the query statement that you
use in cbfts web admin UI must follow bleve's full-text index [query
string](https://github.com/blevesearch/bleve/wiki/Query%20String%20Query)
syntax rules.

See: [https://github.com/blevesearch/bleve/wiki/Query%20String%20Query](https://github.com/blevesearch/bleve/wiki/Query%20String%20Query)

tbd - worked examples of queries and their JSON.

    {
      "timeout": 0,
      "consistency": {
        "level": "",
        "vectors": {}
      },
      "query": {
        "query": null,
        "size": 0,
        "from": 0,
        "highlight": null,
        "fields": null,
        "facets": null,
        "explain": false
      }
    }

### Scoring

tbd

### Pagination

tbd

### Results highlighting

tbd

### Faceted searches

tbd

# Index document counts

tbd

# Index consistency

tbd

---

Copyright (c) 2015 Couchbase, Inc.

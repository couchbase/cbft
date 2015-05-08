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
      "timeout": 0,
      "consistency": {
        "level": "",
        "vectors": {}
      },
      "query": ...
    }

There are several common fields:

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
use in cbft's web admin UI must follow bleve's full-text index [query
string](https://github.com/blevesearch/bleve/wiki/Query%20String%20Query)
syntax rules.

See: [https://github.com/blevesearch/bleve/wiki/Query%20String%20Query](https://github.com/blevesearch/bleve/wiki/Query%20String%20Query)

TBD - add worked examples of queries and their resuling JSON.

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

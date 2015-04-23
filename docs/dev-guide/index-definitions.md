# Index definitions

## Index properties

## Index Name

An index has a name, or _Index Name_, that is a unique identifier for
the index.

An index name is comprised of alphanumeric characters, hyphens and
underscores (no whitespace characters).

## Index Type

An index has a type, or _Index Type_.

An often used index type, for example, would be ```bleve```, for
full-text indexing.

Some available index types include...

- ```bleve``` - a full-text index powered by the
  [bleve](http://blevesearch.com) engine.

- ```blackhole``` - for testing; a blackhole index type ignores all incoming
  data, and returns errors on any queries.

- ```alias``` - an index alias provides a naming level of indirection to one
  or more actual, target indexes.

## Index Params

An index has optional _Index Params_.

The meaning of the index params depends on the index type.

For example, if the index type is ```bleve```, then the index params
includes the JSON mapping information that is used to configure the
bleve full-text engine.

For example, if the index type is ```alias```, then the index params
is the JSON that defines one or more target indexes for the index
alias.

## Source Type

An index has a _Source Type_, which specifies the kind of data source
that is used to populate the index.

An often used source type, for example, would be ```couchbase```,
which would be used when a user wants to index all the documents that
are stored in a Couchbase bucket.

Some available source types include...

- ```couchbase``` - a Couchbase Server bucket will be the data source.
- ```nil``` - for testing; a nil data source never has any data.

## Source Name and Source Params

An index also has _Source Name_ and optional _Source Params_.

The meaning of the source name and source params depend on the source
type of the index.

For example, when the source type is "couchbase", then the source name
is treated as a Couchbase bucket name, and the source params would
define any optional, additional parameters needed to connect that
named Couchbase bucket.

## Index definition operations

You can list, retrieve, create and delete index definitions in cbft
using its REST API or using its web admin UI.

Please see the [REST API reference](api-ref/#index-definition) for
documentation on GET'ing, PUT'ing, and DELETE'ing index definitions.

# Using the web admin UI

To start cbft, please see the [getting started](../index.md) guide.

## To create a new index

- Navigate to the ```Indexes``` screen.

- Click on the ```New Index``` button.

## Updating index definitions

## Deleting indexes

## Index aliases

tbd

## Index definition tips & tricks

tbd

---

Copyright (c) 2015 Couchbase, Inc.

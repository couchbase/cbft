# Index definitions

## Index attributes

An index has several attributes that a user needs to specify when
defining an index:

- Index Name - required
- Index Type - required
- Index Params
- Source Type - required
- Source Name
- Source UUID
- Source Params
- Plan Params

And, an index has system-generated properties when the an index is
first created:

* Index UUID

### Index Name (indexName)

An index has a name, or _Index Name_, that is a unique identifier for
the index.

An index name is comprised of alphanumeric characters, hyphens and
underscores (no whitespace characters).

### Index Type (indexType)

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

### Index Params (indexParams)

An index has optional _Index Params_.

The meaning of the index params depends on the index type.

For example, if the index type is ```bleve```, then the index params
includes the JSON mapping information that is used to configure the
bleve full-text engine.

For example, if the index type is ```alias```, then the index params
is the JSON that defines one or more target indexes for the index
alias.

### Source Type (sourceType)

An index has a _Source Type_, which specifies the kind of data source
that is used to populate the index.

An often used source type, for example, would be ```couchbase```,
which would be used when a user wants to index all the documents that
are stored in a Couchbase bucket.

Some available source types include...

- ```couchbase``` - a Couchbase Server bucket will be the data source.
- ```nil``` - for testing; a nil data source never has any data.

### Source Name (sourceName)

An index also has an optional _Source Name_, whose meaning is
dependent on the source type of the index.

For example, when the source type is "couchbase", then the source name
is treated as a Couchbase bucket name, and the source params would
define any optional, additional parameters needed to connect that
named Couchbase bucket.

### Source UUID (sourceUUID)

### Source Params (sourceParams)

An index also has an optional _Source Params_, whose meaning is
dependent on the source type of the index.

The Source Params allow for extra parameters to be defined.

Most of these are advanced connection and tuning parameters.

However, if you'd like to index a non-default bucket that has a
password, then you need to supply an ```authUser``` and possibly an
```authPassword```.

For example, perhaps you'd like to index the ```beer-sample``` bucket.

Then, in the Source Params JSON textarea...

- specify the "authUser" to be the bucket's name
  (```"beer-sample"```).

- specify the "authPassword" to be the bucket's password (the empty
  password is just ```""```).

For example, your Source Params JSON would then look like...

    {
      "authUser": "beer-sample",
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

### Plan Params (planParams)

### Index UUID (indexUUID)

The cbft system generates and assigns a unique _Index UUID_ to an
index when an index is first created and every time the index
definition is updated.

That is, an edit or update of an index definition by a user would
result in a new Index UUID being generated and assigned to the index.

# Index definition operations

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

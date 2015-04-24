# Index definitions

## Index attributes

An index has several attributes that a user can specify when defining
an index:

* Index Name - required
* Index Type - required
* Index Params
* Source Type - required
* Source Name
* Source Params
* Source UUID
* Plan Params

And, an index will have (read-only) system-generated attributes when
the an index is first created:

* Index UUID

### Index Name (indexName)

An index has a name, or _Index Name_, that is a unique identifier for
the index.

An index name is comprised of alphanumeric characters, hyphens and
underscores (no whitespace characters).  The first character of an
index name must be an alphabetic character (a-z or A-Z).

### Index Type (indexType)

An index has a type, or _Index Type_.

An often used index type, for example, would be ```bleve```, for
full-text indexing.

Some available index types include...

- ```bleve``` - a full-text index powered by the
  [bleve](http://blevesearch.com) engine.

- ```blackhole``` - for testing; a blackhole index type ignores all
  incoming data, and returns errors on any queries.

- ```alias``` - an index alias provides a naming level of indirection
  to one or more actual, target indexes.

### Index Params (indexParams)

An index has an optional _Index Params_ JSON attribute.

The meaning of the index params depends on the index type.

For example, if the index type is ```bleve```, then the index params
includes the JSON mapping information that is used to configure the
bleve full-text engine.

For example, if the index type is ```alias```, then the index params
should be the JSON that defines one or more target indexes for the
index alias.

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

### Source Params (sourceParams)

An index also has an optional _Source Params_ JSON attribute, whose
meaning is dependent on the source type of the index.

The Source Params allow for extra parameters to be defined, and are
usually advanced connection and tuning parameters for configuring how
cbft should retrieve data from a data source.

For example, when your source type is ```couchbase```, and you'd like
to index a Couchbase bucket that has a password, then you need to
specify an ```authUser``` and an ```authPassword``` as part of the
source params JSON.

For example, perhaps you'd like to index the ```beer-sample``` bucket.

Then, in the Source Params JSON...

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

### Source UUID (sourceUUID)

An index also has an optional _Source UUID_ attribute, whose meaning
is dependent on the source type of the index.

For example, when the source type is "couchbase", then the source
UUID, which is optional, is treated as a Couchbase bucket UUID, in
order to allow a strict identification of the correct bucket.

### Plan Params (planParams)

An index has a _Plan Params_ JSON attribute, by which a user can
specify how cbft should plan to partition the index and to allocate
index partitions across cbft nodes in a cbft cluster.

An example plan params JSON:

    {
        "maxPartitionsPerPIndex": 20,
        "numReplicas": 1,
        "hierarchyRules": null,
        "nodePlanParams": null,
        "planFrozen": false
    }

The fields in a plan params include:

* maxPartitionsPerPIndex: integer >= 0

* numReplicas: integer >= 0

* hierarchyRules: JSON object

* nodePlanParams: JSON object

* planFrozen: bool, frozen == true

### Index UUID (indexUUID)

The cbft system generates and assigns a unique _Index UUID_ to an
index when an index is first created and whenever the index definition
is updated.

That is, if you edit or update an index definition, the cbft system
will re-generate a new Index UUID for changed index definition.

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

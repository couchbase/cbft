# Index definition operations

You can list, retrieve, create and delete index definitions in cbft
using its REST API or using its web admin UI.

Please see the [REST API reference](api-ref) for
documentation on programmatically GET'ing, PUT'ing, and DELETE'ing
index definitions.

## Using the web admin UI

To start cbft and its included web admin UI, please see the [getting
started](../index.md) guide.

## To create a new index

The [getting started](../index.md) guide also includes a basic
tutorial on creating a full-text (bleve) index.

To create an index in the web admin UI, using your web browser:

- Navigate to the ```Indexes``` screen.

- Click on the ```New Index``` button.

## Editing, cloning, deleting index definitions

To edit an index definition in the web admin UI, using your web
browser:

- Navigate to the ```Indexes``` screen.

- There you will find a list of indexes.

Each listed index will have buttons for these operations:

- the ```edit``` button allows you to update an index definition.

- the ```clone``` button allows you to copy an index definition.

- the ```delete``` button allows you to delete an index definition.

# Index attributes

An index has several attributes, several of which are required to be
specified when defining index:

* Index Name - _required attribute_
* Index Type - _required attribute_
* Index Params
* Source Type - _required attribute_
* Source Name
* Source Params
* Source UUID
* Plan Params
* Index UUID - _system generated / read-only_

## Index Name (indexName)

An index has a name, or _Index Name_, that is a unique identifier for
the index.

An index name is comprised of alphanumeric characters, hyphens and
underscores (no whitespace characters).  The first character of an
index name must be an alphabetic character (a-z or A-Z).

## Index Type (indexType)

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

More information on the ```bleve``` and ```alias``` index types are
available further below in this document.

## Index Params (indexParams)

An index has an optional _Index Params_ JSON attribute.

The interpretation of the index params depends on the index type.

For example, if the index type is ```bleve```, then the index params
includes the JSON mapping information that is used to configure the
bleve full-text engine.

For example, if the index type is ```alias```, then the index params
should be the JSON that defines one or more target indexes for the
index alias.

## Source Type (sourceType)

An index has a _Source Type_, which specifies the kind of data source
that is used to populate the index.

An often used source type, for example, would be ```couchbase```,
which would be used when a user wants to index all the documents that
are stored in a Couchbase bucket.

Some available source types include...

- ```couchbase``` - a Couchbase Server bucket will be the data source.
- ```nil``` - for testing; a nil data source never has any data.

More information on the ```couchbase``` source type is
available further below in this document.

## Source Name (sourceName)

An index also has an optional _Source Name_, whose interpretation is
dependent on the source type of the index.

For example, when the source type is "couchbase", then the source name
is treated as a Couchbase bucket name, and the source params would
define any optional, additional parameters needed to connect that
named Couchbase bucket.

## Source Params (sourceParams)

An index also has an optional _Source Params_ JSON attribute, whose
interpretation depends on the source type of the index.

The Source Params allow for extra parameters to be defined, and are
usually advanced connection and tuning parameters for configuring how
cbft should retrieve data from a data source.

## Source UUID (sourceUUID)

An index also has an optional _Source UUID_ attribute, whose meaning
depends on the source type of the index.

For example, when the source type is "couchbase", then the source
UUID, which is optional, is treated as a Couchbase bucket UUID, in
order to allow a strict identification of the correct bucket.

## Plan Params (planParams)

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

* ```maxPartitionsPerPIndex```: integer >= 0
* ```numReplicas```: integer >= 0
* ```hierarchyRules```: JSON object
* ```nodePlanParams```: JSON object
* ```planFrozen```: bool, frozen == true

```maxPartitionsPerPIndex``` limits how many source partitions that
cbft can assign to an index partition.  A value of 0 means no limit,
which effectively means that cbft will just assign all source
partitions (for example, 1024 Couchbase "vbuckets") to a single index
partition.

For example, with a Couchbase bucket as your data source, you will
have 1024 source partitions (1024 Couchbase "vbuckets").  If your
```maxPartitionsPerPIndex``` is 200, then you would have 6 index
partitions...

- index partition A covers vbuckets 0 through 199
- index partition B covers vbuckets 200 through 399
- index partition C covers vbuckets 400 through 599
- index partition D covers vbuckets 600 through 799
- index partition E covers vbuckets 800 through 999
- index partition F covers vbuckets 1000 through 1023

Note: cbft acutally uses unique hexadecimal hashes (like
"beer-sample_58dc74c09923851d_607744fc") to identify index partitions
instead of simple alphabetic characters as shown above (the 'A'
through 'F' above).

```numReplicas``` defines how many index partitions replicas cbft
should allocate, not counting the first, master index partition.  For
example, a value of 1 means there should be two copies: the first
master copy plus 1 replica copy.

```hierarchyRules``` defines replica allocation rules or policies for
shelf/rack/row/zone awareness.

TBD

```nodePlanParams``` defines rules on whether a node is paused for
index ingest and/or index queries for a given index or index
partition.

TBD

```planFrozen``` defines whether an index is frozen or paused for
reassignment or rebalancing of index partitions.

## Index UUID (indexUUID)

The cbft system generates and assigns a unique _Index UUID_ to an
index when an index is first created and whenever the index definition
is updated.

That is, if you edit or update an index definition, the cbft system
will re-generate a new Index UUID for changed index definition.

When using the REST API to edit or update an index definition, you can
pass in the index definition's current ```indexUUID``` via the
```prevIndexUUID``` parameter to ensure that concurrent clients are
not inadvertently overwriting each other's changes to an index
definition.

# Index types

## Index type: bleve

tbd

## Index type: alias

tbd

# Source types

## Source type: couchbase

When your source type is ```couchbase```, and you'd like to index a
Couchbase bucket that has a password, then you need to specify an
```authUser``` and an ```authPassword``` as part of the Source Params
(```sourceParams```) JSON.

For example, perhaps you'd like to index a ```product-catalog``` bucket.

Then, in the Source Params JSON...

- specify the "authUser" to be the bucket's name
  (```"product-catalog"```).

- specify the "authPassword" to be the bucket's password, such as
  "PassWordSellMore" (the empty password is just ```""```).

For example, your Source Params JSON would then look like...

    {
      "authUser": "product-catalog",
      "authPassword": "PassWordSellMore",
      "clusterManagerBackoffFactor": 0,
      "clusterManagerSleepInitMS": 0,
      "clusterManagerSleepMaxMS": 20000,
      "dataManagerBackoffFactor": 0,
      "dataManagerSleepInitMS": 0,
      "dataManagerSleepMaxMS": 20000,
      "feedBufferSizeBytes": 0,
      "feedBufferAckThreshold": 0
    }

The other parameters are for specifying optional, advanced connection
tuning and error-handling behavior for Couchbase DCP streams:

- ```clusterManagerBackoffFactor```: float - numeric factor (like 1.5)
  for how cbft should increase its sleep time in between retries when
  (re-)connecting to a Couchbase cluster manager.

- ```clusterManagerSleepInitMS```: int - initial sleep time
  (millisecs) before first retry to a Couchbase cluster manager.

- ```clusterManagerSleepMaxMS```: int - maximum sleep time (millisecs)
  between re-connection attempts to a Couchbase cluster manager.

- ```dataManagerBackoffFactor```: float - numeric factor (like 1.5)
  for how cbft should increase its sleep time in between retries when
  (re-)connecting to a Couchbase data manager node
  (memcached/ep-engine).

- ```dataManagerSleepInitMS```: int - initial sleep time (millisecs)
  before first retry to a Couchbase data manager.

- ```dataManagerSleepMaxMS```: int - maximum sleep time (millisecs)
  between re-connection attempts to a Couchbase data manager.

- ```feedBufferSizeBytes```: int - buffer size in bytes provided
  for DCP flow control.

- ```feedBufferAckThreshold```: float - used for DCP flow control and
  buffer-ack messages when this percentage of FeedBufferSizeBytes is
  reached.

---

Copyright (c) 2015 Couchbase, Inc.

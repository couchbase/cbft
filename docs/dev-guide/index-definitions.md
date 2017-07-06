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

- There you will find a list of the indexes, if any, that you had
  previously defined.

Each listed index will have buttons for these operations:

- the index ```edit``` button allows you to update an index
  definition.

- the index ```clone``` button allows you to copy an index definition.

- the index ```delete``` button (trash can icon) allows you to delete
  an index definition.

Note: if you click on the index ```delete``` button, you will have a
chance to first confirm the index definition deletion operation.
Deleting an index, importantly, is a permanent operation.

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

An index has a required name, or _Index Name_, which is a unique
identifier for the index.

An index name is comprised of alphanumeric characters, hyphens and
underscores (no whitespace characters).  The first character of an
index name must be an alphabetic character (a-z or A-Z).

## Index Type (indexType)

An index has a required type, or _Index Type_.

An often used index type, for example, would be ```bleve```, for
full-text indexing.

Some available index types include...

- ```bleve``` - a full-text index powered by the
  [bleve](http://blevesearch.com) full-text engine.

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

More information on the ```bleve``` and ```alias``` index params JSON are
available further below in this document.

## Source Type (sourceType)

An index has a required _Source Type_, which specifies the kind of
data source that is used to populate the index.

An often used source type, for example, would be ```couchbase```,
which would be used when a user wants to index all the documents that
are stored in a Couchbase bucket.

Some available source types include...

- ```couchbase``` - a Couchbase Server bucket will be the data source.
- ```nil``` - for testing; a nil data source never has any data.

More information on the ```couchbase``` source types are available
further below in this document.

## Source Name (sourceName)

An index also has an optional _Source Name_, whose interpretation is
dependent on the source type of the index.

For example, when the source type is ```couchbase```, then the source
name is treated as a Couchbase bucket name, and the source params
would define any optional, additional parameters needed to connect
that named Couchbase bucket.

## Source Params (sourceParams)

An index also has an optional _Source Params_ JSON attribute, whose
interpretation depends on the source type of the index.

The Source Params allow for extra parameters to be defined, and are
usually advanced connection and tuning parameters for configuring how
cbft should retrieve data from a data source.

## Source UUID (sourceUUID)

An index also has an optional _Source UUID_ attribute, whose meaning
depends on the source type of the index.

For example, when the source type is ```couchbase```, then the source
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

```maxPartitionsPerPIndex```, or "max number of source partitions per
index partition", limits how many source partitions that cbft can
assign to or allocate toto an index partition.

A value of 0 means no limit, which effectively means that cbft will
just allocate all source partitions to a single index partition.

For example, with a Couchbase bucket as your data source (the source
type is ```couchbase```), you will have 1024 source partitions (1024
Couchbase "vbuckets"), and then:

If your ```maxPartitionsPerPIndex``` is 0, then you would have a
single index partition that will be assigned to be responsible for all
vbuckets.

If your ```maxPartitionsPerPIndex``` is 200, then you would have 6
index partitions...

- index partition A covers vbuckets 0 through 199
- index partition B covers vbuckets 200 through 399
- index partition C covers vbuckets 400 through 599
- index partition D covers vbuckets 600 through 799
- index partition E covers vbuckets 800 through 999
- index partition F covers vbuckets 1000 through 1023

cbft actually uses unique hexadecimal hashes (like
"beer-sample_58dc74c09923851d_607744fc") to identify those index
partitions instead of simple alphabetic characters as shown above (the
'A' through 'F' above).

The format of the index partition identifier is...

    <indexName>_<indexUUID>_<hash-of-source-partition-identifiers>

```numReplicas``` defines how many additional index partitions
replicas cbft should allocate, not counting the first assigned index
partition.

For example, a value of 1 for ```numReplicas``` means there should be
two copies: the first copy plus 1 replica copy of the index
partitions.

For example, a value of 0 for ```numReplicas``` means there should be
only the first copy and no extra replica copies of the index
partitions.

```hierarchyRules``` defines replica allocation rules or policies for
shelf/rack/row/zone awareness.

TBD

```nodePlanParams``` defines rules on whether a node is paused for
index ingest and/or index queries are allowed for a given index or
index partition.

TBD

```planFrozen``` defines whether an index is frozen or paused for
automatic reassignment or rebalancing of index partitions.

## Index UUID (indexUUID)

The cbft system generates and assigns a unique _Index UUID_ to an
index when an index definition is first created and whenever the index
definition is updated.

That is, if you edit or update an index definition, the cbft system
will re-generate a new Index UUID for the changed index definition.

When using the REST API to edit or update an index definition, you can
optionally pass in the index definition's current ```indexUUID``` via
the ```prevIndexUUID``` parameter on your request to ensure that
concurrent clients are not inadvertently overwriting each other's
changes to an index definition.

# Index types

## Index type: bleve

For the ```bleve``` index type, here is an example, default index
params JSON:

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
        "analysis": {}
      },
      "store": {
        "kvStoreName": "boltdb"
      }
    }

There are two "top-level" fields in that bleve index params JSON:

- ```mapping```
- ```store```

The ```mapping``` field is a JSON sub-object and is a representation
of bleve's ```IndexMapping``` configuration settings.

That is, the value of the ```mapping``` field is passed directly to
the bleve full-text engine's ```IndexMapping``` parser.

A bleve ```IndexMapping``` is a complete, standalone, declarative
configuration of a logical bleve full-text index.

Please see [bleve's documentation](http://blevesearch.com) for more
information on the ```IndexMapping```.

The ```store```field is a JSON sub-object and is a representation of
bleve's ```kvconfig``` configuration settings.

NOTE: For web admin UI users, the ```store``` field input textarea is
hidden by default.  To make it visible, click on the ```Show advanced
settings``` checkbox in the index creation form or index edit form.

The ```store``` field has an important sub-field: ```kvStoreName```.

The ```kvStoreName``` defines the persistent storage implementation
that will be used for the bleve index.

Allowed values for ```kvStoreName``` include:

- ```"boltdb"``` - a pure-golang key-value storage library

- ```"mem"``` - a memory-only "storage" implementation, that does not
  actually persist index data entries to disk.

- ```"goleveldb"``` - a pure-golang re-implementation of the leveldb
  storage library (EXPERIMENTAL)

- TBD (leveldb, ...others...)

The other sub-fields under the ```store``` JSON sub-object are
dependent on the persistent storage implementation that is being used.

Note: underneath the hood, both the parsed ```mapping``` and
```store``` objects are used when cbft invoke's bleve's ```NewUsing```
API when cbft needs to construct a new full-text index.

## Index type: alias

For the ```alias``` index type, here is an example, default index
params JSON:

    {
      "targets": {
        "%yourIndexName%": {
          "indexUUID": ""
        }
      }
    }

You can specify one or more "%yourIndexName%" entries listed under the
```targets``` sub-object.

For example, perhaps you wish to have a naming level-of-indirection so
that applications can make queries without any application-side
reconfigurations.

In one scenario, perhaps you have a sales management application that
makes queries against a "LastQuarterSales" alias.  The alias is
targeted against a "sales-2014-Q4" index, such as...

    {
      "targets": {
        "sales-2014-Q4": {
          "indexUUID": ""
        }
      }
    }

Later, when 2015 Q1 sales data is completed (end of the quarter) and a
new index is built, "sales-2015-Q1", then the "LastQuarterSales" alias
can be retargetted by the administrator to point to the latest
"sales-2015-Q1" index...

    {
      "targets": {
        "sales-2015-Q1": {
          "indexUUID": ""
        }
      }
    }

The optional ```indexUUID``` field in the index alias definition
allows you to exactly specify a specific target index definition via
the target index definition's ```indexUUID```.

If the target index is redefined and its ```indexUUID``` value is
regenerated or reassigned by cbft, then queries against an index alias
with a mismatched indexUUID will result in error responses.

### Multi-target index alias

You can also have an index alias point to more than one target index.

For example, perhaps you wish to have a LastSixMonthsSales alias.  It
can be configured to point to the last two quarters of real indexes...

    {
      "targets": {
        "sales-2014-Q4": {
          "indexUUID": ""
        },
        "sales-2015-Q1": {
          "indexUUID": ""
        }
      }
    }

This is also useful for situations where you have indexes holding data
from different datasources, such as a "product-catalog-index",
"customer-index", "employee-index", "intranet-docs-index".

You can then have a single index alias that points to all the above
target indexes so that applications can query just a single endpoint
(the index alias).

# Source types

## Source type: couchbase

When your source type is ```couchbase```, and you'd like to index a
Couchbase bucket that has a password, then you need to specify an
```authUser``` and an ```authPassword``` as part of the Source Params
(```sourceParams```) JSON.

For example, perhaps you'd like to index a ```product-catalog```
Couchbase bucket.

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
      "feedBufferAckThreshold": 0,
      "noopTimeIntervalSecs":1
    }

The other parameters are for specifying optional, advanced connection
tuning and error-handling behavior for how cbft creates and manages
Couchbase DCP (data change protocol) streams:

- ```clusterManagerBackoffFactor```: float - numeric factor (like 1.5)
  for how cbft should increase its sleep time in between retries when
  re-connecting to a Couchbase cluster manager.

- ```clusterManagerSleepInitMS```: int - initial sleep time
  (millisecs) for cbft's first retry on re-connecting to a Couchbase
  cluster manager.

- ```clusterManagerSleepMaxMS```: int - maximum sleep time (millisecs)
  between re-connection attempts to a Couchbase cluster manager.

- ```dataManagerBackoffFactor```: float - numeric factor (like 1.5)
  for how cbft should increase its sleep time in between retries when
  re-connecting to a Couchbase data manager node
  (memcached/ep-engine server).

- ```dataManagerSleepInitMS```: int - initial sleep time (millisecs)
  for cbft's first retry on re-connecting to a Couchbase data manager.

- ```dataManagerSleepMaxMS```: int - maximum sleep time (millisecs)
  between re-connection attempts to a Couchbase data manager.

- ```feedBufferSizeBytes```: int - buffer size in bytes provided
  for DCP flow control.

- ```feedBufferAckThreshold```: float - used for DCP flow control and
  buffer-ack messages when this percentage of
  ```feedBufferSizeBytes``` is reached.

- ```noopTimeIntervalSecs```: int - time interval in seconds of NO-OP
  messages for DCP flow control, needs to be set to a non-zero value
  to enable no-ops.

## Index definition REST API

You can use the REST API to create and manage your index definitions.

For example, using the curl command-line tool, here is an example of
creating an index definition.  The index definition will be named
```beer-sample```, will have index type of
[bleve](http://blevesearch.com), and will have the "beer-sample"
bucket from Couchbase as its datasource...

    curl -XPUT 'http://localhost:8094/api/index/myFirstIndex?indexType=bleve&sourceType=couchbase'

To list all your index definitions, you can use...

    curl http://localhost:8094/api/index

Here's an example of using curl to delete that index definition...

    curl -XDELETE http://localhost:8094/api/index/beer-sample

For more information on the REST API, please see the
[REST API reference](api-ref) documentation.

---

Copyright (c) 2015 Couchbase, Inc.

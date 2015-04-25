# Clustering cbft

To run multiple cbft nodes in a cluster, you need to use a Cfg
provider that supports clustering.

Of note: the default ```simple``` Cfg provider that often used for
developer environments is local-only and is non-cluster'able.

## Setting up a Couchbase Cfg provider

The ```couchbase``` Cfg provider supports clustering.  It uses a
Couchbase bucket to store cbft's configuration data, and multiple cbft
nodes are all pointed to the same Couchbase bucket to coordinate and
rendevous the cluster-wide configuration information.  To use the
```couchbase``` Cfg provider:

- Pick a name for the Couchbase bucket that will be used to store cbft
  cluster configuration data.  For example, ```cfg-bucket```.

- Using Couchbase's web UI or tools, create the ```cfg-bucket```.

- Start cbft with the ```couchbase``` Cfg provider, pointed at the
  ```cfg-bucket```...

    ./cbft -cfg=couchbase:http://cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091

## Adding cbft nodes

On a different machine, you can next start another cbft node, pointed
at the same ```cfg-bucket```...

    ./cbft -cfg=couchbase:http://cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091

Since those two cbft nodes are using the same ```cfg-bucket``` as
their Cfg provider, those two cbft nodes are now clustered.

Additionally, you can run another cbft node on the same machine, but
specify a different, unique ```bindHttp``` port number and
```dataDir``` for each cbft node.  For example:

    mkdir -p /data/cbft-9090
    mkdir -p /data/cbft-9091
    mkdir -p /data/cbft-9092

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9090 \
           -bindHttp=:9090

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9091 \
           -bindHttp=:9091

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9092 \
           -bindHttp=:9092

## Availability of the Cfg provider

Of note, if the Couchbase cluster goes down, the cbft nodes will
continue to run and service query requests, but you will not be able
to make any cbft cluster configuration changes (modify index
definitions or add/remove cbft nodes) until the Couchbase cluster
returns to online, normal running health.

Another note, if the Couchbase bucket is deleted (e.g., someone
deletes the ```cfg-bucket```), the cbft nodes will continue to
independently run and service query requests, but you also will not be
able to make any cbft cluster configuration changes (modify index
definitions or add/remove cbft nodes).

Additionally, if the Couchbase bucket (```cfg-bucket```) is recreated
but in blank, new, empty state, or was flush'ed, the cbft nodes will
also reset to empty.  That is the Couchbase bucket (```cfg-bucket```)
is considered the _source of truth_ for the cbft cluster's
configuration.

## Removing cbft nodes

tbd

## Index replicas

tbd

## Restarts of cbft nodes

tbd

---

Copyright (c) 2015 Couchbase, Inc.

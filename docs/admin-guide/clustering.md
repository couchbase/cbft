# Clustering cbft

To run multiple cbft nodes in a cluster, you need to use a Cfg
provider that supports clustering.

Of note: the default ```simple``` Cfg provider that often used for
developer environments is local-only and is non-cluster'able.

## Setting up a Couchbase Cfg provider

The ```couchbase``` Cfg provider supports clustering.

The ```couchbase``` Cfg provider uses a Couchbase bucket to store
cbft's configuration data, where multiple cbft nodes that are all
pointed to the same Couchbase bucket will use the Couchbase bucket to
coordinate and rendevous on cbft's cluster-wide configuration
information.

To use the ```couchbase``` Cfg provider:

- Pick a name for the Couchbase bucket that will be used to store cbft
  cluster configuration data.  For example, ```cfg-bucket```.

- Using Couchbase's web UI or tools, create the ```cfg-bucket```.

- Choose the smallest memory resource quota for the ```cfg-bucket```
  (e.g., 100MB RAM).  The amount of data that will be stored in the
  ```cfg-bucket``` will be very small, even for large cluster sizes,
  as only cbft cluster configuration and metadata will be maintained
  in the ```cfg-bucket```.

- Next, start cbft with the ```couchbase``` Cfg provider, pointed at
  the ```cfg-bucket```...

    ./cbft -cfg=couchbase:http://cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -bindHttp=10.1.1.10:8095

## The bindHttp address and port

You must specify a valid bind IP address with the ```bindHttp```
command-line parameter for clustering to work.

The IP address allows clients and peer cbft nodes to contact the cbft
node using the provided ```bindHttp``` IP address.

Each node in the cbft cluster should have its own, unique
```bindHttp``` address and port.

## Adding cbft nodes

On a different machine, you can next start a second cbft node, pointed
at the same, shared ```cfg-bucket```...

    ./cbft -cfg=couchbase:http://cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -bindHttp=10.1.1.11:8095

Since those two cbft nodes are using the same ```cfg-bucket``` as
their Cfg provider, those two cbft nodes are now clustered.

## A cluster running on a single machine

Additionally, you can run another cbft node on the same machine, but
specify a different, unique ```bindHttp``` port number and
```dataDir``` for each cbft node.  For example:

    mkdir -p /data/cbft-9090
    mkdir -p /data/cbft-9091
    mkdir -p /data/cbft-9092

Then run the following three commands on the same machine, each in its
own terminal or shell session:

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9090 \
           -bindHttp=10.1.1.10:9090

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9091 \
           -bindHttp=10.1.1.10:9091

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9092 \
           -bindHttp=10.1.1.10:9092

The above will result in a three node cbft cluster all running on the
same machine.

This can be usefule for testing and for experimenting with cbft
cluster capabilities and behavior, without the cost of needing
separate machines.

## Availability of the Cfg provider

If the Couchbase cluster that powers the ```cfg-bucket``` goes down,
the cbft nodes will continue to run and service query requests, but
you will not be able to make any cbft cluster configuration changes
(modify index definitions or add/remove cbft nodes) until the
Couchbase cluster returns to online, normal running health.

Related, if the Couchbase ```cfg-bucket``` is accidentally deleted,
the cbft nodes will continue to independently run and service query
requests, but you will also not be able to make any cbft cluster
configuration changes (modify index definitions or add/remove cbft
nodes).

Additionally, if the Couchbase ```cfg-bucket``` is deleted and
recreated (so ends up in blank, new, empty state, or was flush'ed),
the cbft nodes will also reset to an empty, brand new cbft cluster
without any indexes defined.

Put another way, the Couchbase bucket (```cfg-bucket```) is considered
the _source of truth_ for the cbft cluster's configuration and is a
key platform dependency of the cbft cluster.

## cbft node states

Each cbft node is registered into the cbft cluster with node state.

The node states:

- ```wanted```: a cbft node in wanted state is expected to be part of
  the cluster and will have index partitions automatically assigned to
  it.

If a cbft node in wanted state temporarily disappears (e.g. machine or
process stops), it will not lose its previous index partition
assignments, but will be expected to come back soon (machine reboot
and/or process restart) so that it can continue servicing its
previously assigned index partitions.

This design approach was favored because a cbft node might have a
large amount of persistent index data that will be inefficient for
other cbft nodes to rebuild, and having index partition assignments
"bounce around" different cbft nodes would lead to wasteful thrashing
of resources.

- ```known```: a cbft node in known state is listed in the cluster,
  but will not be assigned any index partitions.

- ```unknown```: this is a node that is not part of the cluster, is
  not actually listed in cbft's cluster configuration data, and by
  definition, will not be assigned any index partitions.

## Removing cbft nodes

To remove a cbft node from a cluster, you need to change its
registered state to unknown.  To do so, if you had a cbft node running
previously as...

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9090 \
           -bindHttp=:9090

- First, stop the cbft node process (e.g., kill the process on linux).

- Then, run the cbft node with the additional command-line parameters
  of ```--register=unknown```

    ./cbft -cfg=couchbase:http://my-cfg-bucket@couchbase-01:8091 \
           -server=http://couchbase-01:8091 \
           -dataDir=/data/cbft-9090 \
           -bindHttp=10.1.1.10:9090 \
           -register=unknown

The key to removing a node with the ```--register=unknown```
command-line parameter is that your invocation must have the same
```-bindHttp```, ```-cfg```, and node UUID (stored previously in the
dataDir as the ```cbft.uuid``` file)

That will move the cbft node into ```unknown``` state in the cluster,
and any of the cbft node's previously assigned index partitions will
be re-assigned to other, remaining wanted cbft nodes in the cluster.

## Node identity

The cbft node's UUID and bindHttp (address:port) values must be unique
for a new cbft node to join a cluster.

Please see the administrator's guide on [running cbft
definitions](running) for more information on the node UUID and
bindHTTP values.

## Index replicas

Once you have more than one cbft node in a cluster, then you can
leverage cbft's index replica features.

Index replicas are indexes built up by just re-indexing the data again
from the original data source on a different cbft node.  That is, it
is not chain replication (A -> X -> Y) but is instead star replication
(A -> X; A -> Y).

To enable replicas for an index, you need to set the ```numReplicas```
configuration field for an index definition to greater than 0.  Please
see ```numReplicas``` documentation in the developer's guide on [index
definitions](../dev-guide/index-definitions) for more information.

---

Copyright (c) 2015 Couchbase, Inc.

# Running cbft

The [getting started](../index.md) guide provides a basic introduction
to starting cbft.

This document provides information on some additional considerations.

## cbft dependencies

The cbft program executable is designed to be a single, standalone,
self-sufficient binary image and should not require additional shared
libraries or pre-installed dependencies.

## Data directory

cbft requires a data directory where it can store and maintain local
configuration data and index data.

This data directory path is defined by the ```dataDir``` command-line
parameter.

If a ```dataDir``` is not specified, cbft will create a "data"
directory in its current working directory for its data directory.

Advanced users may wish to have a separate filesystem which can handle
larger data volumes and high storage I/O performance.

Users running in cloud-based environments may wish to consider
non-ephemeral, mounted block stores, such as EBS (Elastic Block Store)
on Amazon Web Services EC2 systems or equivalent for your cloud
hosting provider.

In a container-based environment, such as Docker or equivalent, users
should consider a non-ephemeral, high performance host volume for
their cbft ```dataDir``` data directory.

## Node UUID

An important file stored in the data directory is the ```cbft.uuid```
file.

The ```cbft.uuid``` file records the _Node UUID_ of the cbft node,
which is a unique, persistent identifier for a cbft node that is
critical for clustering multiple cbft nodes together.

The ```cbft.uuid``` file must remain the same and readable across
restarts of the cbft process for cbft clustering to work properly.

The first time a cbft is started, with an empty data directory, cbft
will detect that the ```cbft.uuid``` file does not exist and will
generate a new node UUID and save it to a new ```cbft.uuid``` file.

On a restart of the cbft node, the cbft node must find the same
```cbft.uuid``` file in order to have the same node UUID across
process restarts.

If you move the data directory (e.g., such as to move the data
directory a storage volume with more available storage space or higher
performance) be sure to also move/copy over the ```cbft.uuid``` file.

## BindHttp / Port numbers

cbft provides a HTTP/REST API endpoint, listening on the address:port
specified by the ```bindHttp``` command-line parameter.

The default value for ```bindHttp``` is "0.0.0.0:8094", so the default
bind address is 0.0.0.0 and default port number is 8094.

For example:

    ./cbft -bindHttp=0.0.0.0:8094 -server=http://cb-01:8091

For clustering and for remote application/client accessibility, you
must specify an actual IP address instead of the "0.0.0.0" (and also
not use "127.0.0.1"), as that supplied IP address will be used by
other cbft nodes and client applications to contact your cbft node.

For example, if your IP address is "10.1.1.10", then:

    ./cbft -bindHttp=10.1.1.10:8094 -server=http://cb-01:8091 \
           -cfg=couchbase:http://cfg-bucket@cb-01:8091

The port number (e.g., 8094) must also be enabled on your firewall
systems, if any, to allow remote access.

Each cbft node in a cbft cluster can have its own, different port
number.

Multiple cbft nodes can be run on a single machine and clustered
toegther, by giving each cbft node its own unique port number.  This
can be useful for testing.

## Securing cbft

WARNING / TODO: cbft Developer Preview release currently does not
provide security features (e.g., encryption, authentication).

---

Copyright (c) 2015 Couchbase, Inc.

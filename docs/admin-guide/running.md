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

Advanced users may wish to have a separate filesystem which can handle
large data volumes and high storage I/O performance.

Users running in cloud-based environments may wish to consider
non-ephemeral, mounted block stores, such as EBS (Elastic Block Store)
on Amazon Web Services EC2 systems or equivalent for your cloud
hosting provider.

In a container-based environment, such as Docker or equivalent, users
should consider a non-ephemeral, high performance host volume for
their cbft data directory.

## Port numbers

cbft provides a HTTP/REST API endpoint, listening on the address:port
specified by the ```bindHttp``` command-line parameter.  By default,
that is address 0.0.0.0 and port number 8095.

For example:

    ./cbft -bindHttp=0.0.0.0:8095 -server=http://cb-01:8091

For clustering and for application/client accessibility, that port
number (e.g., 8095) must be enabled for access by any firewall
systems.

Each cbft node in a cbft cluster can have its own, different port
number.  This allows users to test out cbft clusters on a local
machine, by specifying a different port number for each cbft node,
even though they're all running on the same machine.

## Securing cbft

WARNING / TODO: cbft Developer Preview release currently does not
provide security features (e.g., encryption, authentication) and
should also not be used in production settings.

---

Copyright (c) 2015 Couchbase, Inc.

# Increasing performance

There are several aspects to consider on increasing the performance of
your indexes.

Especially, there are often with competing tradeoffs at play, in
particular:

- indexing performance, or ingest performance - this is the speed at
  which the indexing system can incorporate data from a data source.

- query performance - this is how fast the system responds to a query.

Additionally, there are also the classic, additonal tradeoffs or
dimensions of speed: latency versus throughput.

Underlying it all, finally, is cost, regarding the resources that you
can deploy to meet a required level of performance and the
administration and maintenance of those resources.

There are several ways to address the above aspects...

## Scale up

A easy to understand approach, often without changing any other
application dependencies or configuration assumptions, is to increase
the power of each cbft node, with additional and/or more powerful
CPU's, more RAM, and faster storage (e.g., SSD's).

## Scale out

A related approach, in contrast, would be to add more nodes to a cbft
cluster.

These "scale out" nodes might individually be cheaper than "scale up"
machines, but collectively have more power as a cluster than a cluster
of powerful nodes.

Especially, with a granular level of index partitions (many index
partitions), those index partitions can be distributed across many
nodes to harness the collective CPU and I/O throughput of the
multiple, cheaper machines.  With enough networking bandwidth, this
can increase indexing or ingest throughput.

But, not all is free with scale-out: more index partitions spread out
across more nodes can be detrimental to query performance.

## Queries are scatter/gather

cbft must distribute a query request to every index partition
(scatter) and merge/coallesce the results (gather) before returning a
final response back to a query client.

With a scale-out cluster of many machines (in order to get higher
indexing throughput), this might reduce query performance.

## Query-only cbft nodes

Advanced: cbft has the ability to run nodes that are "query only".
These cbft nodes will not performance any indexing work nor directly
manage or maintain any stored index data, but only respond to query
requests and perform the querying scatter/gather work.

You can then use cheaper machines for these query-only nodes, as they
don't need fast I/O storage capabilities, so you can more easily have
many more query-only nodes deployed.

To run a cbft node in query-only mode, see the ```-tags```
command-line flag, with the ```queryer``` tag.  For example...

    ./cbft -tags queryer [...other cbft params...]

## Tighter selectivity

Consider, for example, the case where you're searching an index of a
customer service issues database.

If the search term is the word "problem", the it's likely that every
single index partition will have index entires for the "problem" term,
leading to a lot of results that need to be retrieved and
merged/collasced during a query of "problem".

In contrast, if the search term is something highly selective, like
the word "IRx0003914", then perhaps only a very few index partitions
with have index entries for that unique, uncommon term.  Most index
partitions will have empty results, which are much easy to
merge/coallesce at higher performance.

An application developer can leverage this behavior with UI or other
application approaches that send queries for special terms to
purposefully defined indexes that support tighter selectivity.

## Smaller indexes

A related approach to increase performance is to reduce index sizes

Some index types such as the ```bleve``` full-text engine include
features to filter away parts of documents, such as by ignoring
certain fields, not storing entire documents, and not storing
contextual information such as to enable search hit highlighting.

If the application requirements allow turning off or not utilizing
those advanced features, you can reduce index sizes to increase
performance.

Smaller index sizes can be more completely cached in memory, which
allows cbft to avoid expensive I/O storage accesses, and hence also
increase performance.

## Heterogeneous node capabilities

cbft also supports the ability for a cbft cluster to be comprised of a
mix of different machine capabilities, where some machines may be
bigger or more powerful than others.

cbft can utilize the more powerful machines by assigning those
machines more index partitions than the less powerful machines.

To tell cbft that a machine is more powerful, please see the
```-weight``` command-line flag.  For example...

    ./cbft -weight 2 [...other cbft params...]

## Relaxing consistency

cbft includes the optional ability when processing a query to ensure
that indexes are "caught up", where all the latest data from the data
source has been incorporated into the index, before the query
processing proceeeds.

This allows an application to "read your own writes" (RYOW), where
query results will reflect the latest mutations done by an application
thread.

However, if application requirements allow, cbft supports the ability
to instead query the index even if the index is out of date.  This may
be useful for some cases to increase the apparent or perceived
performance to the users of the overall application, at the tradeoff
of sometimes receiving older or "stale" index results.

## Advanced storage options

TBD

## Compacting data

TBD

## Testing and experiments

At the end of all these complex design tradeoffs and theories, the
ultimate answers will come from real-world data and results from
testing and experiments on actual hardware and datasets.

---

Copyright (c) 2015 Couchbase, Inc.

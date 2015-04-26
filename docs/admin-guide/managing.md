# Managing cbft

This document talks about the "levers and toggles" available to manage
cbft indexes and cbft nodes.

# Managing cbft indexes

## Index building

Index building is the process by which cbft sets up connections to
data sources, retrieves data from the data sources, analyzes that
data, and updates index data in memory and persisted to storage, in
concurrent fashion.

By concurrent, we mean that cbft uses internal pipelines or data
streams, as opposed to, for example, completely retrieving all the
data from a data source as a complete step before starting any
analysis.  Instead, cbft's approach allows for incremental processing,
which is suited to also handle updates or mutations of data from the
data source over time.

cbft will automatically start its concurrent index building steps as
soon as you create an index.

During the index building phase, the index is queryable.  However,
cbft may not have processed or ingested all the data, of course, from
the data source yet.  In this case, queries will return responses for
the data so far incorporated in a "best effort" fashion.

If a cbft node has to restart will in the midst of index building, the
restarted cbft will attempt to "pick up where it left off" as much as
possible and if the data source allows.

## Rebuilding indexes

Sometimes, an administrator wishes to rebuild an index from scratch,
or as is sometimes described, "rebuilding an index from zero" (in
reference to an empty start condition and/or to a 0 sequence number or
very beginning of a sequence of data).

To rebuild an index from scratch in cbft, using your web browser in
the web admin UI:

- Navgiate to the ```Indexes``` page.

- Find the row for the index you wish to rebuild.

- On that same row, click on the ```edit``` button, where you'll next
  see an edit form for you index definition.

- Without making any changes to your index definition form (especially
  don't change the index name), click on the ```Update Index``` button
  at the bottom of the form.

- Your index will start rebuilding from scratch or from zero.

- Of note, the index UUID will change as part of this operation, which
  allows applications to detect that the index definition has
  "changed".  This may be important for some applications that are
  caching results, etc.

## Disabling/enabling indexing

Sometimes, an administrator needs to pause index maintenance or
ingest.  For example, perhaps during key hours of heavy traffic, the
administrator would like to dedicate more resources to query
performance, at the cost of an out of date index.

To pause index ingest activities, in the web admin UI:

- Navigate to the ```Indexes``` page.

- Click on the index name link for the index whose ingest you wish to
  pause.

- Click on the ```Manage``` sub-tab for the index.

- Click ```Disable Ingest``` button.

The ```Index Ingest``` state will turn from ```enabled``` to
```disabled```, and the button you just clicked will turn into an
```Enable Ingest``` button.

To re-enable index processing and data ingest:

- Click on the ```Enable Ingest``` button.

## Disabling/enabling queries

Sometimes, an administrator needs to temporarily disable the ability
for applications to make queries on an index.  For example, perhaps
during some 2:00AM hours of maintenance time, the administrator would
like to index processing or ingest to "catch up" to the most recent
data source mutations.  Or perhaps, the administrator would like to
rebuild indexes from scratch at this point.  In any case, by disabling
queries, the administrator can help ensure more resources are going to
index ingest processing.  That is, a slow, massive query might steal
system resources from the main priority of getting the index up to
date.

To disable index query'ability, in the web admin UI:

- Navigate to the ```Indexes``` page.

- Click on the index name link for the index whose querying you wish
  to disable.

- Click on the ```Manage``` sub-tab for the index.

- Click ```Disable Queries``` button.

The ```Index Queries``` state will turn from ```enabled``` to
```disabled```, and the button you just clicked will turn into an
```Enable Queries``` button.

At this time, applications sending query requests for your index will
receive error responses.

To re-enable queries on your index:

- Click on the ```Enable Queries``` button.

## Disabling/enabling partition reassignments

Normally, as cbft nodes are added or removed from a cbft cluster, the
cbft system automatically rebalances and reassigns index partitions to
the remaining nodes in the cluster.

Sometimes, an administrator needs to disable this automatic index
partition reassignment, on a per index basis, because reassigned index
partitions need to go through index building, and any queries during
an index rebuild will see only the responses for data that have been
indexed so far.

To disable index parittion reassignments, in the web admin UI:

- Navigate to the ```Indexes``` page.

- Click on the index name link for the index whose index partitions
  you wish to to have reassignments disabled.

- Click on the ```Manage``` sub-tab for the index.

- Click ```Disable Reassignments``` button.

The ```Index Partition Reassignments``` state will turn from
```enabled``` to ```disabled```, and the button you just clicked will
turn into an ```Enable Reassignments``` button.

At this time, any cluster membership changes (cbft nodes added or
removed) will not trigger an automatic index partitions reassignment
for the index.

To re-enable index partition reassignments on your index:

- Click on the ```Enable Reassignments``` button.

## Node/cluster changes and zero downtime

tbd

## Index definition changes and zero downtime

tbd

## Advanced storage options

EXPERIMENTAL!

tbd

## Compacting data

tbd

## Backup and restore

cbft includes various REST API endpoints to be able request the cbft
cluster's current configuration and index definitions, which can be
used for restoration purposes.

The Cfg provider (i.e., a Couchbase ```my-cfg-bucket```) should also
have replication enabled and be backed up for production usage.

Because cbft is used as an indexing server, the index data entries
maintained by cbft should also be able to be rebuilt "from scratch"
from the original "source of truth" data sources.

At root, though, the end-all/be-all safety net and recommended
practice is that cbft index creation scripts should be checked into
source-code control systems so that any development, test or
administration colleagues on the user's team can replicate a cbft
configuration at will.

# Managing cbft nodes

## Forcing a manager kick

A button to ```Kick Manager``` is available in the web admin UI, on
the ```Manage``` page.  Clicking it forces the cbft node to re-run its
management activities, which include re-partitioning indexes into
index partitions and re-assigning index partitions to nodes in the
cluster.

Of note, if the nodes and index definitions in a cluster have not
changed, then the re-paritioning and re-assignment activities of a
manager kick should result in the exact same "plan"; hence, a manager
kick in that case would result effectively with a no-op.

An optional ```Kick Message``` input field allows the user to provide
text that will be logged by the cbft node, so that an administrator
can correlate manage kick requests with server-side activities.

---

Copyright (c) 2015 Couchbase, Inc.

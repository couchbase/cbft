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

## Index definition changes and zero downtime

When an index definition is created or modified, cbft will rebuild the
index partitions for that index from scratch, starting from an empty
state for that index.

During that time, queries against a modified index will see "missing"
results, as an index rebuild can take time to ingest or process data
from the data source.

To alleviate this situation and have "zero downtime" with respect to
query'ability, a user can utilize cbft's index alias feature.

The idea is to leverage the level of indirection in naming that an
alias provides, where an application sends its queries to an index
alias instead of directly to real indexes.

As an example, imagine the user creates a real index definition, such
as for ```ProductCatalogIndex-01```.

Then the user also creates an index alias, called
```ProductCatalogAlias```, which has ```ProductCatalogIndex-01``` as
its target.

The user's application is configured to make queries against
```ProductCatalogAlias```, and everything works fine.

Some time later, however, the team discovers a need for additional
features, such as perhaps needing to adjust bleve's full-text
tokenization configuration.

Rather than directly editing the ```ProductCatalogIndex-01```, instead
the team creates a brand new index, ```ProductCatalogIndex-02```,
which has the improved index configuration.

The team lets ```ProductCatalogIndex-02``` index build up to
acceptable amount of data.

The team may also, optionally, wish to turn off indexing ingest for
```ProductCatalogIndex-01```, if application requirements allow for
some amount of stale'ness in indexes.

When ```ProductCatalogIndex-02``` is ready, the team edits the
```ProductCatalogAlias``` definition to point to
```ProductCatalogIndex-02``` instead of pointing to the previous
```ProductCatalogIndex-01```.

The application continues to query ```ProductCatalogAlias``` with no
apparent downtime of queries.

As an advanced approach, the team may also allow some subset of the
application or subset of users (e.g., "beta" users) to instead query
the ```ProductCatalogIndex-02```, even whil it is building up, in
order to get a preview of the changed index configuration's behavior.

The resource cost of using an index alias for zero downtime to queries
is double that of a single index, but some applications may have zero
downtime as a necessary requirement for production and are willing to
bear the extra cost.

## Node/cluster changes and zero downtime

Similar to with handling index definition changes with zero downtime,
an administrator can use index aliases in order to provide zero
downtime for application queries even as cluster membership changes
(cbft nodes are added or removed).

Normally, as nodes are added or removed from a cbft cluster (as
different cbft nodes are registered as wanted or unwanted or unknown),
the cbft system will automatically reassign index partitions amongst
the remaining nodes.  The reassigned index partitions need to build
from scratch, however, which leads to queries not providing responses
to all data as the index partitions are rebuilt.

The solution is to use an index alias so that applications can
continue to query the old index.

Additionally, _before_ the administrator adds or removes nodes from a
cbft cluster, the administrator should disable index partition
reassignments for the current index definition.  See the instructions
above for how to disable index partition reassignments.

Then, the administrator can add new cbft nodes, and the current index
definition will remain "stable", where its index partitions will
remain assigned to the set of old cbft nodes.

The administrator would next define a new index, where the index
partitions of the new index would be assigned to both the old,
remaining cbft nodes and also to the newly added cbft nodes.

The administrator would then monitor the cbft cluster, watching for
indexing and ingest progress on the new index.

When the new index has indexed or ingested enough data from the data
source, the administrator can then edit the index alias and repoint
the alias away from the old index definition and instead point the
alias to the new index definition.

Then the administrator can delete the old index definition.

This operation allows applications to query the index alias with no
loss of indexed data, but at the cost of requiring twice the resources
to support two indexes in a cluster.

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

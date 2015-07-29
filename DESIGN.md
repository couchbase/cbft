cbft + couchbase Integration Design Document

Status: DRAFT

This design document focuses on the integration of cbft into Couchbase
Server; a.k.a. "cbftint".  Extra emphasis is given to clustering
related features such as rebalance, failover, etc.

-------------------------------------------------
# Links

Related documents:

* cbft's technical user documentation (Developer Preview)
  * http://labs.couchbase.com/cbft/
* cbgt's design document
  * The cbgt library provides generic infrastructure to manage
    distributed, replicated, clustered indexes.
  * cbft uses the cbgt library by registering a bleve full-text
    implementation as a plug-in into cbgt.
  * https://github.com/couchbaselabs/cbgt/blob/master/IDEAS.md (GT)
* ns-server's design documents
  * https://github.com/couchbase/ns_server/blob/master/doc
  * https://github.com/couchbase/ns_server/blob/master/doc/rebalance-flow.txt (rebalance-flow.txt)

-------------------------------------------------
# cbft Design Recap

For those needing a quick recap of cbft's main design concepts...

* Multiple cbft nodes can be configured together as a cluster when
  they share the same configuration (or Cfg) backend database.  For
  the cbftint project, we plan to use ns-server's metakv configuration
  system as the Cfg backend.

* An index in cbft is split or partitioned into multiple index
  partitions, known as PIndexes.  Roughly speaking, a PIndex has a
  1-to-1 relationship with a bleve full-text index.  To process query
  requests, a query needs to be scatter-gather'ed across the multiple
  PIndexes of an index.

* This partitioning of indexes into PIndexes happens at index creation
  time (i.e., index definition time).  To keep things simple for now,
  we assume that the number of PIndexes that are allocated per index
  doesn't change over the life of an index (although that might be a
  future feature).

* As the cbft cluster topology changes, however, the assignment of
  which cbft nodes are responsible for which PIndexes can change, due
  to reassignments from a cbft subsystem called the planner.  e.g.,
  cbft node 00 is responsibile for "PIndex af977b".  When a second
  cbft node 01 joins the cbft cluster, the planner reassigns PIndex
  af977b from cbft node 00 to cbft node 01.

* At index definition time, a PIndex is also configured with a source
  of data (like a couchbase bucket).  That data source will have one
  or more data source partitions (i.e., VBuckets).  For example, the
  beer-sample bucket has 1024 VBuckets.  So, we could say something
  like...
  * "PIndex af977b" is assigned to cover VBuckets 0 through 199;
  * "PIndex 34fe22" is assigned to cover VBuckets 200 through 399;
  * and so on with more PIndexes to cover up to VBucket 1023.

* An index can also be configured to be replicated.  In that case, the
  planner merely assigns a PIndex to more than one cbft node.  Each
  PIndex replica or instance will be independently built up from from
  direct DCP streams from KV engines.  That is, cbft uses star
  topology for PIndex replication instead of chain topology.  So, we
  could say something like...
  * PIndex af977b, which is assigned by the planner to cover VBuckets
    0 through 199, is assigned to cbft nodes 01 and 04.
  * When cbft node 04 is removed from the cbft cluster, then PIndex
    af977b is reassigned by the planner to cbft nodes 01 and 02.

For more information, please see cbgt's design document (GT).

-------------------------------------------------
# cbftint Design

In this section, we'll describe the planned steps of how ns-server
will spawn cbft nodes and orchestrate those cbft nodes along with KV
(memcached/ep-engine) nodes.  And, we'll describe scenarios of
increasing complexity and how we plan to handle them in methodical,
step-by-step fashion.

## Single CB Node, With Full-Text Service Disabled

In a single node couchbase cluster, or a so-called "cluster of one",
the simplest case is when the user hasn't enabled the cbft (or
full-text) service type.  In this simple situation, we expect
ns-server's babysitter on that single couchbase node to not spawn any
cbft processes.

NOTE: We don't expect the user to be able to dynammically change the
enabled service types for an existing, already initialized couchbase
node.  Instead, to change the service types for a node, the user must
rebalance the node out and in again.

## Single CB Node, With Full-Text Service Enabled

If the full-text service type is enabled for a node, we expect
ns-server's babysitter to spawn a cbft process, roughly something
like...

    CBAUTH=some-secret \
    ./bin/cbft \
      -cfg=metakv \
      -tags=feed,janitor,pindex,queryer \
      -dataDir=/mnt/cb-data/data/@cbft \
      -server=127.0.0.1:8091 \
      -bindHttp=0.0.0.0:9110 \
      -extra="{\"ns_server_rest\":\"127.0.0.1:8091\"}"

A brief description of those cbft process parameters:

### CBAUTH=some-secret

This cbauth related environment variable(s) allows ns-server to pass
secret credentials to cbft.  This allows cbft to use the cbauth/metakv
golang libraries in order to...

* allow cbft to use metakv as distributed configuration (Cfg) store,
  such as for storing cbft's metadata like full-text index
  definitions, cbft cluster memberships, etc.

* allow cbft to access any couchbase bucket as a data-source (even if
  the bucket is password protected or when the passwords are changed
  by administrators); that is, cbft will be able to retrieve cluster
  VBucket maps and create DCP streams for any couchbase bucket.

* allow cbft to authorize and protect its REST API, where cbft can
  perform auth checks on incoming REST API requests such as for
  queries, index definition changes, and stats/monitoring requests.

### -cfg=metakv

This command-line parameter tells cbft to use metakv as the Cfg
storage provider.

### -tags=feed,janitor,pindex,queryer

The tags parameter tells cbft to run only some subset of internal cbft
related services.  Of note, cbft's "planner" is explicitly _not_
listed in the -tags list, as this design proposal aims to leverage
ns-server's master facilities (ns-server's ability to dynamically
determine a single master node in a cluster) to more directly control
invocations of cbft's planner.

Using ns-server's master facilities will be a more reliable approach
than using cbft's basic behavior of having all cbft nodes concurrently
race each other in order to run their own competing planners; it will
also be less wasteful than having competing planners throw away work
when they lose any concurrent re-planning races.

Note that we do not currently support dynamically changing the tags
list for a cbft node.  In particular, changing the "pindex" tag (even
with a cbft process restart) will have complex rebalance-like
implications of needing PIndex movement; instead, users can
rebalance-out a node and rebalance it back in with a different set of
node services.

Related: this design also doesn't support the scenario of adding
unmanaged, standalone cbft instances to a cbft cluster that's
ns-server managed (such as by downloading cbft manually, and manually
trying to join it to a cbftint cluster via direct Cfg access).  That
is, each cbft instance in this design must be ns-server managed.

### -dataDir=/mnt/cb-data/data/@cbft

This is the data directory where cbft can store its node-specific
index data and metadata.

Of note, the dataDir is not planned to be dynamically changable at
runtime.  That is, if ns-server mistakenly restarts cbft with a brand
new, empty, different dataDir, the restarted cbft node will try to
join the cbft cluster like a brand new additional cbft node (since it
has a different cbft.uuid file), but the old cbft node (with the same
bindHttp but old cbft.uuid file) will still be registered and prevent
the restarted cbft node from joining.  To repair this undesirable
situation, the old node (with the old cbft.uuid) must be unregistered
from the Cfg (such as by manually using the UNREG_CBFT_NODE tool, to
be described later in this document).

On the other hand, if a future version of ns-server instead carefully
arranges to stop cbft, move/copy/symlink the dataDir contents to a new
location, and restart cbft pointed at the new dataDir, the cbft node
will resume with the same cluster membership identity as expected
(assuming there was no file data corruption).  This will work because
the restarted cbft node will have the same dataDir/cbft.uuid file
contents.

### -server=127.0.0.1:8091

This is ADDR:PORT of the REST API of the couchbase ns-server that cbft
will use by default for its data source (i.e., the default container
of buckets).

### -bindHttp=0.0.0.0:9110

This is the ADDR:PORT that cbft process will listen to in order to
provide the cbft REST API.  The bindHttp ADDR:PORT must be unique in
the cluster.  (NOTE: We'll talk about changing IP addresses in a later
section.)

### -extra=extra-JSON-from-ns-server

The is additional JSON information that ns-server needs to associate
with this cbft process.  For example...

    {
        "ns_server_rest":"127.0.0.1:8091"
    }

In particular, ns-server will want to repeatedly poll the cbft process
for its latest stats metrics and counters.  The REST response expected
by ns-server includes ns-server's outer REST address, and the extras
JSON is a way for ns-server to pass down its outer REST address and
other (future) information.

## cbft process registers into the Cfg

At this point, as the cbft process starts up, the cbft process will
add its node definition (there's just a single cbft node so far) into
the Cfg (metakv) system.

That way, other clients of the Cfg system can discover the cbft nodes
in the cbft cluster.

## cbft is ready for index DDL and an index is created (IC0)

At this point, full-text indexes can be defined using cbft's REST API.
The cbft node will save any created index definitions into its Cfg
(metakv) system.

Other clients of the Cfg system, then, can now discover the index
definitions of the cbft clsuter.

## A Managed, Central Planner (MCP)

The Cfg system has a subscription feature, so Cfg clients can be
notified when data in the distributed Cfg changes.

We'll use this feature and introduce a new, separate, standalone
planner-like program, called the "Managed, Central Planner" (MCP),
which will be used as a cluster-wide singleton.  ns-server's master
facilities will spawn, re-spawn and stop a single, cluster-wide
instance of this new MCP process.

A simple version of the MCP is roughly equivalent to this existing
cbft command-line option...

    cbft -tags=planner ...

There's a possibility that ns-server's master facilities might have
more than one master running with potential races between concurrent
masters.  That's suboptimal but survivable, as cbft's planner (and
MCP) will use CAS-like features in the Cfg to determine Cfg update
race winners.

Of note, an Enterprise Edition of cbftint might ship with a more
advanced MCP program, such as a planner than moves PIndexes with
more efficient orchestration.

## MCP updates the plan (UP0)

The MCP is awoken due to its subscription to Cfg changes (from the IC0
step from above) and splits the index definitions into one or more
PIndexes.  The MCP then assigns the PIndexes to cbft nodes (there's
only one cbft node so far, so this is easy; in any case, the planner
already is able to assign PIndexes across multiple cbft nodes).

The MCP then stores this updated plan into the Cfg.

A plan then has two major parts:
* a splitting of logical index definitions into PIndexes;
* and, an assignment of PIndexes to cbft nodes.

## cbft Janitors Wake Up To Clean Up The Mess

Individual cbft janitors on the cbft nodes (there's just one cbft node
so far in this simple case) are awoken due to the Cfg change
subscriptions (from previous step UP0) and create or shutdown any
process-local PIndex instances as appropriate.

That is, a cbft janitor will try to make process-local runtime changes
on its cbft node to reflect the latest plan, including starting and
stopping any PIndexes on the local cbft node.

## A PIndex instance creates DCP feeds

A PIndex instance creates DCP feeds, using the cluster map from the
ns-server, creating connections to the appropriate KV-engines.

Since the plan includes the assignment of source partitions (or
VBuckets) to every PIndex, the DCP streams that are created will have
the appropriate subset of VBucket ID's.

NOTE: It might be helpful for diagnostics for ns-server to pass name
information to the cbft (perhaps as a command-line parameter (-extra)
or environment variable) that helps cbft construct useful DCP stream
names.

## Simple Rebalances, Failovers, Restarts.

At this point, assuming our first cbft process isn't moving (i.e., the
first ns-server always stays in the couchbase cluster), and assuming
our first cbft process is the only cbft instance in the cluster, then
a whole series of KV Engine related scenarios are handled
automatically by cbft.  These include...

* KV engine restarts
* ns-server restarts
* lost connection to KV engine
* lost connection to ns-server
* rebalance, failover, VBucket cluster map changes

Most of this functionality is due to cbft's usage of the cbdatasource
library: https://github.com/couchbase/go-couchbase/tree/master/cbdatasource

The cbdatasource library has exponential backoff-retry logic when
either a streaming connection to ns-server (for vbucket maps) or a DCP
streaming connection fails.  Documentation on cbdatasource's
backoff/retry options (such as timeouts) are here:
http://godoc.org/github.com/couchbase/go-couchbase/cbdatasource#BucketDataSourceOptions

cbdatasource also handles when VBuckets are moving or rebalancing,
including handling NOT_MY_VBUCKET messages.

cbdatasource also handles when the cbft node restarts, and is able to
reconnect DCP streams from where the previous DCP stream last left
off.

In our example, we start with our first node...

* cb-00 - cbft enabled

Then, when we add/remove/failover/delta-node-recover any other node,
as long as those other nodes have cbft disabled and cb-00 stays in the
cluster, then everything "should just work" at this point in the
story:

* cb-00 - cbft enabled
* cb-01 - cbft disabled
* cb-02 - cbft disabled

We're still essentially running just a cbft "cluster" of a single cbft
node, even though there are multiple KV nodes with VBuckets moving all
around the place.  The simplification here is that cbft doesn't look
much different from any other "external" application that happens to
be using DCP.

## Handling IP address changes

One issue: as soon as the second node cb-01 was added, the IP address
of cb-00 might change (if not already explicitly specified to
ns-server during cb-00's initialization web UI/REST screens).  This is
because ns-server has a feature that supports late-bound IP-address
discovery, where IP-addresses can be (re-)assigned once a 2nd node
appears in a couchbase cluster.

At this point, the cbft node membership metadata stored in the Cfg by
cbft includes the -bindHttp ADDR:PORT value from the command line.
cbft uses that bindHttp ADDR:PORT to as both a unique cbft node
identifier and also when a cbft node wants to talk to other cbft nodes
(such as during scatter/gather index queries).

A simple proposal is that when ns-server discovers it must change the
IP address of a couchbase node (such as when cb-01 is added to the
"cluster" of cb-00), then ns-server must run some additional
(proposed) work...

* ns-server stops cbft's MCP (Managed, Central Planner) on the master
  ns-server node.
* ns-server stops cbft.
* ns-server invokes a (proposed, to be specified) synchronous
  command-line tool/program that atomically rewrites the bindHttp
  ADDR:PORT's in the Cfg (e.g., from 0.0.0.0:9110 to cb-00:9110).
* ns-server restarts cbft with the new -bindHttp ADDR:PORT
  (i.e., -bindHttp=cb-00:9110).
* ns-server restarts cbft's MCP.

The above work should happen before any actual node joining or
rebalancing occurs.

On cb-01, the cbft process should not have been started by ns-server
until after node joining has completed to the point where both cb-00
and cb-01 are able to use the same Cfg (metakv) system.

Assumption: this design assumes that once an IP address is rewritten
and finalized, the IP address no longer changes again for a couchbase
node.

As an alternative (possible but rejected) design: instead of using the
proposed "bindHttp" approach above, we could have each cbft node
instead have a globally unique, generated node UUID that isn't
overloaded with networking information.  In addition, there would need
to be a separate mapping that allows clients (and cbft's queryer) to
translate from logical cbft node UUID's to actual IP addresses.  That
translation map or level of indirectional can then be more easily,
dynamically changed to handle IP address changes.  This alternative
design idea, however, requires more cbft changes and its extra level
of indirection optimizes for an uncommon case (IP address changing) so
this alternative design isn't as favored.

## Adding more than one cbft node

Since IP addresses are now being rewritten and finalized, more
couchbase nodes can now be added into the cluster with the cbft
service type enabled.  As each cbft process is started on the new
nodes, the cbft process registers itself into the Cfg (metakv) system.

Whenever the cbft cluster membership information changes, the MCP will
notice (since the MCP is subscribing to Cfg changes), and the MCP will
re-plan any assignments of PIndexes to the newly added cbft nodes.

The cbft janitors running on the existing and new cbft nodes will see
that the plan has changed and stop-&-start PIndexes as needed,
automatically stopping/starting any related DCP feeds as necessary.

This means adding more than one cbft node is now supported; for
example, the design now supports simple, homogeneous topologies:

* cb-00 - cbft enabled
* cb-01 - cbft enabled
* cb-02 - cbft enabled
* cb-03 - cbft enabled

And the design also support heterogeneous "multi-dimensional scaling"
(MDS) topologies:

* cb-00 - cbft enabled
* cb-01 - cbft disabled
* cb-02 - cbft disabled
* cb-03 - cbft enabled

## Rebalance out a cbft node

When a couchbase node is removed from the cluster, if the couchbase
node has the cbft service type enabled, here are the proposed steps to
handle rebalancing out a cbft node:

* (RO-10) ns-server invokes a (to be written) command-line program
  that unregisters a cbft node from the Cfg (UNREG_CBFT_NODE).

* (RO-20) ns-server shuts down the cbft process on the to-be-removed
  couchbase node.

* (RO-30) ns-server deletes or cleans out the dataDir subdirectory
  tree that was being used by cbft, especially the dataDir/cbft.uuid
  file.

This UNREG_CBFT_NODE command-line program (final name is TBD), when
run on the to-be-removed node, roughly looks like the following
existing cbft command-line option...

    ./cbft -register=unknown ...

Additional straightforward development will be needed to create an
actual UNREG_CBFT_NODE program or command-line tool than can be run on
any node in the cluster, to allow ns-server to unregister any cbft
node from the Cfg.  Especially, we would want ns-server to be able to
run UNREG_CBFT_NODE on its master ns-server node.

The above steps and invocation of UNREG_CBFT_NODE can happen at the
end of ns-server's rebalance-out steps for a node, which is an
approach that favors keeping cbft mostly stable during rebalance.
That means hitting "Stop Rebalance" would leave cbft mostly as-is on a
node-by-node basis.

## Unable to meet replica counts

Of note, removing a cbft node (and, not having enough cbft nodes in
the first place) can mean that the MGP is not able to meet replication
requirements for its PIndexes.  i.e., user asks for replica count of
2, but there's only 1 cbft node in the cbft cluster.

cbft needs to provide a REST API (perhaps, as part of stats monitoring
REST responses) that allows ns-server to detect this situation of "not
enough cbft nodes" and display it in ns-server's UI appropriately.

## Hard Failover

The proposal to handle hard failover is that ns-server's master node
should invoke the UNREG_CBFT_NODE program (same as from step RO-10
above) and run the MCP (if not already running).

This will cause MCP to re-plan and reassign PIndexes to any remaining
cbft nodes.

## Graceful Failover and Cooling Down

Graceful failover is feature that concerns the KV engine, since the KV
engine has the primary data of the system that must be gracefully kept
safe.  As a first step, then, we might choose to do nothing to support
graceful failover.

As an advanced step, though, a proposal to handle graceful failover
would be similar to handling hard failover, but additionally,
ns-server's master node should invoke REST API's on the
to-be-failovered cbft node to pause cbft's index ingest and to pause
cbft's query-ability on the to-be-failovered cbft node.

See the current management REST API's of cbft here:
http://labs.couchbase.com/cbft/api-ref/#index-management

Of note: those cbft REST API's need to be improved to allow ns-server
to limit the scope of index pausing and query pausing to just a
particular node.

Note that we don't expect to support cancellation of graceful
failover, but if that's needed, ns-server can invoke cbft's REST API
to resume index ingest and querying.

## Gaps In The Simple Design So Far

Although we now support adding, removing and failover'ing multiple
cbft nodes in the cluster at this point in the design, this simple
design approach is actually expected to be quite resource intensive
(lots of KV backfills) and has availability issues.

### Backfill Inefficiency

For example, if we started out with just a single node cb-00 and then
added nodes cb-01, cb-02 & cb-03 into the cluster at the same time,
then eventually all the cbft janitors on those three newly added nodes
will awake at nearly the same time and all start their DCP streams at
nearly the same time.  This would lead to a huge, undesirable load on
the KV system due to lots of concurrent KV backfills across all
vbuckets.

Ideally, we would like there to be throttling of concurrent KV
backfills to avoid killing the system.

### Index Unavailability

Following on the same example scenario, at the same time, the cbft
janitor on cb-00 will awaken and stop 3/4th's of its PIndexes (the
ones that have been reassigned to the new cbft nodes on cb-01/02/03).

So, any queries at this time will see lack of hits or results
(assuming the PIndexes are slow to build on the new cbft nodes).

Ideally, we would like there to be little or no impact to cbft queries
and index availability during a rebalance.

## Addressing Backfill Inefficiency And Index Unavailability

Let's address these issues one at a time, where we propose to fit into
the state changes and workflow of ns-server's rebalance design.  In
particular, during rebalance ns-server provides some advanced
concurrency scheduling maneuvers to increase efficiency, availability
and safety (see ns-server's rebalance-flow.txt document for more
details)...

* ns-server can limit the number of concurrent KV backfills per node
  (usually to 1 KV backfill per node).
* ns-server disables compactions during some sub-phases of the
  rebalance moves to increase throughput.
* ns-server disables indexing during some sub-phases of the rebalance
  moves to increase throughput and to implement "consistent view
  queries under rebalance".
* ns-server prioritizes VBucket moves to try to keep number of active
  VBuckets "level" throughout nodes in the cluster.

Pictorially, here is a diagram on concurrency during rebalance (from
rebalance-flow.txt) on a single couchbase node, circa 2.2.x...

         VBucket Move Scheduling On A Single KV Node
    Time
      |  /------------\
      |  | Backfill 0 |                  Backfills cannot happen
      |  \------------/                  concurrently on a KV node.
      |        |         /------------\
      |  +------------+  | Backfill 1 |
      |  | View Index |  \------------/
      |  | File 0     |        |
      |  |            |  +------------+  However, view indexing _can_
      |  |            |  | View Index |  happen concurrently with KV
      |  |            |  | File 1     |  backfills and other
      |  +------------+  |            |  view indexing.
      |        |         |            |
      |        |         +------------+
      |        |              |
      |         \------+-----/
      |                |
      |  /----------------------------\  Compaction for a set of vbucket
      |  | Compact both src & dest.   |  moves cannot happen concurrently
      v  \----------------------------/  with another set of vbucket moves.

## Increasing availability of PIndexes

TBD / design sketch - basically, cbftint will need to delete old
PIndexes only after new PIndexes are built up on the new nodes.  An
advanced MCP (Managed, Central Planner) could provide this
orchestration.

## Limit backfills

TBD / design sketch - each new pindex being built up on a new node
means KV backfills.  Need to throttle the number of concurrent "new
pindex builds".  An advanced MCP (Managed, Central Planner) could
provide this throttling.

## Controlled compactions of PIndexes

TBD / design sketch - cbft will need REST API's to disable/enable
compactions (or temporarily change compaction timeouts).  Again, this
is likely an area for the MCP to orchestrate.

-------------------------------------------------
# Requirements Review

NOTE: We're currently missing a formal product requirements
document (PRD), so these requirements are based on anticipated PRD
requirements.

In this section, we cover the list of requirements and describe how
the design meets the requirements.

Requirements with the "GT-" prefix originally come from the cbgt
IDEAS.md design document.

## GT-CS1 - Consistent queries during stable topology.

Clients should be able query for results where the indexes have
incorporated at least up to a given set of {vbucket to seq-num}
pairings.

## GT-CR1 - Consistent queries under datasource rebalance.

Queries should be consistent even as data source KV nodes are added
and removed in a clean rebalance.

## GT-CR2 - Consistent queries under cbgt topology change.

Queries should be consistent even as full-text nodes are added and
removed in a clean takeover fashion.

## GT-OC1 - Support optional looser "best effort" options.

The options might be along a spectrum from stale=ok to totally
consistent.  The "best effort" option should probably have lower
latency than a totally consistent GT-CR1 query.

For example, perhaps the client may want to just ask for consistency
around just a subset of vbucket-seqnum's.

## GT-IA1 - Index aliases.

This is a level of indirection to help split data across multiple
indexes, but also not change your app all the time.  Example: the
client wants to query from 'last-quarter-sales', but that means a
search limited to only the most recent quarter index of
'sales-2014Q3'.  Later, an administrator can dynamically remap the
'last-quarter-sales' alias to the the newest 'sales-2014Q4' index
without any client-side application changes.

## GT-MQ1 - Multi-index query for a single bucket.

This is the ability to query multiple indexes in one request for a
single bucket, such as the "comments-fti" index and the
"description-fti" index.

## GT-MQ2 - Multi-index query across multiple buckets.

Example: "find any docs from the customer, employee, vendor buckets
who have an address or comment about 'dallas'".

## GT-NI1 - Resilient to data source KV node down scenarios.

cbft instances should try to automatically reconnect to a recovered KV
data source node and resume indexing from where they left off.

## GT-E1 - The user should be able to see error conditions.

For example yellow or red coloring on node down and other error
conditions.

Question: how to distinguish between I'm behind (as normal) versus
I'm REALLY behind on indexing.  Example: in 2i project, it can detect
that "I'm sooo far REALLY behind that I might as well start from zero
instead of trying catch up with all these mutation deltas that
will be throwaway work".

In ES, note the frustrating bouncing between yellow, green, red;
ns-server example, not enough CPU & timeouts leads to status
bounce-iness.

## GT-NQ1 - Querying still possible if data source node goes down.

Querying of a cbgt cluster should be able to continue even if some
data source KV nodes are down.

## GT-PI1 - Ability to pause/resume indexing.

## IPADDR - IP Address Changes.

IP address discovery is "late bound", when couchbase server nodes
initially joins to a cluster.  A "cluster of one", in particular, only
has an erlang node address of "ns_1@127.0.0.1".

ns-server also has feature where node names might also be manually
assigned.

## BUCKETD - Bucket Deletion Cascades to Full-Text Indexes.

If a bucket is deleted, any full-text indexes based on that bucket
should be also automatically deleted.

There whould be user visible UI warnings on these "cascading deletes"
of cbft indexes.

(Perhaps ns-server invokes a synchronous command to unregister / delete
cbft indexes?)

(What about cascade delete (or listing) of index aliases that
(transitively) point to an index (or to a bucket datasource)?)

## BUCKETR - Bucket Deletion & Recreation with the same name.

## BUCKETF - Bucket Flush.

## RIO - Rebalance Nodes In/Out.

(Need ability / REST API to quiesce or cool down a cbft process?)

## RP - Rebalance progress estimates/indicator.

## RS - Swap Rebalance.

## FOH - Hard Failover.

Besides hard failover in normal operations, consider the possibility
of a hard failover in the midst of a rebalance or any other
non-steady-state scenario.

## FOG - Graceful Failover.

Reject any new requests and wait for any inflight requests to finish
before failover.

## AB - Add Back Rebalance.

## DNR - Delta Node Recovery.

This involves leveraging cbft's old index files.

## RP1 - Rebalance Phase 1 - VBucket Replication Phase.
## RP2 - Rebalance Phase 2 - View Indexing Phase.
## RP2 - Rebalance Phase 3 - VBucket Takeover Phase.

## RSTOP - Ability to Stop Rebalance.

A.k.a, "put the pencils down".

## MDS-FT - Multidimensional Scaling For Full Text

Ability to rebalance cbft resources indpendent of other services.

## CIUR - Consistent Indexes Under Rebalance.

This is the equivalent of "consistent view index queries under
rebalance".

## QUERYR - Querying Replicas.

## QUERYLB - Query Load Balancing To Replicas.

## QUERYLB-EE - Query Load Balancing To Replicas, Enterprise Edition

Perhaps EE needs to be more featureful than simple round-robin or
random load-balancing, but targets the most up-to-date replica.

Or the least-busy replica.

## ODS - Out of Disk Space.

Out of disk space conditions are handled gracefully (not segfaulting).

## ODSR - Out of Disk Space Repaired.

After an administrator fixes the disk space issue (adds more disks;
frees more space) then full-text indexing should be able to
automatically continue successfully.

## KP - Killed Processes (linux OOM, etc).

## RSN - Return of the Shunned Node.

## DLC - Disk Level Copy/Restore of Node.

This is the scenario when a user "clones" a node via disk/storage
level maneuvers, such as usage of EBS snapshot features or tar'ing up
a whole dataDir.

The issue is that old cbft.uuid files might still (incorrectly) be copied.

## UI - Full-Text tab in Couchbase's web admin UI.

## STATS - Stats Integration into Couchbase's web admin UI.

## AUTHI - Auth integration with Couchbase for indexing.

cbft should be able to access any bucket for full-text indexing.

## AUTHM - Auth integration with Couchbase for admin/management.

cbft's administration should be protected.

## AUTHQ - Auth integration with Couchbase for queries.

cbft's queryability should be protected.

## AUTHPW - Auth credentials/pswd can change (or is reset).

Does this affect cbauth module?

## TLS - TLS/SSL support.

## HC - Health Checks.

## QUOTAM - Memory Quota per node.

## QUOTAD - Disk Quota per node.

## BFLIMIT - Limit Backfills.

ns_single_vbucket_mover has a policy feature that limits the number of
backfills to "1 backfill during rebalance in or out of any node" (see
rebalance-flow.txt)

## RCOMPACT - Index Compactions Controlled Under Rebalance.

During rebalance, ns_server pauses view index compactions until a
configurable number of vbucket moves have occurred for efficiency (see
rebalance-flow.txt)

This might prevent huge disk space blowup on rebalance (MB-6799?).

## RPMOVE - Partition Moves Controlled Under Rebalance.

During rebalance, ns_server limits outgoing moves to a single vbucket
per node for efficiency (see rebalance-flow.txt).  Same should be the
case for PIndex reassignments.

See "rebalanceMovesBeforeCompaction".

## RSPREAD - Rebalance Active VBucket Spreading.

ns_server prioritizes VBuckets moves that equalize the spread of
active VBuckets across nodes and also tries to keep indexers busy
across all nodes.  PIndex moves should have some equivalent
optimization.

## COMPACTF - Ability for force compaction right now.

## COMPACTO - Ability to compact files offline (while service down).

Might be useful to recover from out-of-disk space scenarios.

## COMPACTP - Ability to pause/resume automated compactions.

## COMPACTC - Ability to reconfigure automated compaction policy.

## TOOLBR - Tools - Backup/Restore.

## TOOLCI - Tools - cbcollectinfo.

## TOOLM - Tools - mortimer.

## TOOLN - Tools - nutshell.

## TOOLDUMP - Tools - couch_dbdump like tools.

## UPGRADESW - Handles upgrades of sherlock-to-watson.

## UPGRADE - Future readiness for watson-to-future upgrades.

## UTEST - Unit Testable.

## QTEST - QE Testable / Instrumentable.

## REQTRACE - Ability to trace a request down through the layers.

## REQPILL - Ability to send a fake request "pill" down through the layers.

## REQTIME - Ability to track "where is the time going".

This should ideally be on a per-request, individual request basis.

## REQTHRT - Request Throttling

ns-server REST & CAPI support a "restRequestLimit" configuration.

## DCPNAME - DCP stream naming or prefix

Allow for DCP stream prefix to allow for easier diagnosability &
correlation (e.g., these DCP streams in KV-engine come from cbft due
to these indexes from these nodes).

## TIMEREW - Handle NTP backward time jumps gracefully

## RZA - Handle Rack/Zone Awareness & Server Groups

## CBM - Works with cbmirror

-------------------------------------------------
# Random notes / TODO's / section for raw ideas

ip address changes on node joining
- node goes from 'ns_1@127.0.0.1' (or 0.0.0.0?)
    to ns_1@REAL_IP_ADDR
ip address rename

bind-addr needs a REAL_IP_ADDR
from the very start to be clusterable?

all nodes start with the node having a "wrong" bindHTTP addr (like
127.0.0.1 or 0.0.0.0), so ideas:

- fix reliance on bindHTTP
- allow true node UUID, with "outside" mapping of UUID to contactable
  http address
- add command to rename a node's bindHTTP

best effort queries
- vs return error
- keep old index entries even if index definition changes
- vs rebuild everything

node lifecycle
- known
- wanted
-- moving to wanted
- unwanted
-- moving to unwanted
- unknown

               unwanted wanted
    unknown    ok         N/A
    known      ok         ok

index lifecycle

A vbucket in a view index has a "pending", "active", "cleanup" states,
that are especially used during rebalance orchestration.  Perhaps
PIndexes need equivalent states?

add command-line param where ns-server can force a default node UUID,
for better cross-correlation/debuggability of log events.  And, we can
look for the return of the shunned node.

add command-line tools/params for outside systems (like ns-server) to
add/remove nodes?

Policy ideas...

- Do node adds first, before removes?
  Favor more capacity earlier.

- Move all KV vbuckets before moving any cbft pindexes?

- Favor FT index builds on new nodes, before FT index builds on
  remaining nodes?  (Favors utilizing empty disks earlier.)

- Favor FT index builds with priority 0 pindexes first.

Two different kinds of rebalance...

- KV vbucket rebalancing
- cbft pindex rebalancing

Current, easiest cbft pindex rebalance implementation...
- easy, but bad behavior (CE edition--?)
- cbft process started on new node, joins the cfg...
- then instant (re-)planner & janitor-fication...
- means apparently full-text index downtime and DDoS via tons of
  concurrent DCP backfills.

state machine:
- running
- warming
- cooling
- stoppable/stopped
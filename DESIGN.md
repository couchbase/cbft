cbft + couchbase Integration Design Document

Status: DRAFT-01

This design document focuses on the integration of cbft into Couchbase
Server; a.k.a. "cbftint".  Extra emphasis is given to clustering
related features such as rebalance, failover, etc.

Brief summary: cbft will by babysat by ns-server; cbft will store is
configuration metadata into ns-server's metakv system; and, after the
end of the existing rebalancing of KV VBuckets and view/GSI builds,
ns-server will enter an additional, user-cancellable rebalancing phase
for cbft-related index partitions by invoking a new cmd-line program
called MCP (cbft's Managed Central Planner).

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
  1-to-1 relationship with a bleve full-text index (although cbgt can
  support additional types of indexes).  To process query requests, a
  query needs to be scatter-gather'ed across the multiple PIndexes of
  an index.

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

## Command-line Parameters

Next, we briefly describe the cbft command-line parameters that
ns-server uses:

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

(DDIR-RM) By default, ns-server must clear the dataDir directory tree
before starting the cbft process, especially the dataDir/cbft.uuid
file.  (But not always; see later (DNR) regarding delta node
recovery).

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

This is additional JSON information that ns-server wishes to associate
with this cbft process.  For example...

    {
        "ns_server_rest":"127.0.0.1:8091"
    }

In particular, ns-server will want to repeatedly poll the cbft process
for its latest stats metrics and counters.  The REST response expected
by ns-server includes ns-server's REST address (for proper stats
aggregation), and the extras JSON command-line parameter is a way for
ns-server to pass down its REST address and other (future) information
down to cbft.

Underneath the hood: the cbgt library treats this extras JSON as an
opaque string that's just stored as opaque metadata and passed along.
Only the stats REST handling logic in cbft to support ns-server
integration uses it.

## cbft Node Registers Into The Cfg

At this point, as the cbft process starts up, the cbft process will
add its node definition (there's just a single cbft node so far) into
the Cfg (metakv) system as a wanted cbft node.

That way, other clients of the Cfg system can discover the cbft nodes
in the cbft cluster.

----
## cbft Is Ready For Index Creations (INDEX-DDL)

At this point, full-text indexes can be defined using cbft's REST API.
The cbft node will save any created index definitions into its Cfg
(metakv) system.

Other clients of the Cfg system, then, can now discover the index
definitions of the cbft clsuter.

----
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

## MCP Updates The Plan (UP0)

The MCP is awoken due to its subscription to Cfg changes (from the
INDEX-DDL step from above) and splits the index definitions into one
or more PIndexes.  The MCP then assigns the PIndexes to cbft nodes
(there's only one cbft node so far, so this is easy; in any case, the
planner already is able to assign PIndexes across multiple cbft
nodes).

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

## DCP Streams Are Started

A PIndex includes enough information for the cbft system to start DCP
feeds or streams, using the cluster map from the ns-server, where cbft
can create DCP connections to the appropriate KV-engines.

Since the plan includes the assignment of source partitions (or
VBuckets) to every PIndex, the DCP streams that are created will have
the appropriate subset of VBucket ID's.

NOTE: It might be helpful for diagnostics for ns-server to pass name
information to the cbft (perhaps as a command-line parameter (-extra)
or environment variable) that helps cbft construct useful DCP stream
names.

## Simple Rebalances, Failovers, Restarts

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

### A Swap Rebalance Case From Tech Support

One scenario raised by technical support engineering (James Mauss) is
when you have a cluster with nodes A, B, C, D, with service types
enabled like...

           A B C D
    KV   : y y y
    cbft :       y

That is, node D is a cbft-only node.

Then, there's a swap rebalance of nodes A, B, C for nodes E, F, G,
leaving node D in the cluster...

           D E F G
    KV   :   y y y
    cbft : y

This case should work because cbft only contacts its local ns-server
on 127.0.0.1 to get the cluster map.

## Handling IP Address Changes

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

## Adding More Than One cbft Node

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

## Rebalance Out A cbft Node

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

## Swap Rebalance Of cbft Nodes

The blance library used by cbgt for partition map computation is meant
to handle swap rebalance just as a natural edge case.  In other words,
this design depends on blance to get swap rebalance right, with no
extra special code or case'ing needed to handle swap rebalance
scenarios.

## Unable To Meet Replica Counts

Of note, removing a cbft node (and, not having enough cbft nodes in
the first place) can mean that the MGP is not able to meet replication
requirements for its PIndexes.  i.e., user asks for replica count of
2, but there's only 1 cbft node in the cbft cluster.

cbft needs to provide a REST API (perhaps, as part of stats monitoring
REST responses) that allows ns-server to detect this situation of "not
enough cbft nodes" and display it in ns-server's UI appropriately.

Of note, cbft should be able to report the difference between...

* not enough nodes to meet replication constraints.
* not enough PIndex replicas have been assigned to meet replication
  constraints (i.e., there was a failover where there are still enough
  nodes remaining in the cluster to meet replication constraints, but
  there hasn't yet been a rebalance).

## Hard Failover

The proposal to handle hard failover is that ns-server's master node
should invoke the UNREG_CBFT_NODE program (same as from step RO-10
above).

The UNREG_CBFT_NODE must remove the following from the Cfg:
* remove the node from the nodes-wanted and nodes-known sections.
* remove the node from the planned PIndexes.
* promote any replica PIndexes, if any, to active or primary position.

Importantly, during a hard failover, the MCP should _not_ be invoked
to re-plan and assign PIndexes to remaining cbft nodes in order to
meet replication constraints.  Instead, creating new replica PIndexes
to remaining nodes should wait until user later starts a rebalance.

## Graceful Failover and Cooling Down

Graceful failover is feature that concerns the KV engine, since the KV
engine has the primary data of the system that must be gracefully kept
safe.  As a first step, then, we might choose to do nothing to support
graceful failover.

As an advanced step, though, a proposal to handle graceful failover
would be similar to handling hard failover, but additionally,
ns-server's master node should invoke REST API's on the
to-be-failovered cbft node at the start of the graceful failover in
order to pause cbft's index ingest and to pause cbft's query-ability
on the to-be-failovered cbft node.

See the current management REST API's of cbft here:
http://labs.couchbase.com/cbft/api-ref/#index-management

Of note: those cbft REST API's need to be improved to allow ns-server
to limit the scope of index pausing and query pausing to just a
particular node.

Note that we don't expect to support cancellation of graceful
failover, but if that's needed, ns-server can invoke cbft's REST API
to resume index ingest and querying.

----
## Major Gaps In The Simple Design So Far

Although we now support adding, removing and failover'ing multiple
cbft nodes in the cluster at this point in the design, this simple
design approach is actually expected to be quite resource intensive
(lots of backfills) and has availability issues.

### Backfill Overload

For example, if we started out with just a single node cb-00 and then
added nodes cb-01, cb-02 & cb-03 into the cluster at the same time,
then the cbft janitors on the three newly added nodes will awake at
nearly the same time and all start their DCP streams at nearly the
same time.  This would lead to a huge, undesirable load on the KV
system due to lots of concurrent KV backfills across all vbuckets.

Ideally, we would like there to be throttling of concurrent KV
backfills to avoid overloading the system.

### Index Unavailability

Continuing with the same example scenario, at the nearly same time,
the cbft janitor on cb-00 will awaken and stop 3/4th's of its PIndexes
(the ones that have been reassigned to the new cbft nodes on
cb-01/02/03).

So, any new queries at this time will see a significat lack of search
hits (assuming the PIndexes are slow to build on the new cbft nodes).

Ideally, we would like there to be little or no impact to cbft queries
and index availability during a rebalance.

## Addressing Backfill Overload And Index Unavailability

To address these major gaps, we have a fork in the road of design paths:

* A. We could attempt to fit into ns-server's rebalance flow and
  orchestration; that is, interleave some KV VBucket moves and
  view/GSI re-indexing with PIndex moves, taking care fit into
  ns-server's throttling phases.
* B. Instead of interleaving, ns-server could move all KV VBuckets and
  view/GSI indexes first, and then hand off full PIndex rebalancing
  control to the MCP.

Pathway B is easier to implement, so we'll use that pathway, while
still trying to design the MCP with external pausability/resumability
to allow us to potentially grow to a future pathway A implementation.

### ns-server's Rebalance Flow

Let's recap ns-server's rebalance flow, so we understand the benefits
it provides.

During rebalance, ns-server provides some advanced concurrency
scheduling maneuvers to increase efficiency, availability and safety.
We recap those maneuvers here, but please see ns-server's
rebalance-flow.txt document for more details.

* ns-server rebalances only one bucket at a time.
* ns-server prioritizes VBucket moves to try to keep number of active
  VBuckets "level" throughout KV nodes in the cluster.
* ns-server also limits concurrent KV backfills to 1 per node.
* ns-server disables compactions during VBucket moves and view
  indexing, to increase efficiency/throughput at the cost of using
  more storage space.
* after 64 VBucket moves, ns-server kicks off a phase of compactions
  across the cluster.
* ns-server disables indexing during some sub-phases of the rebalance
  moves to implement "consistent view queries under rebalance".

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

### Limiting Concurrent cbft Backfills

When a PIndex is assigned to a cbft node, that cbft node will start
DCP streams on the data source KV nodes based on the VBucket's covered
by the PIndex.  Those DCP streams mean backfills on the KV nodes.

MCP can provide KV backfill throttling by limiting the number of
concurrent PIndex reassigments.  A simple policy for MCP would be that
MCP can ensure that a cbft node only concurrently builds up a max of N
new PIndexes, where N could have a default of 1.

From the KV engine's point of view, that simple policy could still
mean more than 1 backfill at a time, though, so if tighter throttling
is needed, then perhaps MCP can additionally limit the concurrent
PIndex reassignments to a max of M per cluster.  M configured to be 1
would have the most throttling, but perhaps inefficiently leave some
KV nodes underutilized during rebalancing, where the currently
"moving" PIndex doesn't have any VBuckets from some KV nodes.

An even more advanced MCP might try to dynamically tune the M and N
concurrency parameters to maximize throughput, such as by examining
the VBuckets for each PIndex and cross-correlating those VBuckets to
their current KV nodes.

### Increasing Availability of PIndexes

The proposed design is that the MCP should only unassign or remove an
old PIndex instance from a cbft node only after some new PIndex
instance has been built up on some other cbft node.

The MCP can do this by computing a new plan but not publishing it yet
to all the janitors.  Instead, MCP saves the new, final plan off to
the side somewhere in the Cfg, similar to how ns-server computes a
fast-forward VBucket map.

If MCP dies and restarts, or even gets restarted on a different
couchbase node (i.e., ns-server master moved), then MCP should
theoretically be able to pick up where it left off since it had saved
its fast-forward-plan to the distributed Cfg.  Practically, though, if
the MCP died or was restarted, the ns-server should stop the rebalance
with an error message.

The MGP can then repeatedly choose some subset of the
fast-forward-plan to publish in throttled steps to the janitors (via
Cfg updates), so that subscribing janitors will make progress.  In
other words, the MCP is roughly using the Cfg as a persistent,
distributed message blackboard, where the MCP performs recurring,
episodic writes to the janitor-visible plan in the Cfg to move the
distributed cbft janitors closer and closer to the final, fast-forward
plan.

In pseudocode, the MCP roughly does the following, running concurrent
worker activity across nodes...

    M := 1 // Max number of PIndex builds per cluster.
    N := 1 // Max concurrent inbound PIndex builds per node.

    for node in nodes {
      for i := 0; i < N; i++ {
        go nodeWorker(node)
      }
    }

    for i := 0; i < M; i++ {
      // Tokens available to throttle concurrency.  The # of outstanding
      // tokens might be changed dynamically and can also be used
      // to synchronize with any optional, external orchestrator
      // (i.e., ns-server wants cbft to do X number of moves with
      // M concurrency before forcing a compaction).
      nodeWorkerTokensSupplyCh <- i
    }

    func nodeWorker(node) {
      while true {
        nodeWorkerToken, ok := <-nodeWorkerTokensSupplyCh
        if !ok then break // Perhaps done or was cancelled (by ns-server?).

        pindexToReassign, oldNode, ok :=
          calculateNextPIndexToAssignToNode(node)
        if !ok then break // No more incoming PIndexes for this node.

        // Updates janitor-visible plan.
        assignNodeToPIndex(pindexToReassign, node)

        wasCancelled := waitForPIndexReadyOnNode(pindexToReassign, node)
        if wasCancelled then break

        if oldNode != nil {
          // Updates janitor-visible plan.
          unassignNodeToPIndex(pindexToReassign, oldNode)

          wasCancelled := waitForPIndexRemovedFromNode(pindexToReassign, oldNode)
          if wasCancelled then break
        }

        nodeWorkerTokensReleaseCh <- nodeWorkerToken
      }
    }

### calculateNextPIndexToAssignToNode

TODO: A next design issue is the design sketch of the
"calculateNextPIndexToAssignToNode" function, which will likely be a
reusable part of the blance library:
https://github.com/couchbaselabs/blance

### Controlled Compactions of PIndexes

cbft will need to provide REST API's to disable/enable compactions (or
temporarily change compaction timeouts?) to enable outside
orchestration (i.e., from ns-server).

These compaction timeouts should likely be ephemeral (a cbft process
restart means compaction configurations come back to normal,
non-rebalance defaults).

## Delta Node Recovery (DNR)

Delta node recovery allows a node that's added back to the cluster to
reuse existing data and index files, if any, for a speedier cluster
recovery.

If the ns-server determines that an added-back node is meant to be
delta-node-recovered, then ns-server should not clean out the dataDir
directory tree before starting the cbft process on the added-back node
(see step DDIR-RM above), but ns-server should only delete the
dataDir/cbft.uuid file.  That is, any PIndex files and subdirectories
in the dataDir should remain untouched, as the cbft process might be
able to reuse them instead of having to rebuild PIndexes from scratch.

MCP doesn't have any memory of the previous cluster configuration, and
of which PIndexes were assigned to which previous (recovered) cbft
nodes and in what states (i.e., master vs. replica).  Without this
memory, MCP won't be able to handle delta node recovery very well, and
could likely reassign an inefficient set of PIndexes to the recovered
node.  The design proposal to address this issue is that in a
delta-node-recovery situation, ns-server will invoke an extra
tool/program (to be spec'ed; DNRFTS) that will list into the Cfg any
PIndexes that are found on the recovered cbft node.  This will give
the MCP the information it needs to plan a recovery more efficiently
(i.e., increase the chance that MCP will reuse the PIndexes that
already exist on the recovered cbft node).

Of note, we should take care that there might be a race here to
consider, where the janitor on the recovered cbft process wakes up,
discovers a whole bunch of old, seemingly unnecessary PIndex
subdirectories and starts removing them, before the MCP has a chance
to update the plans and assign those PIndexes to the recovered node.
The DNRFTS step proposed above needs to address this potential race.

## Bucket Deletion

If a bucket is deleted, any full-text indexes based on that bucket
should be, by default, automatically deleted.

The proposal is ns-server invoke a synchronous command-line tool or
program that can list and/or delete the cbft indexes that have a data
source from a given bucket.

The listing operation allows ns-server to potentially display UI
warnings to the user who's about to perform a bucket deletion.

This command-line tool would also allow administrators to perform the
same listing and "cascading delete" operation manually.

TODO: What about cascade delete (or listing) of index aliases that
(transitively) point to an index (or to a bucket datasource)?

-------------------------------------------------
# Requirements Review

NOTE: We're currently missing a formal product requirements
document (PRD), so these requirements are based on anticipated PRD
requirements.

In this section, we cover the list of requirements and describe how
the design meets the requirements.

(NOTE / TODO: at this point this list is just a grab bag of things
we've heard of that we know we must address.)

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

There should be user visible UI warnings on these "cascading deletes"
of cbft indexes.

## BUCKETDA - Bucket Deletion & Index Aliases

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

More feedback from James Mauss:

* rebalance with node ports are blocked (aws security zone).
  * i.e., cbft can't talk to KV port 11210
* want valid error msgs in all these cases
* fill up disk space and repair/recover
  * e.g., to test, create a big file (like via dd)
  * have a valid error msg
  * then heal by deleting the big file
* handle temporarily running out of file handles
  * and, decent error msg
* hostname doesn't resolve
  * i.e., orchestrator can see new node but not the other way
    at botht eh ns-server and memcached level
* windows: something has a file lock on your file
  * that something is antivirus, or whatever opens file in readonly
  * ask patrick varley, who has tool to do that
* compaction getting stuck and/or not actually compacting or
  compacting fast enough

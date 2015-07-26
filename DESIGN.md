cbft + couchbase Integration Design Document

Status: DRAFT

This design document focuses on the integration of cbft into Couchbase
Server; a.k.a. "cbftint".  Extra emphasis is given to clustering
related features such as rebalance, failover, etc.

-------------------------------------------------
# Links

Related documents:

* cbgt design documents
  * The cbgt library provides generic infrastructure to manage
    distributed, replicated, clustered indexes.
  * https://github.com/couchbaselabs/cbgt/blob/master/IDEAS.md (GT)
* ns-server design documents
  * https://github.com/couchbase/ns_server/blob/master/doc
  * https://github.com/couchbase/ns_server/blob/master/doc/rebalance-flow.txt (rebalance-flow.txt)

-------------------------------------------------
# cbft Design Recap

For those needing a quick recap of cbft's main design concepts...

* Multiple cbft nodes can be operated together as a cluster when they
  share the same configuration (or Cfg) backend database.  For the
  cbftint project, we plan to use ns-server's metakv configuration
  system as the Cfg backend.

* An index in cbft is split or partitioned into multiple index
  partitions, known as PIndexes.  A PIndex has a 1-to-1 relationship
  with a bleve full-text index.

* This partitioning of indexes into PIndexes happens at index creation
  time (i.e., index definition time).  To keep things simple for now,
  we assume that the number of PIndexes that are allocated per index
  does not change over the life of an index (although that might be a
  future feature).

* As the cbft cluster topology changes, however, the assignment of
  which cbft nodes are responsible for which PIndexes can change.
  e.g., cbft node 00 is responsibile for "PIndex af977b".  When a
  second cbft node 01 joins the cbft cluster, the "planner" subsystem
  in cbft might reassign PIndex af977b from cbft node 00 to cbft node
  01.

* With regards to PIndex partitioning, a PIndex is configured (at
  index definition time) with a source of data (like a couchbase
  bucket).  That data source will have one or more source partitions
  (VBuckets).  For example, the beer-sample bucket has 1024 VBuckets.
  So, we could say something like...
  * "PIndex af977b" is assigned to cover VBuckets 0 through 199;
  * "PIndex 34fe22" is assigned to cover VBuckets 200 through 399;
  * and so on with more PIndexes up to VBucket 1023.

* A PIndex can be replicated, where each PIndex replica will be
  independently built up from from direct DCP streams from KV engines.
  That is, cbft uses star topology for PIndex replication instead of
  chain topology.  So, we could say something like...
  * "PIndex af977b", which is assigned to cover VBuckets 0 through 199,
  is assigned to cbft nodes 001 and 004.
  * When cbft node 004 is removed from the cbft cluster, then "PIndex
  af977b is reassigned to cbft nodes 001 and 002.

-------------------------------------------------
# cbftint Design

In this design section, we'll describe the planned steps of how
ns-server will spawn cbft nodes and orchestrate those cbft nodes along
with KV (memcached/ep-engine) nodes.  And, we'll describe scenarios of
increasing complexity in methodical, step-by-step fashion.

## Single CB Node, With Full-Text Service Disabled

In a single node CB cluster, or a so-called "cluster of one", the
simplest case is when the user hasn't enabled the cbft (or full-text)
node service type.  In this simple situation, we expect ns-server's
babysitter on that single node to not spawn any cbft processes.

NOTE: We don't expect the user to be able to toggle the node service
types on-demand for an existing, already initialized node.

## Single CB Node, With Full-Text Service Enabled

If the full-text node service type is enabled, we expect ns-server's
babysitter to spawn a cbft process, roughly something like...

    CBAUTH=some-secret \
    ./bin/cbft \
      -cfg=metakv \
      -tags=feed,janitor,pindex,queryer \
      -dataDir=/mnt/cb-data/data/@cbft \
      -server=127.0.0.1:8091 \
      -bindHttp=0.0.0.0:9110

A brief description of those cbft process parameters:

### CBAUTH=some-secret

This cbauth related environment variable(s) allows ns-server to pass
secret credentials to cbft.  This allows cbft to use the cbauth/metakv
golang libraries in order to...

* allow cbft to use metakv as distributed configuration (Cfg) store,
  such as for storing metadata like index definitions, node
  memberships, etc.

* allow cbft to access any couchbase bucket as a data-source (even if
  password protected or as the passwords are changed by
  administrators); that is, cbft will be able to retrieve cluster
  VBucket maps and create DCP streams for any bucket.

* allow cbft to authorize and protect its REST API, where cbft can
  perform auth checks on incoming queries, index definition API
  requests, and stats/monitoring API requests.

### -cfg=metakv

This command-line paramater tells cbft to use metakv as the Cfg
storage provider.

### -tags=feed,janitor,pindex,queryer

The tags parameter tells cbft to run only some subset of internal cbft
related services.  Of note, the 'planner' is explicitly _not_ listed
here, as this design proposal will leverage ns-server's master
facilities (ns-server's ability to dynamically determine a single
master node in a cluster) to more directly control invocations of
cbft's planner.

Using ns-server's master facilities will be a more reliable approach
than using cbft's basic behavior of having all cbft nodes concurrently
race each other in order to run their own competing planners.

Note that we do not currently support changing the tags list for a
node.  In particular, changing the "feed,janitor,pindex" tags would
have complex rebalance-like implications of needing pindex movement;
instead, users can rebalance-out a node and rebalance it back in with
a different set of node services.

### -dataDir=/mnt/cb-data/data/@cbft

This is the data directory where cbft can store index and
node-specific data and metadata.

Of note, the dataDir is not planned to be dynamically changable at
runtime.  If ns-server restarts cbft with a brand new, different
dataDir, the cbft node will behave (as expected) like a brand new
additional cbft node (with a differeng cbgt UUID).  On the other hand,
if a future version of ns-server instead carefully arranges to stop
cbft, move/copy the dataDir contents to a new location, and restart
cbft pointed at the new dataDir, the cbft node will resume with the
same cluster membership identity as expected (assuming there was no
file data corruption).

### -server=127.0.0.1:8091

This is ADDRL:PORT of the default couchbase server that cbft will use
for its data-source (container of buckets).

### -bindHttp=0.0.0.0:9110

This is the ADDR:PORT that cbft will listen for cbft's REST API.  The
bindHttp ADDR:PORT must be unique in the cluster.  (NOTE: We'll soon
talk about changing IP addresses below.)

## cbft process registers into the Cfg

At this point, as the cbft process starts up, cbft will save its node
definition (there's just a single cbft node so far) into the Cfg
(metakv) system.

## cbft is ready for index DDL and an index is created (IC0)

At this point, full-text indexes can be defined using cbft's REST API.
The cbft node will save any created index definitions into its Cfg
(metakv) system.

## A Managed, Global Planner

The Cfg system has a data-changes subscription feature, so programs
can be notified when data in the distributed Cfg changes.  We'll use
this feature and introduce a new, separate, standalone planner-like
program which will be used as a cluster-wide singleton.  ns-server's
master facilities will spawn, re-spawn and stop a single instance of
this new Managed, Global Planner (MGP) process (just a single instance
throughout the entire cluster).

The MGP is roughly equivalent to...

    cbft -tags=planner ...

There's a possibilility ns-server's master facilities might actually
have more than one master running with potential races between
concurrent masters.  That's suboptimal but ok, as cbft's planner (and
MGP) will use CAS-like features in the Cfg to determine Cfg update
race winners.

Of note, an Enterprise Edition of cbftint might ship with a more
advanced planner program, such as a planner than moves PIndexes with
more efficient orchestration.

## MGP updates the plan (UP0)

The MGP is awoken due to a Cfg change (IC0) and splits the index
definition into one or more PIndexes, and then assigns the PIndexes to
nodes (there's only one node so far, but even with multiple nodes, it
would be the same pathway).

The MGP then stores this updated plan into the Cfg.

## Individual cbft janitors see the plans were changed and try to
   cleanup the mess to match the latest plan.

Individual cbft janitors on the cbft nodes (there's just one) are
awoken due to the Cfg change (UP0) and create or shutdown PIndex
instances as appropriate.

## A PIndex instance creates DCP feeds

A PIndex instance creates DCP feeds, using the cluster map from the
"-server" ns-server, and connecting to the appropriate KV-engines.
Since we know the list of VBuckets that are assigned to a PIndex,
those DCP streams will each be created with the appropriate subset of
VBucket ID's.

## A KV Engine fails, restarts, VBuckets are moved/rebalanced, the
   VBucket cluster map changes, etc.

At this point, assuming the cbft process isn't moving, then a whole
series of KV Engine related scenarios are handled automatically by
cbft.  cbft uses the cbdatasource library, which has exponential
backoff retry logic when a DCP connection fails and DCP streams need
to be reconnected, even if VBuckets are moving/rebalancing.

In our example, we start with our first node...

* cb-00 - cbft enabled

Then, when we add/remove/failover/delta-node-recover any other node,
as long as those other nodes have cbft disabled, everything "should
just work" at this point in the story:

* cb-00 - cbft enabled
* cb-01 - cbft disabled
* cb-02 - cbft disabled

We're still essentially running just a cbft "cluster" of a single cbft
node, even though there are multiple KV nodes with VBuckets moving all
around the place.  The simplification here is that cbft doesn't look
much different from any other "external" application that's a user of
DCP.

One issue: as soon as the second node was added (cb-01), the IP
address of cb-00 might change (if not already explicitly specified on
cb-00's initialization web UI/REST screens), as this late-bound IP
address discovery is an ns-server feature.

## Handling IP address changes

At this point, the metadata stored in the Cfg includes the -bindHttp
command-line parameter, where cbft uses that bindHttp ADDR:PORT to
identify nodes and to implement scatter/gather queries.

A simple proposal is when ns-server discovers it must change the IP
address of a node (such as when cb-01 is added to the "cluster" of
cb-00), then ns-server must run some additional work...

* ns-server stops cbft's MGP (Managed, Global Planner).
* ns-server stops cbft.
* ns-server invokes a synchronous cmd-line tool/program (to be
  specified) that atomically rewrites the bindHttp ADDR:PORT's (e.g.,
  from 0.0.0.0:9110 to cb-00:9110).
* ns-server restarts cbft with the new -bindHttp ADDR:PORT
  (cb-00:9110).
* ns-server restarts cbft's MGP.

The above work should happen before any actual node joining or
rebalancing occurs.

Assumption: this design assumes that once an IP address is rewritten
and finalized, the IP address no longer changes for a node.

An alternative design: instead of using the "bindHttp" type of
approach, we could have each cbft node have a unique, generated node
UUID.  In addition, a separate mapping that allows clients (and the
queryer) to translate from node UUID's to actual IP addresses would
need to be managed, but that translation map can be more easilly,
dynamically changed.  This alternative design requires more cbft
changes and its extra level of indirection optimizes for an uncommon
case (IP address changing) so this alternative design is less favored.

## Multiple cbft nodes

After IP addresses are rewritten and finalized, more nodes can be
added into the cbft cluster.  Whenever cbft node cluster topology
changes, the MGP will notice (due to its Cfg subscription), and
it will re-plan any assignments of PIndexes to nodes.

The janitors in each cbft process will notice any changed PIndex
assignments (due to more Cfg subscriptions) and automatically
stop/start any DCP feeds as necessary.

This means multiple cbft nodes are now supported; for example, the
design now supports homogeneous topologies:

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

## It works, it's simple, but it's inefficient and lacks availability

Although we now support multiple cbft nodes in the cluster at this
point in the design, this simple approach is very resource intensive
and has availability gaps.

For example, in the "homogeneous topology", if we started out with
cb-00 and added nodes cb-01, cb-02 & cb-03 into the cluster at the
same time, then eventually all the janitors on those three new nodes
will awake at the same times and all start DCP streams at the same
time, leading to a huge load on the system (lots of KV backfills).

At the same time, the cbft janitor on cb-00 will awaken and stop
3/4th's of its PIndexes (the ones that have been reassigned to the new
nodes of cb-01/cb-02/cb-03).  So, any queries at this time will see
lack of data (assuming the PIndexes are slow to build on the new
nodes).

Let's address these issues one at a time, where we propose to fit into
the state-changes and workflow of ns-server's rebalance design.  In
particular, during rebalance here are some advanced ns-server
maneuvers (see ns-server's rebalance-flow.txt document)...

* ns-server can limit the number of concurrent KV backfills per node
  (usually to 1 KV backfill per node).
* ns-server disables compactions during some sub-phases of the
  rebalance moves to increase throughput.
* ns-server disables indexing during some sub-phases of the rebalance
  moves to increase throughput and to implement "consistent view
  queries under rebalance".
* ns-server prioritizes VBucket moves to try to keep number of active
  VBuckets "level" throughout nodes in the cluster.

## Increasing availability of PIndexes

TBD / design sketch - basically, cbftint will need to delete old
pindexes only after new pindexes are built up on the new nodes.  An
advanced MGP (Managed, Global Planner) could provide this
orchestration.

## Limit backfills

TBD / design sketch - each new pindex being built up on a new node
means KV backfills.  Need to throttle the number of concurrent "new
pindex builds".  An advanced MGP (Managed, Global Planner) could
provide this throttling.

## Controlled compactions of PIndexes

TBD / design sketch - cbft will need REST API's to disable/enable
compactions (or temporarily change compaction timeouts).  Again, this
is likely an area for the MGP to orchestrate.

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

(Perhaps ns-server invokes a synchronous cmd to unregister / delete
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

## MDS-RI - Multidimensional Scaling - ability to rebalance Full-Text
   indexes indpendent of other services.

## RRU-EE - Rebalance Resource Utilization More Efficient With
   Enterprise Edition.

## CIUR - Consistent Indexes Under Rebalance.

This is the equivalent of "consistent view index queries under
rebalance".

## QUERYR - Querying Replicas.

## QUERYLB - Query Load Balancing Amongst Replicas.

## QUERYLB-EE - Query Load Balancing Amongst Replicas, But Only With
   Enterprise Edition.

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

## COMPACTO - Ability to compact files offline (while server is down?)

Might be useful to recover from out-of-disk space scenarios.

## COMPACTP - Ability to pause/resume automated compactions (not
   explicitly forced).

## COMPACTC - Ability to configure/reconfigure automated compaction policy.

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

## REQTIME - Ability to track "where is the time going" on a
   per-request, individual request basis.

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

add cmd-line param where ns-server can force a default node UUID, for
better cross-correlation/debuggability of log events.  And, we can
look for the return of the shunned node.

add cmd-line tools/params for outside systems (like ns-server) to
add/remove nodes?

From rebalance-flow.txt, Aaron's diagram on concurrency...

              VBucket Move Scheduling
    Time
      |   /------------\
      |   | Backfill 0 |                       Backfills cannot happen
      |   \------------/                       concurrently.
      |         |             /------------\
      |   +------------+      | Backfill 1 |
      |   | Index File |      \------------/
      |   |     0      |            |
      |   |            |      +------------+   However, indexing _can_ happen
      |   |            |      | Index File |   concurrently with backfills and
      |   |            |      |     1      |   other indexing.
      |   |            |      |            |
      |   +------------+      |            |
      |         |             |            |
      |         |             +------------+
      |         |                   |
      |         \---------+---------/
      |                   |
      |   /--------------------------------\   Compaction for a set of vbucket moves
      |   |  Compact both source and dest. |   cannot happen concurrently with other
      v   \--------------------------------/   vbucket moves.

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
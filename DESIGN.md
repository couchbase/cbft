cbft + couchbase Integration Design Document

Status: DRAFT

This design document focuses on the integration of cbft into Couchbase
Server, with emphasis on clustering related features (rebalance,
failover, etc.).

-------------------------------------------------
# Links

Related documents:

* cbgt design documents
  * https://github.com/couchbaselabs/cbgt/blob/master/IDEAS.md (GT)
* ns-server design documents
  * https://github.com/couchbase/ns_server/blob/master/doc
  * https://github.com/couchbase/ns_server/blob/master/doc/rebalance-flow.txt (rebalance-flow.txt)

-------------------------------------------------
# cbft Design Recap

For those who haven't read about or have forgotten about the design of
cbft (couchbase full-text server), here are some main concepts.

* Multiple cbft nodes can be operated as cluster when they share the
  same configuration (or Cfg) backend database.  For the cbftint
  project, we plan to use ns-server's metakv configuration system as
  the Cfg backend.

* An index in cbft is split or partitioned into multiple index
  partitions (or PIndexes).  This partitioning happens at index
  creation or definition time and does not change over the life of the
  index.

* As the cbft cluster topology changes, however, the assignment of
  which cbft nodes are responsible for which (copies of) PIndexes can
  change.  e.g., when there are zero PIndex replicas configured, and
  cbft node 004 is added to a cluster, responsibility for "PIndex
  af977b" is reassigned by cbft's "planner" subsystem from cbft node
  003 to cbft node 004.

* With regards to PIndex partitioning, a PIndex is configured (at
  index definition time) with a data-source that has one or more
  source partitions (VBuckets).  For example, the beer-sample bucket
  has 1024 VBuckets.  So, we could say something like...
  * "PIndex af977b" is assigned to cover VBuckets 0 through 199;
  * "PIndex 34fe22" is assigned to cover VBuckets 200 through 399;
  * and so on up to VBucket 1023.

* A PIndex can be replicated, where each PIndex instance or replica
  will be independently built up from from direct DCP streams from KV
  engines.  That is, cbft uses star topology for PIndex replication
  instead of chain topology.  So, when there is 1 replica configured
  for an index (so, 2 copies), we could say something like "PIndex
  af977b has been assigned to cbft nodes 003 and 004".  When cbft node
  004 is removed from the cluster, then the instances of "PIndex
  af977b was reassigned to cbft nodes 003 and 002".

-------------------------------------------------
# cbftint Design

In this design section, we'll describe the planned steps of how
ns-server will spawn cbft nodes and orchestrate those cbft nodes along
with KV (memcached/ep-engine) nodes.  And, we'll describe scenarios of
increasing complexity in methodical, step-by-step fashion.

## Single CB Node, With Full-Text Service Disabled

In a single node CB cluster, or a so-called "cluster of one", the
simplest case is when the user hasn't enabled the cbft (or full-text)
node service type for the CB node.  In this simple situation, we
expect ns-server's babysitter to not spawn any cbft process.

## Single CB Node, With Full-Text Service Enabled

If the full-text node service type is enabled, we expect ns-server's
babysitter to spawn a cbft process, roughly something like...

    CBAUTH=<some-secret-from-ns-server> \
    ./bin/cbft \
      -cfg=metakv \
      -tags=feed,janitor,pindex,queryer \
      -dataDir=/mnt/cb-data/data/@cbft \
      -server=127.0.0.1:8091 \
      -bindHttp=0.0.0.0:9110

Roughly, those cbft process parameters mean...

### CBAUTH=<some-secret-from-ns-server>

This cbauth related environment variable(s) allows ns-server to pass
secret credentials to cbft so that cbft can use the cbauth/metakv
golang libraries in order to...

* allow cbft to use metakv as distributed configuration (Cfg) store,
  such as for storing metadata like index definitions, node
  membership, etc.

* allow cbft to access any couchbase bucket as a data-source (even if
  password protected); that is, cbft will be able to retrieve cluster
  VBucket maps and create DCP streams for any bucket.

* allow cbft to authorize and protect its REST API, where cbft can
  provide auth checks for incoming queries, index definition API
  requests, and stats/monitoring API requests.

### -cfg=metakv

This command-line paramater tells cbft to use metakv as the Cfg
storage provider.

### -tags=feed,janitor,pindex,queryer

This tells cbft to run only some cbft-related services.  Of note, the
'planner' is explicitly _not_ listed here, as this design proposal
leverages ns-server's master orchestrator to more directly control
invocations of cbft's planner.  Using ns-server's "master global
singleton" will be a more reliable approach as opposed to using cbft's
normal behavior of having all cbft nodes concurrent race each other in
order to run their own competing planners.

Explicitly listing tags also leaves room for a future, potential
ability to have certain nodes only run cbft's queryer, for even more
advanced dimensions of multidimensional scaling (query-only nodes).

Note that we do not currently support changing the tags list for a
node.  In particular, changing the "feed,janitor,pindex" tags would
have rebalance-like implications of needing pindex movement.

### -dataDir=/mnt/cb-data/data/@cbft

This is the data directory where cbft can save metadata and index
data.

Of note, the dataDir is not planned to be dynamically changable at
runtime.  If ns-server restarts cbft with a brand new, different
dataDir, the cbft node will behave (as expected) like a brand new node
and rebuild indexes from scratch.  Also, if ns-server instead
carefully arranges to stop cbft, move/copy the dataDir contents to a
new location, and restart cbft pointed at the new dataDir, cbft will
resume as expected (assuming there was no file data corruption).

### -server=127.0.0.1:8091

This is default couchbase server that cbft will use for its
data-source (container of buckets).

### -bindHttp=0.0.0.0:9110

This is the ADDR:PORT that cbft will listen for cbft's REST API.  It
must be unique in the cluster.  (NOTE: We'll soon talk about changing
IP addresses below.)

## cbft process registers into the Cfg

At this point, as part of cbft starts up, cbft will save its node definition
(there's just a single node so far) into the Cfg (metakv) system.

## cbft is ready for index DDL

At this point, full-text indexes can be defined using cbft's REST API.
The cbft node will save index definition into its Cfg (metakv) system.



When an index is defined, nd cbft will create DCP streams to the single KV node.

upgrades



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

Clients should be able to ask, "I want query results where the
full-text-indexes have incorporated at least up to this set of
{vbucket to seq-num} pairings."

## GT-CR1 - Consistent queries under datasource rebalance.

Full-text queries should be consistent even as data source (Couchbase
Server) nodes are added and removed in a clean rebalance.

## GT-CR2 - Consistent queries under cbgt topology change.

Full-text queries should be consistent even as cbgt nodes are added
and removed in a clean takeover fashion.

## GT-OC1 - Support optional looser "best effort" options.

The options might be along a spectrum from stale=ok to totally
consistent.  The "best effort" option should probably have lower
latency than a totally consistent CR1 query.

For example, perhaps the client may want to just ask for consistency
around just one vbucket-seqnum.

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

This is the ability to query multiple indexes across multiple buckets
in a single query, such as "find any docs from the customer, employee,
vendor buckets who have an address or comment about 'dallas'".

## GT-NI1 - Resilient to datasource node down scenarios.

If a data source (couchbase cluster server node) goes down, then the
subset of a cbgt cluster that was indexing data from the down node
will not be able to make indexing progress.  Those cbgt instances
should try to automatically reconnect and resume indexing from where
they left off.

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

## GT-NQ1 - Querying still possible if datasource node goes down.

Querying of a cbgt cluster should be able to continue even if some
datasource nodes are down.

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

Even failover in the midst of cbft rebalance ("put the pencils down").

## FOG - Graceful Failover.

Reject any new requests and wait for any inflight requests to finish
before failover.

## AB - Add Back Rebalance.

## DNR - Delta Node Recovery.

## RP1 - Rebalance Phase 1 - VBucket Replication Phase.
## RP2 - Rebalance Phase 2 - View Indexing Phase.
## RP2 - Rebalance Phase 3 - VBucket Takeover Phase.

## RSTOP - Ability to Stop Rebalance.

## MDS-RI - Multidimensional Scaling - ability to rebalance Full-Text
   indexes indpendent of other services.

## RRU-EE - Rebalance Resource Utilization More Efficient With
   Enterprise Edition.

## CIUR - Consistent Indexes Under Rebalance.

This is the equivalent of "consistent view indexes under rebalance".

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
frees more space) then the full-text system should be able to
automatically continue successfully.

## KP - Killed Processes (linux OOM, etc).

## RSN - Return of the Shunned Node.

## DLC - Disk Level Copy/Restore of Node.

This is the scenario when a user "clones" a node via disk/storage
level maneuvers, such as incorrect usage of EBS snapshot or tar'ing up
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

## RSPREAD - Rebalance Resource Spreading.

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

## UPGRADE - Future readiness for upgrades.

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

-------------------------------------------------
# Random notes / TODO's

ip address changes on node joining
- node goes from 'ns_1@127.0.0.1' (or 0.0.0.0?)
    to ns_1@REAL_IP_ADDR
ip address rename

bind-addr needs a REAL_IP_ADDR
from the very start to be clusterable?

all nodes tart of on node with "wrong" bindHTTP addr (like 127.0.0.1
or 0.0.0.0), so ideas:
- fix reliance on bindHTTP
- allow true node UUID, with "outside" mapping of UUID to contactable http address
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

idea: never run planner in cbftint?  But allow ns-server to invoke
planner on as-needed basis whenever ns-server needs (during a master
orchestrator / DML change event).  Then EE edition could allow a more
advanced planner.

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

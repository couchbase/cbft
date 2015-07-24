cbft + couchbase Integration Design Document

Status: DRAFT

This design document focuses on integrating cbft with Couchbase Server
and focuses especially on integrating with couchbase's features like
Rebalance, Failover, etc.

Contents

* Links
* Design
* Requirements Review

-------------------------------------------------
# Links

References to related documents:

* cbgt design - https://github.com/couchbaselabs/cbgt/blob/master/IDEAS.md (GT)
* ns-server documents, especially "rebalance flow"...
  * https://github.com/couchbase/ns_server/blob/master/doc
  * https://github.com/couchbase/ns_server/blob/master/doc/rebalance-flow.txt (rebalance-flow.txt)

-------------------------------------------------
# Design

In this design section, we describe the planned steps of how ns-server
will spawn cbft nodes and orchestrate them with KV nodes
(memcached/ep-engine) during scenarios of increasing complexity.

## Single CB Node Starts

In a single node CB cluster (a "cluster of one"), the simplest case is
when the user hasn't enabled the cbft ("Full-text") node service type
for the CB node.  In this case, we expect ns-server/babysitter to not
spawn any cbft process.

If a Full-text service type is enabled for the single CB node, we also
expect the ns-server/babysitter to spawn a cbft node roughly something
like...

    CBAUTH=<some-auth-secret-from-ns-server> \
    ./bin/cbft \
      -cfg=metakv \
      -tags=feed,janitor,pindex,queryer \
      -dataDir=/mnt/cb-data/data/@cbft \
      -server=127.0.0.1:8091 \
      -bindHttp=0.0.0.0:9110

Roughly, those parameters mean...

### CBAUTH

This cbauth related environment variable(s) allows ns-server to pass
secret credentials to cbft so that cbft can use the cbauth/metakv
golang libraries in order to...

* allow cbft to use metakv as distributed configuration store, as a
  place where cbft can store metadata for index definitions, node
  membership, and more.

* allow cbft to access any CB bucket (whether password protected or
  not) as a data-source (and create DCP streams to those buckets).

* allow cbft to authorize and protect its REST API, where cbft can
  provide auth checks for queries, index definition API's and its
  stats/monitoring API's.

### -cfg=metakv

This tells cbft to use metakv as the Cfg storage provider.

### -tags=feed,janitor,pindex,queryer

This tells cbft to run only some cbft-related services.  Of note, the
planner is explicitly _not_ listed, as this design proposal leverages
ns-server's master orchestrator to more directly control invocations
of the planner (see below).  This will be a more reliable approach as
opposed to cbft's normal behavior of having all cbft nodes "race each
other" in order to run their own competing planners.

This explicit tags approach also leaves room for a future, potential
ability to have certain nodes only run cbft's queryer, for even more
advanced dimensions of multidimensional scaling.

Note that we do not currently support changing the tags list for a
node.  In particular, changing the "feed,janitor,pindex" tags would
have rebalance-like implications of needing pindex movement.

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

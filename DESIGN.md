cbft + couchbase Integration Design Document

Status: DRAFT

This design document focuses on integrating cbft with Couchbase Server
and focuses especially on integrating with couchbase's features like
Rebalance, Failover, etc.

-------------------------------------------------
# Links

References to related documents:

* cbgt design - https://github.com/couchbaselabs/cbgt/blob/master/IDEAS.md (GT)
* ns-server documents, especially "rebalance flow"...
  * https://github.com/couchbase/ns_server/blob/master/doc
  * https://github.com/couchbase/ns_server/blob/master/doc/rebalance-flow.txt (rebalance-flow.txt)

-------------------------------------------------
# Requirements

(NOTE: We're currently missing a formal PRD (product requirements
document), so these requirements are based on anticipated PRD
requirements.)

## GT-CS1 - Consistent queries during stable topology.

(Requirements with the "GT-" prefix originally come from the cbgt
IDEAS.md design document.)

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

## IPAC - IP Address Changes.

IP address discovery is "late bound", when couchbase server nodes
initially joins to a cluster.  A "cluster of one", in particular, only
has an erlang node address of "ns_1@127.0.0.1".

## BUCKETD - Bucket Deletion Cascades to Full-Text Indexes.

If a bucket is deleted, any full-text indexes based on that bucket
should be also automatically deleted.

There whould be user visible UI warnings on these "cascading deletes"
of cbft indexes.

## RIO - Rebalance Nodes In/Out.

## RP - Rebalance progress estimates/indicator.

## RS - Swap Rebalance.

## FOH - Hard Failover.

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

## QUERYLB - Querying Load Balancing.

## QUERYLB-EE - Query Load Balancing To Replicas With Enterprise Edition.

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

## RPMOVE - Partition Moves Controlled Under Rebalance.

During rebalance, ns_server limits outgoing moves to a single vbucket
per node for efficiency (see rebalance-flow.txt).  Same should be the
case for PIndex reassignments.

## RSPREAD - Rebalance Resource Spreading.

ns_server prioritizes VBuckets moves that equalize the spread of
active VBuckets across nodes and also tries to keep indexers busy
across all nodes.  PIndex moves should have some equivalent
optimization.

## TOOLBR - Tools - Backup/Restore.

## TOOLCI - Tools - cbcollectinfo.

## TOOLM - Tools - mortimer.

## TOOLN - Tools - nutshell.

## UPGRADE - Future readiness for upgrades.

-------------------------------------------------
# Random notes / TODO's

ip address changes on node joining
- node goes from 'ns_1@127.0.0.1'
    to ns_1@REAL_IP_ADDR
ip address rename

bind-addr needs a REAL_IP_ADDR
from the very start to be clusterable?

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
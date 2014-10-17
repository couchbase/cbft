A file for ideas/notes...

---------------------------------------------
Requirements

Every requirement has a unique ID for easy reference (like "CS1"),
where any numbers don't mean anything (especially such as priority).

CS1 - Consistent queries during stable topology.

Clients should be able to ask, "I want query results where the
full-text-indexes have incorporated at least up to this set of
{vbucket to seq-num} pairings."

For example, the app does some mutations; then the app
does some full-text query.  The app will want the full-text
results to have incorporated at least their mutations.

Of note, concurrent clients might be racing each other,
but the idea is that when we simplify to just a single
client with no system failures, it should work as expected.

CR1 - Consistent queries under datasource rebalance.

Full-text queries should be consistent even as data source nodes are
added and removed in a clean rebalance.

Implementation note: maybe the implementation for CS1 will help us
get CR1 "for free".

CR2 - Consistent queries under cbft topology change.

Full-text queries should be consistent even as cbft nodes are added
and removed in a clean takeover fashion.

Implementation sketch: perhaps don't blow away the index on the old
node until the new node has built up the index; and perhaps there's
some takeover handshake?

OC1 - Support optional looser "best effort" options (along a spectrum
to totally consistent) that's less expensive than a totally consistent
CR1 implementation.

For example, perhaps the client may want to just ask for consistency
around just one vbucket.

IA1 - Index aliases.

Level of indirection to help split data across multiple indexes, but
also not change your app all the time.  Example: query from
'recent-sales' index, but only want to search the most recent quarter
index of 'sales-2014Q3', which an administrator can dynamically remap
to the 'sales-2014Q4' index.

MQ1 - Multi-index query for a single bucket.

MQ2 - Multi-index query across multiple buckets.

This is the ability to query multiple indexes across multiple buckets
in a single query, such as "find any docs from the customer, employee,
vendor buckets who have an address or comment about 'dallas'".

Example:

Buckets: customer, employee, vendor

Indexes: customer_by-address (fields: addr, city, state, country),
         customer_by-comments,
         employee_by-address,
         vendor_by-address,
         vendor_by-star-rating

The client wants a query hits this subset of indexes and merge results:
       customer_by-address,
       customer_by-comments,
       employee_by-address,
       vendor_by-address
       (but not the vendor_by-star-rating)

Implementation sketch - although there might be many separate PIndexes
for each bucket (and vbucket), the Queryer should be able to
scatter/gather across those PIndexes and merge results together to meet
this requirement.  But, beware of relevance count issues!

Implies that user is using the indexes the same way (no mismatched
types: string vs integers).

Note: foreign key case, for example, might lead to unexpected matches.

Note: this multi-bucket requirement might be incompatible with
couchbase bucket "container" semantics?

In ES, an index alias can point to multiple indexes to support MQ1.

NI1 - Resilient to datasource node down scenarios.

If a data source (couchbase cluster server node) goes down, then the
subset of cbft that were indexing data from the down node will not
be able to make indexing progress.  Those cbft instances
should try to automatically reconnect and resume indexing.

E1 - The user should be able to see error conditions (e.g., yellow / red
color) on node down and other error conditions.

Question: how to distinguish between I'm behind (as normal) versus
I'm REALLY behind on indexing.  Example: in 2i project, it can detect
that "I'm sooo far REALLY behind that I might as well start from zero
instead of trying catch up with all these mutation deltas that
will be throwaway work".

In ES, note the frustrating bouncing between yellow, green, red;
ns-server example, not enough CPU & timeouts leads to status
bounce-iness.

NQ1 - Querying still possible if datasource node goes down.

Querying of cbft should be able to continue even if
some datasource nodes are down.

PI1 - Ability to pause/resume indexing.

---------------------------------------------
Imaginary N1QL syntax...

  CREATE FULLTEXT INDEX XXX on Bucket (...optional params...);

  CREATE FULLTEXT INDEX customer_FTI on customers;

Especially, separation of indexes from bucket terminology.

The phrase "I want to do a full-text query on a bucket" isn't quite
right.  Instead, we're going for "you can do a full-text query on a
full-text index on a bucket".

Also, the index aliases proposed above might not belong to any single
bucket.

---------------------------------------------
Proposed highlevel design concepts and "subparts"...

Inside a single cbft process...

- PIndex (an "Index Partition" that consumes StreamRequests)

- Streams (a channel of StreamRequest)

- Feed (hook up a data source & pushes requests
        into 1 or more StreamRequests)

- Manager (manages a set of PIndexes, StreamRequests, and Feeds)

-- Planner (assign partitions to cbft nodes and PIndexes;
            makes the "plan" or "map"; which are written to the Config)

-- Janitor (tries to make reality match the plan in the Cfg by
            starting & stopping local PIndexes and Feeds as needed)

- Queryer (scatter/gathers across relevant PIndexes)

- Cfg (a distributed, consistent config database)

Every cbft node is homogeneous for a simple deployment story.

An PIndex (a.k.a, an Index Partition, or a "Physical Index") consumes
a Stream and maintains a single bleve index.  This PIndex *might* be
covering just be a partition of a larger index, but an PIndex doesn't
really know.  Higher levels of the system (Manager) have logical index
to PIndex mapping.  An PIndex, in contrast, just knows about a single
Stream as its input, and a PIndex doesn't know about couchbase,
buckets, vbuckets, or DCP/TAP.

A Stream is a channel of StreamRequest objects, which might represent
mutations (document updated, deleted), or "administrative" requests
(like please-shutdown, compact, delete-index, snapshot, negotitate
rollback/restart, negotiate a checkpoint, etc).

A Feed is an interface that will have different implementations
(TAPFeed, DCPFeed, TestFeed, etc) that pumps requests into a Stream.
A Feed is responsible for connecting (and reconnecting) to a data
source.  A TestFeed, for example, can send a whole series of
interesting requests down a Stream for testing difficult scenarios.
During reconnections to wobbly data sources, it's the responsibility
of the different Feed implementations to implement backoff strategies.

A Manager is a collection of PIndex'es, Streams, and Feeds.  It has
the mapping of buckets and vbuckets/partitions to
PIndexes/Streams/Feeds.  A Manager singleton will be that single
"global" object in a cbft process rather than having many global
variables (although for testing, there might be many Manager instances
in a test process to validate concurrency scenarios, etc).  A Manager
has API to create and delete logical indexes for higher levels of cbft
(like admin REST endpoints).

The Manager has helpers: Planner & Janitor.  When a new logical "full
text index" is created for a bucket, for example, the Manager engages
its Planner to assign partitions/vbuckets to PIndexes across cbft
instances.  The Janitor will detect "messes" (divergence from plans to
reality) and will make moves to help change reality to be closer to
the plan, such as by creating/deleting PIndexes, Feeds and Streams.  A
Planner, then, decides the 1-to-1, 1-to-N, N-to-1, N-to-M
fan-in-or-out assignment of partitions to PIndexes.

A Cfg is some consistent, distributed database; think: gometa, etcd,
zookeeper kind of system.  It needs a "watcher" ability where clients
can subscribe to data changes (configuration changes).

A Queryer can query against one or more PIndexes (perhaps even one day
to remote PIndexes by communicating with remote Queryers).  Initially,
perhaps it can only do just a single PIndex, but the API should be
multi-PIndex ready.

A HTTP/REST (and next-generation protocol / green-stack) networking
layer sits on top of all of it for index mgmt and querying endpoints
that clients can access.  During a query, this networking layer
accesses a Manager for the relevant mapping and invokes the Queryer
with the PIndexes that need to be accesses.  This networking layer will
provide the necessary AUTH checks.

---------------------------------------------
What happens when creating a full-text index...

Let's "follow a request" through the system of a user creating a
logical full-text index.  The user supplies inputs of data source
bucket, indexName, indexMapping, using a client SDK that eventually
communicates with some cbft instance (doesn't matter which one).

10 Then Manager.CreateIndex() on that cbft instance is invoked with
the creation arguments.

20 The Manager saves logical full-text instance configuration data to
the Cfg system.

30 The Cfg store should have enough "watcher" or pub/sub capability so
that any other subscribed cbft instances can hear about the news that
the configuration changed.

40 So, Planners across the various cbft nodes across the cbft cluster
will get awoken (hey, something changed, there's planning to do!)...

50 ASIDE: By the way, each cbft instance or process has its own
unique, persistent cbft-ID (likely saved in the dataDir somwhere).

52 And all those cbft-ID's will also listed in the Cfg.

53 That is, when a cbft instance starts up it writes its cbft-ID and
related instance data (like, here's my address, and I got N cpus and M
amount of RAM here) to the Cfg.

54 Only some subset of cbft-ID's, however, are "wanted" by the user or
sysadmin.

56 The sysadmin can use other API's to mark some of the known cbft
instances in the Cfg as "wanted".

58 END ASIDE.

60 So, each awoken Planner in a cbft instance will work independently.

70 The Planner takes input of logical full-text configuration, the
list of wanted cbft-instances, the CB cluster vbucket map, and AllowedVersion.

72 If any of the above inputs changes, the Planner needs to be
re-awoken and re-run.

80 The Planner functionally (in a deterministic, mathematically
function sense) computes an assignment of partitions or vbuckets to
PIndexes and assigns those PIndexes to cbft instances.

90 So, there are N indepedent Planners running across the cbft cluster
that independently see something needs to be planned, and assumming the
planning algorithm is deterministic, each Planner instance should come
up with the same plan, the same determinstic calculation results, no matter
where it's running on whatever cbft node.

92 ASIDE: what about cases of versioning, where some newer
software versions, not yet deployed and running homogenously on every
node, have a different, improved Planning algorithm?

94 For multi-versioning, a Planner must respect their AllowedVersion
input.  A newer deployed version of the Planner writes an updated
AllowedVersion into the Cfg.  Older Planners should then stop working.

100 The first version Planner might be very simple, such as a basic
1-to-1 mapping.  For example, perhaps every cbft-instance receives
_every_ vbucket partition into a single PIndex instead of actually
doing real partitoning (so, that handles the "single node"
requirements of the first Developer Preview).

110 The hope is if we improve the Planner to be smarter over time,
where there's enough separation of responsibilites here so that the
later parts of the system don't care so much

120 The Planners will then save the plans down into the Cfg so
that later parts of the system can use it as input.

122 Also clients will be able to access the plan from the Cfg in order
to learn of the expected locations of PIndexes across the cbft
cluster.

130 Some CAS-like facility in Cfg will be necessary to help make this
work well.

140 There might be some concern that planning won't be deterministic,
because planning might need to include things like, cpu utilization,
disk space, number of Pindexes already on a node, etc.

142 The key idea is that re-planning should only be done on topology
changes (add/remove wanted cbft nodes or logical config additions (add
logical full-text index).  If CPU utilization changes, that is, don't
do replanning, similar to how we don't do an automatic Rebalance in CB
if CPU on just a single CB node temporarily spikes.

144 General machine "capability level" (4 cpus vs 32 cpus) can be
input into Planning, to be able to handle heterogeneous machine types,
where we expect # of CPU's won't change per machine; or, even if #
cpu's does change, we won't replan.

146 A related thought is we'd want to keep PIndex assignments across a
cbft cluster relatively stable (not try to move or rebuild potentially
large bleve index files at the drop of a hat).

160 Consider the case where a new cbft node joins and is added to the
"wanted" list.  Some nodes have seen the news, others haven't yet, so
it's an inherently race-full situation where Planners are racing
to recompute their plans.

162 One important assumption here that the Cfg system provides a
consistent answer.

164 And, the Cfg system provides some CAS-like facilities for
any Planners that are racing to save their latest plans.

166 Still, some cbft nodes might be slow in hearing the news,
and clients must be careful to handle this situation.

170 Eventually, a cbft instance / Manager / Planner is going to have a
new, latest & greatest plan that's different than it's previous plan.
Next, responsibility switches to the Janitor.

180 Each Janitor on a cbft instance knows its cbft-ID, and can focus
on the subset of the plan related to that cbft-ID.

190 A Janitor can then create or delete local PIndexes (bleve indexes)
and setup/teardown Feeds as needed to match the subset of the plan.

200 Care must be taken so that any inflight queries are handled well
during these PIndex shutdowns and deletions.

300 If the planning does however turn out to be
non-derministic/non-functional, then we can still use the Cfg system
to help determine a single, leased master Planner to do the planning
and write results into the Cfg

310 But, even with a single, elected master Planner, it'll still take
some time for news of the new plan to get out to all the cbft
instances; so, beware the inherently concurrent race conditions here.

320 For example, a Queryer might try to contact some instances that
haven't heard the news yet.

330 To alleviate that, one thought is perhaps a Queryer might try to
ask all its target cbft nodes "are you up to plan #12343?" before
doing a full query across those nodes?  Or, only do this PIndex query
if you are up to plan #12343, and I'm willing to wait/block for T
timeout for you to get up to date.

400 The hope is the cbft instances are all homogeneous, and during
their independent Planning that they also don't have to talk to each
other but can separately arrive at the same answers.



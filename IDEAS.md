A file for ideas/notes...

---------------------------------------------
Requirements

Every requirement listed here has a unique ID for easy reference (like
"CS1"), where the number suffix doesn't mean anything (especially, the
number does not connote priority).

# CS1 - Consistent queries during stable topology.

Clients should be able to ask, "I want query results where the
full-text-indexes have incorporated at least up to this set of
{vbucket to seq-num} pairings."

For example, the app does some mutations; then the app does some
full-text query.  The app will want the full-text results to have
incorporated at least their mutations.

Of note, concurrent clients might be racing each other, but the idea
is that when we simplify to just a single client with no system
failures, the client should get expected results.

# CR1 - Consistent queries under datasource rebalance.

Full-text queries should be consistent even as data source (Couchbase
Server) nodes are added and removed in a clean rebalance.

# CR2 - Consistent queries under cbft topology change.

Full-text queries should be consistent even as cbft nodes are added
and removed in a clean takeover fashion.

Implementation sketch: perhaps don't blow away the index on the old
node until the new node has built up the index; and perhaps there's
some takeover handshake?

# OC1 - Support optional looser "best effort" options (along a
spectrum from stale=ok to totally consistent) that has lower latency
than a totally consistent CR1 query.

For example, perhaps the client may want to just ask for consistency
around just one vbucket-seqnum.

# IA1 - Index aliases.

This is a level of indirection to help split data across multiple
indexes, but also not change your app all the time.  Example: the
client wants to query from 'last-quarter-sales', but that means a
search limited to only the most recent quarter index of
'sales-2014Q3'.  Later, an administrator can dynamically remap the
'last-quarter-sales' alias to the the newest 'sales-2014Q4' index
without any client-side application changes.

# MQ1 - Multi-index query for a single bucket.

This is the ability to query multiple indexes in one request for a
single bucket, such as the "comments-fti" index and the
"description-fti" index.

Implementation sketch: an index alias that "fans-out" to multiple
actual indexes might be useful to support this requirement.

# MQ2 - Multi-index query across multiple buckets.

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

# NI1 - Resilient to datasource node down scenarios.

If a data source (couchbase cluster server node) goes down, then the
subset of a cbft cluster that was indexing data from the down node
will not be able to make indexing progress.  Those cbft instances
should try to automatically reconnect and resume indexing from where
they left off.

# E1 - The user should be able to see error conditions (e.g., yellow /
red color) on node down and other error conditions.

Question: how to distinguish between I'm behind (as normal) versus
I'm REALLY behind on indexing.  Example: in 2i project, it can detect
that "I'm sooo far REALLY behind that I might as well start from zero
instead of trying catch up with all these mutation deltas that
will be throwaway work".

In ES, note the frustrating bouncing between yellow, green, red;
ns-server example, not enough CPU & timeouts leads to status
bounce-iness.

# NQ1 - Querying still possible if datasource node goes down.

Querying of a cbft cluster should be able to continue even if some
datasource nodes are down.

# PI1 - Ability to pause/resume indexing.

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

- PIndex - an "Index Partition".

- Feed - hooks up to a data source & pushes data + requests
         into 1 or more PIndexes.

- Manager - manages a set of PIndexes and Feeds;
            also manages a Planner and Janitor.

-- Planner - assigns datasource partitions to PIndexes
             and then assigns those PIndexes to nodes.

-- Janitor - tries to make reality match the Planner's plan
             by starting & stopping local PIndexes and Feeds
             as needed.

- Queryer - scatter/gathers across relevant PIndexes.

- Cfg - a distributed, consistent config database
        where index definitions and plans can be stored.

Every cbft node is homogeneous in order to provide a simple story for
deployment.

An PIndex (a.k.a, an Index Partition, or a "Physical Index") will have
different backend implementation types, and a PIndex of type "bleve"
maintains a single bleve index.  A PIndex *might* be covering just a
subset of a larger index, but an PIndex doesn't really know that.
Higher levels of the system (Manager/Planner/Janitor) have a logical
index (aka, "LIndex") to PIndex mapping.  A PIndex, in particular,
doesn't really know about data-source concepts like couchbase cluster,
buckets, vbuckets, or DCP/TAP.

A Feed is an interface that will have different implementations
(TAPFeed, DCPFeed, TestFeed, etc).  A Feed is responsible for
connecting to a data source (and reconnecting as necessary).  A Feed
pumps updates from its data source into one or more PIndexes.  A
TestFeed, for example, can send a whole series of interesting updates
to PIndexes for testing difficult scenarios.  Of note, a PIndex
doesn't know about Feeds, so that any code (like test code) might be
driving updates to a PIndex's API.  During scenarios to flapping or
wobbly data sources, it's the responsibility of the different Feed
implementations to implement reconnection backoff strategies.

A Manager has a collection of PIndexes and Feeds and manages the
hook-ups between them.  A Manager singleton will be that single
"global" object in a cbft process rather than having many global
variables (although for testing, there might be many Manager instances
in a test process to validate difficult cluster scenarios, etc).  A
Manager has API to list, create and delete logical indexes for use by
higher levels of cbft (like admin REST endpoints).

The Manager has helpers: Planner & Janitor.  When a new logical index
is created, for example, the Manager engages its Planner to assign
data source partitions to PIndexes and also to assign those PIndexes
across cbft nodes.  A Planner, then, decides the 1-to-1, 1-to-N,
N-to-1, N-to-M fan-in-or-out assignment of partitions to PIndexes.

Once the Planner has updated the plans, the Janitor then will detect
"messes" (divergence from plans to reality) and will make moves to
help move reality to be closer to the plan, such as by
creating/deleting PIndexes and Feeds.

A Cfg is some consistent, distributed database; think: gometa, etcd,
zookeeper kind of system.  It needs a "watcher" ability where clients
can subscribe to data changes (configuration changes).

A Queryer can query against one or more PIndexes (perhaps even one day
to remote PIndexes by communicating with remote Queryers).  Initially,
perhaps a simple Queryer can only hit just a single PIndex, but the
API and interfaces should be multi-PIndex and scatter/gather ready for
the future.

A HTTP/REST (and next-generation protocol / green-stack) networking
layer sits on top of all of it for index management and querying
endpoints that clients can access.  During a query, this networking
layer accesses a Manager for the relevant mapping and invokes the
Queryer with the PIndexes that need to be accessed.  This networking
layer will provide the necessary AUTH checks.

---------------------------------------------
What happens when creating a full-text index...

Let's "follow a request" through the system of a user creating a
logical full-text index.  The user supplies inputs of data source
bucket, indexType, indexName, indexParams, using a client SDK that
eventually communicates with some cbft instance (doesn't matter which
one).

10 Then Manager.CreateIndex() on that cbft instance is invoked with
the creation arguments.

20 The Manager saves logical full-text instance configuration data
(aka, a new IndexDef) to the Cfg system.

30 The Cfg store should have enough "watcher" or pub/sub capability so
that any other subscribed cbft instances can hear about the news that
the Cfg has changed.

40 So, Planners across the various cbft nodes across the cbft cluster
will be awoken (hey, something changed, there's (re-)planning
needed).

50 ASIDE: By the way, each cbft instance or process has its own
unique, persistent cbft-ID (likely saved in the dataDir somewhere).

52 And all those cbft-ID's will also be listed in the Cfg.

53 That is, when a cbft instance starts up it writes its cbft-ID and
related instance data (like, here's my address, and I have N cpus and
M amount of RAM here) to the Cfg.

54 Those brand new cbft instances, however, are not engaged right
away.  Only some subset of cbft-ID's, however, will be explicitly
listed as "wanted" by the user or sysadmin.  This is akin to how
Couchbase Server has a separate step or state between Add Server and
Rebalance.

56 The sysadmin can use some API's to mark some subset of the known
cbft instances as "wanted" in the Cfg.

58 END ASIDE.

60 Each awoken (or "kicked") Planner in a cbft instance will work
independently of its concurrent peers in the cluster.

70 The Planner takes input of logical full-text configuration (the
IndexDef's), the list of wanted cbft-instances, and version info.

72 If any of the above inputs changes, the Planner needs to be
re-awoken and re-run.

80 The Planner functionally (in a deterministic, mathematically
function sense) computes an assignment of partitions (in this case,
vbuckets) to PIndexes and then also functionally assigns those
PIndexes to cbft instances.

90 So, there are N indepedent Planners running across the cbft cluster
that independently see something needs to be planned, and assumming
the planning algorithm is deterministic, each Planner instance should
come up with the same plan, the same determinstic calculation results,
no matter where it's running on whatever cbft node.

92 ASIDE: what about cases of versioning, where some newer software
versions, not yet deployed and running homogenously on every node,
have a different, improved Planning algorithm?

94 For multi-versioning, a Planner must respect its version input
parameter.  A newer deployed version of the Planner writes an updated
version into the Cfg.  Older Planners should then stop working when
they detect that their version is outdated.

96 END ASIDE.

100 The first version Planner might be very simple, such as a basic
1-to-1 mapping.  For example, perhaps every cbft-instance receives
_every_ vbucket partition into a single PIndex instead of actually
doing real partitoning (so, that handles the "single node"
requirements of the first Developer Preview).

110 The hope is if we improve the Planner to be smarter over time,
there should be enough separation of responsibilites in the design
here so that the later parts of the system don't need to change so
much when a Planner is upgraded.

112 ((feedback from Alk) What about huge single terms, like
"user:gender" or "user:is-root-admin"?  Sketch answer: it's still
splittable as docID's are suffx'ed onto term keys, so a future Planner
can take that into account.)

120 The Planners will then save the plans down into the Cfg so
that later parts of the system can use it as input.

122 Also clients will be able to access the plan from the Cfg in order
to learn of the expected locations of PIndexes across the cbft
cluster.

130 Some CAS-like facility in Cfg will be necessary to make this
work and have consistent, converging outcomes.

140 There might be some concern that planning won't be deterministic,
because planning might need to include things like, cpu utilization,
disk space, number of Pindexes already on a node, etc.

142 The key idea is that re-planning should only be done on topology
changes (add/remove wanted cbft nodes or logical config changes
(add/remove logical full-text index)).  In contrast, if CPU
utilization changes, we won't do replanning, similar to how we don't
do an automatic Rebalance in Couchbase if CPU on just a single
Couchbase node temporarily spikes.

144 General machine "capability level" (4 cpus vs 32 cpus) can be
input into Planning, to be able to handle heterogeneous machine types,
where we expect # of CPU's won't change per machine; or, even if #
cpu's does change, we won't blithely replan.

146 A related thought is we'd want to keep PIndex assignments across a
cbft cluster relatively stable (not try to move or rebuild potentially
large, persisted PIndex files at the drop of a hat).

160 Consider the case where a new cbft node joins and is added to the
"wanted" list.  Some nodes have seen the news, others haven't yet, so
it's an inherently race-full situation where Planners are racing
to recompute their plans.

162 One important assumption here that the Cfg system provides
consistent storage semantics.

164 And, the Cfg system should provide some CAS-like facilities for
any Planners that are racing to save their latest plans.

166 Still, some cbft nodes might be slow in hearing the news, and
clients must be careful to handle this situation of out-of-date cbft
nodes, which is a guaranteed-to-happen scenario.  (Imagine a cbft node
that's just overworked, slow and out-of-date.)

170 Eventually, a cbft instance / Manager / Planner is going to have a
new, latest & greatest plan that's different than its previous plan.
Next, responsibility switches to the Janitor.

180 Each Janitor running a cbft node knows its cbft-ID, and can focus
on the subset of the plan related to that cbft-ID.

190 A Janitor can then create or delete local PIndexes and
setup/teardown Feeds as needed to match its subset of the plan.

192 (feedback from Alk) Alk & cluster manager team have found, in
contrast to the current design thinking, that single orchestrator in a
cluster is better:

- easier to handle multiple versions, as latest fixes are easier to
  incorporate by always electing some node that has the latest code to
  be the master.

- single master orchestrator is easier to debug and reason about
  rather than concurrent, independent actors.

- for example, when adding new nodes, or new indexes, it's easier to
  sequence the changes for increased sanity.  And, easier to throttle
  the changes, perhaps into step by step batches, such as to avoid
  putting too much load on datasources.

- in short, try to favor design where "nodes are as dumb as possible".

200 Care must be taken so that any inflight queries are handled well
during these PIndex shutdowns and deletions.

300 If the planning does however turn out to be
non-derministic/non-functional, then we can still have a Plan B design
approach and use the Cfg system to help determine a single, leased
master Planner to do the planning and write results into the Cfg

310 But, even with a single, elected master Planner, it'll still take
some time for news of the new plan to get out to all the cbft
instances; so, everyone should be aware of the inherently concurrent
race conditions here.

320 For example, a Queryer might try to contact some instances that
haven't heard the news yet.

330 To alleviate that, one thought is perhaps a Queryer might try to
ask all its target cbft nodes "are you up to plan #12343?" before
doing a full query across those nodes?  Or, only do this PIndex query
if you are up to plan #12343, and I'm willing to wait/block for T
timeout seconds for you to get up to date.

400 With this design, the hope is the cbft instances are all
homogeneous, and during their independent Planning and Janitoring,
that they also don't have to talk to each other but can separately
work and arrive at the same answers.

-------------------------
What about downgrades?

Downgrades might happen when a user starts a rolling upgrade her
cluster of cbft nodes to a latest cbft version.  The new version of
cbft planners will start update Cfg entries with the latest
"ImplVersion" field value, which signals to older cbft nodes to stop
planning (since they might be using an older algorithm).

But, if the user changes her mind and wants to downgrade the cbft
nodes, those latest Cfg entries will remain incorrectly "prioritized",
where the remaining old-version cbft nodes won't do any re-planning or
overwriting.

To solve this, there might need to be a tool (lower priority) to
overwrite the ImplVersion's in the Cfg so that old cbft nodes will
again start participating in planning and Cfg updates.

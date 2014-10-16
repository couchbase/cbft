A file for ideas/notes...

---------------------------------------------
Requirements

Just gave every requirement a unique ID, where the numbers don't mean
anything (such as priority).

CS1 - Consistent queries during stable topology.

Clients should be able to ask, "I want query results where the
full-text-indexes have incorporated at least up to this set of
{vbucket to seq-num} pairings."

For example, the app does some mutations; then the app
does some full-text query.  The app will want the full-text
indexes to have incorporated the mutations.

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

OC1 - Optional looser "best effort" options (along a spectrum to
totally consistent) that's less expensive totally consistent CR1
implementation.

For example, perhaps the client can just ask consistency on just one
vbucket.

IA1 - Index aliases.

Level of indirection to help split data across multiple indexes, but
also not change your app all the time.  Example: query from 'sales'
index, but only want to search the most recent quarter.

MQ1 - Multi-index query in a single bucket.

MQ2 - Multi-index query across multiple buckets.

This is the ability to query multiple buckets in a single query, such
as "find any docs from the customer, employee, vendor buckets who have
an address or comment about 'dallas'".

Buckets: customer, employee, vendor

Indexes: customer_by-address (fields: addr, city, state, country),
         customer_by-comments,
         employee_by-address,
         vendor_by-address,
         vendor_by-star-rating

Query hits this subset of indexes:
       customer_by-address,
       customer_by-comments,
       employee_by-address,
       vendor_by-address
       (but not the vendor_by-star-rating)

Implementation sketch - although there might be many separate Indexes
for each bucket (and vbucket), the Queryer should be able to
scatter/gather across those Indexes and merge results together to meet
this requirement.

Implies that user is using the indexes the same way (no mismatched
types: string vs integers).

Note: foreign key case, for example, might lead to unexpected matches.

Note: this multi-bucket requirement might be incompatible with
couchbase bucket semantics?

In ES, an index alias can point to multiple indexes to support MQ1.

NI1 - Resilient to datasource node down for indexing.

If a data source (couchbase cluster server node) goes down, then the
subset of cbft that were indexing data from the down node pauses.  The
cbft system should try to reconnect.

E1 - The user should be able to see error conditions (e.g., yellow / red
color) on node down and other error condition.

Question: how to distinguish between I'm behind (as normal) versus
I'm REALLY behind on indexing.  Example: in 2i project, it can detect
that "I'm sooo far REALLY behind that I might as well start from zero
instead of trying catch up".

In ES, frustrating bouncing between yellow, green, red; ns-server
example, not enough CPU & timeouts leads to bounce-iness.

NQ1 - Querying still possible if datasource node goes down.

Querying of cbft should be able to continue.

PI1 - Ability to pause/resume indexing.

---------------------------------------------
Imaginary N1QL syntax...

  CREATE FULLTEXT INDEX XXX on Bucket (...optional params...);

  CREATE FULLTEXT INDEX customer_FTI on customers;

Especially, separation of indexes from bucket terminology.

The phrase "I want to do a full-text query on a bucket" isn't quite
right.  Instead, we're going for "you can do a full-text query on a
full-text index on a bucket".

---------------------------------------------
Proposed highlevel design concepts and "subparts"...

Inside a single cbft process...

- PIndex (an "Index Partition" that consumes StreamRequests)

- Streams (a channel of StreamRequest)

- Feed (hook up a data source & pushes requests
        into 1 or more StreamRequests)

- Manager (manages a set of PIndexes, StreamRequests, and Feeds)

- Queryer (scatter/gathers across relevant PIndexes)

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
variables.  A Manager has API to list, setup, teardown and pause
PIndexes, Streams and Feeds.  When a new logical "full text index" is
created for a bucket, for example, the Manager will assign
partitions/vbuckets to PIndexes and hook up all the relevant Stream
channels between Feeds and PIndexes.  A Manager, then, decides the
1-to-1, 1-to-N, N-to-1, N-to-M fan-in-out assignment of partitions to
PIndexes.  A snapshot of this mapping might need to be stored durably
(gometa?).  A part of this mapping might actually be a list of known
or expected cbft nodes.

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

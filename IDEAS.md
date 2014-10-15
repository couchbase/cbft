A file for ideas/notes...

---------------------------------------------
Requirements

Just gave every requirement a unique ID, where the numbers don't mean
anything (such as priority).

CS1 - Consistent queries during stable topology.

Clients should be able to ask, "I want query results where the
full-text-indexes have incorporated at least up to this set of
{vbucket to seq-num} pairings."

CR1 - Consistent queries under datasource rebalance.

Full-text queries should be consistent even as data source nodes are
added and removed in a clean rebalance.

CR2 - Consistent queries under cbft topology change.

Full-text queries should be consistent even as cbft nodes are added
and removed in a clean takeover fashion.

MQ1 - Multi-bucket query.

This is the ability to query multiple buckets in a single query, such
as "find any docs from the customer, employee, vendor buckets who have
an address or comment about 'dallas'".

Implementation sketch - although there might be many separate Indexes
for each bucket (and vbucket), the Queryer should be able to
scatter/gather across those Indexes and merge results together to meet
this requirement.

NI1 - Resilient to datasource node down for indexing.

If a data source (couchbase cluster server node) goes down,
then indexing just pauses.  The cbft system should try to reconnect.

NQ1 - Querying still possible if datasource node goes down.

Querying of cbft should be able to continue.

---------------------------------------------
Proposed highlevel design concepts and "subparts"...

Inside a single cbft process...

- Index (consumes StreamRequests)

- StreamRequests (a channel of StreamRequest)

- Feed (hook up a data source & pushes requests
        into 1 or more StreamRequests)

- Manager (manages a set of Indexes, StreamRequests, and Feeds)

- Queryer (scatter/gathers across relevant Indexes)

An Index consumes a StreamRequests and maintains a single bleve index.
That bleve index *might* just be a partition of a larger index, but an
Index doesn't really know.  Higher levels of the system (Manager) have
the partition to index mapping.  An Index, in contrast, just knows
about a single StreamRequests as its input, and an Index doesn't know
about couchbase, buckets, vbuckets, or DCP/TAP.

A StreamRequests is a channel of StreamRequest objects, which might
represent mutations (document updated, deleted), or "administrative"
requests (like please-shutdown, compact, delete-index, snapshot,
negotitate rollback/restart, negotiate a checkpoint, etc).

A Feed is an interface that will have different implementations
(TAPFeed, DCPFeed, TestFeed, etc) that pumps requests into a
StreamRequests.  A Feed is responsible for connecting (and
reconnecting) to a data source.  A TestFeed, for example, can send a
whole series of interesting requests down a StreamRequests for testing
difficult scenarios.  During reconnections to wobbly data sources,
it's the responsibility of the different Feed implementations to
implement backoff strategies.

A Manager is a collection of Index'es, Streams, and Feeds.  It has the
mapping of buckets and vbuckets/partitions to Indexes/Streams/Feeds.
A Manager singleton will be that single "global" object in a cbft
process rather than having many global variables.  A Manager has API
to list, setup, teardown and pause Indexes, Streams and Feeds.  When a
new logical "full text index" is created for a bucket, for example,
the Manager will assign partitions/vbuckets to Indexes and hook up all
the relevant StreamRequests channels between Feeds and Indexes.  A
Manager, then, decides the 1-to-1, 1-to-N, N-to-1, N-to-M fan-in-out
assignment of partitions to Indexes.  A snapshot of this mapping might
need to be stored durably (gometa?).  A part of this mapping might
actually be a list of known or expected cbft nodes.

A Queryer can query against one or more Indexes (perhaps even one day
to remote Indexes by communicating with remote Queryers).  Initially,
perhaps it can only do just a single Index, but the API should be
multi-Index ready.

A HTTP/REST (and next-generation protocol / green-stack) networking
layer sits on top of all of it for index mgmt and querying endpoints
that clients can access.  During a query, this networking layer
accesses a Manager for the relevant mapping and invokes the Queryer
with the Indexes that need to be accesses.  This networking layer will
provide the necessary AUTH checks.

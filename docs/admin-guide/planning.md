# Planning your deployment

This document provides considerations on how a cbft deployment might
be planned, across different environments and application lifecycle
stages:

- from development
- to staging/test
- and to production

## Development

Developers are expected to likely run cbft in "single node" fashion or
in non-clustered _simple_ mode, directly on their personal development
workstations or laptop machines.

Developers can write applications which query their single, local cbft
node using their favorite web-stack technologies and REST client
software to access their cbft node.

Similar to how developers would have separate scripts to initialize a
database with tables or buckets or schemas -- the so-called "DDL"
(data definition language) instructions of "CREATE TABLE..." / "CREATE
INDEX...", it is expected that developers would also code up separate
scripts to create cbft index definitions.

These index definition scripts would then often be checked into the
application's source code control systems (e.g., like svn or git or
mercurial or equivalent), for repeatability and for sharability with
colleagues.

cbft is designed so that index defintions which developers define and
create on their local, personal development workstations or laptops
can be deployed to production clusters without any changes to
application code that depends on those indexes.

### Staging and test

Testing engineers would likely use the index definition scripts
provided by application developers in order to setup and configure
their testing and staging cbft clusters.

The testing/staging cbft clusters would also be a good place to
reconfirm any sizing and performance assumptions.  For example,
perhaps an updated index definition utilizes new features of the
```bleve``` full-text engine, but those new features require more
resources (cpu, memory, storage) and these changes would ideally need
to be accounted for and reconfirmed before "going live" with
production changes.

### Production

Deployment of index definitions or updated index definitions to
production clusters would ideally follow the same steps that were
tested in testing/staging cbft clusters.

One useful technique would be to utilize cbft's index alias feature to
allow for updated indexes to be be built up in the background without
affecting existing application queries.  Applications would still
continue to query, by using index aliases, to previous index
definitions.  When a new index definition is finally built up enough
and ready for querying, an administrator would then redefine their
index aliases to point at the new, target index definition.

# Additional reading

Tradeoffs and considerations to [increasing index
performance](../dev-guide/performance)

---

Copyright (c) 2015 Couchbase, Inc.

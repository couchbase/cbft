# Diagnosing issues

cbft includes several REST API endpoints to programatically gather
stats, counters and health information as JSON responses.

## REST /api/diag

A key REST API endpoint, however, is ```/api/diag```, which will
gather as much diagnosis information as possible about a cbft node to
be sent in a single JSON response.

For example, for a three node cluster, you could capture the
```/api/diag``` output of each node with something like:

    curl http://cbft-01:8094/api/diag > cbft-01.json
    curl http://cbft-02:8094/api/diag > cbft-02.json
    curl http://cbft-03:8094/api/diag > cbft-03.json

The ```/api/diag``` response can be a quite large JSON object (100's
of KB and often much more).

The motivation with ```/api/diag``` is to simplify working with the
Couchbase community, forums, technical support and other engineers by
making data capture from each cbft node easy in one shot.

TBD - explaining the different sections of the /api/diag JSON.

## REST /debug/pprof

cbft supports the standard "pprof / expvars" diagnostics of golang
systems, allowing users to retrieve details on goroutines, threads,
heap memory usage and more.

---

Copyright (c) 2015 Couchbase, Inc.

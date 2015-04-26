# Diagnosing issues

cbft includes several REST API endpoints to programatically gather
stats, counters and health information as JSON responses.

A key REST API endpoint, however, is ```/api/diag```, which will
gather as much diagnosis information as possible about a cbft node to
be sent in a single JSON response.

For example, for a three node cluster, you could capture the
```/api/diag``` output of each node with something like:

    curl http://cbft-01:8095/api/diag > cbft-01.json
    curl http://cbft-02:8095/api/diag > cbft-02.json
    curl http://cbft-03:8095/api/diag > cbft-03.json

The ```/api/diag``` response can be a quite large JSON object (100's
of KB and often much more).

The motivation with ```/api/diag``` is to simplify working with the
Couchbase community, forums, technical support and other engineers by
making data capture from each cbft node easy in one shot.

tbd - explaining the different sections of the /api/diag JSON.

---

Copyright (c) 2015 Couchbase, Inc.

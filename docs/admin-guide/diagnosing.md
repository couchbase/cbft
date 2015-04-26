# Diagnosing issues

cbft includes several REST API endpoints to programatically gather
stats, counters and health information as JSON responses.

A key REST API endpoint, however, is the ```/api/diag```, which
attempts to gather as much information as possible in a single
request.  For example:

    curl http://cbft-01:8095/api/diag > cbft-01.json
    curl http://cbft-02:8095/api/diag > cbft-02.json
    curl http://cbft-03:8095/api/diag > cbft-03.json

Although these the ```/api/diag``` output may seem to be large, the
motivation with ```/api/diag``` is to simplify diagnosis of issues,
and with working with the Couchbase community, forums, technical
support and other engineers by making data capture easy in one shot.

---

Copyright (c) 2015 Couchbase, Inc.

# Monitoring cbft

cbft provides the following monitoring features:

## Logs

The web admin UI ```Logs``` screen shows the most recent 1000 log
messages for the cbft node.  It is a convenience feature so that an
administrator does not have to log into the machine to see recent
logs.

Recommended practice: an administrator should consider capturing
cbft's stdout/stderr output to rotated files or to a centralized log
service.

## Node monitoring

The web admin UI of cbft provides a ```Monitor``` screen that shows
node-related memory and GC (garbage collection utlization).

## Node index monitoring

The web admin UI provides a dropdown on the ```Monitor``` screen to
control graphs that can allow an administrator to focus in on the
index partition performance of the current cbft node.

Index partition specific graphs include:

- Updates (count of updates)
- Deletes (count of deletes)
- Analysis/Index Time
- Searches (count of searches)
- Search Time

The web admin UI also includes more numeric, per index monitoring
statistics.  Using a web browser...

- Navigate to the ```Indexes``` screen.

- Click on the index name link of the index you're interested in.

- Click on the Monitor sub-tab to see per-index specific stats and
  counters.

The displayed stats and counters are only for the current cbft node.

The ```Refresh``` button will request the latest information from the
current cbft node.

Selecting the ```Show all stats``` checkbox will display even more
stats and counters.

By default, aggregated stats across index partitions for the current
cbft node are shown.

Selecting the ```Show details and timings for each index partition```
checkbox will display non-aggregated details from every index
partition from the current cbft node.

## Memory

tbd

### GC pauses

tbd

## CPU

tbd

## Storage

tbd

## Networking

tbd

## Performance

tbd

---

Copyright (c) 2015 Couchbase, Inc.


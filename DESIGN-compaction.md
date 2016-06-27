cbft compaction design & issues

This writeup focuses on the compaction design of cbft, especially of
Full Text pindexes powered by the bleve full-text indexing engine.

-------------------------------------------------
= TODO's

- need to add a REST api and/or configurability to cbft to allow
  applications (or testing systems) to be able to explicitly control
  compactions.  Until such explicit control appears, a workaround for
  testing is to use the CBFT_ENV_OPTIONS environment variable.  For
  example, for forestdb, please see the forestdbCompactorSleepDuration
  setting below.

-------------------------------------------------
= Key considerations

Compaction of cbft's Full Text pindexes depends on the KV storage
engine (i.e., forestdb, boltdb, mossStore) that the user has
configured as part of their index definition.

Some storage engines such as boltdb, for example, don't have a concept
of compaction, but perform in-place updates of a file.  In such
storage engines, it's possible that files might never really shrink
even though large parts of the files are actually unused.

-------------------------------------------------
= forestdb storage engine

You can configure cbft / bleve to use forestdb as the index storage
engine by setting the "store" / "kvStoreName" property to "forestdb".
Your index definition JSON, for example, would look somewhat like...

    {
        "mapping": {
            ...
        },
        "store": {
            "kvStoreName": "forestdb",
            ...
        }
    }

The forestdb storage engine is used by cbft / bleve in an "automatic"
compaction mode, where cbft / bleve configures forestdb by default to
automatically check whether compaction should be performed on a
regular schedule (see: forestdbCompactorSleepDuration, by default 8
hours) and only when certain configured conditions are met (see:
forestdbCompactionThreshold and forestdbCompaciionMinimumFilesize).

The forestdb storage engine is configurable only on a process-wide
basis, via the CBFT_ENV_OPTIONS environment variable.

== CBFT_ENV_OPTIONS for forestdb

For the forestdb storage engine, the relevant CBFT_ENV_OPTIONS
include...

    forestdbCompactionBufferSizeMax (uint32)
      4MB by default

    forestdbCompactionThreshold (uint8)
      Compaction threshold in the unit of percentage (%). It can be calculated
      as '(stale data size)/(total file size)'. The compaction daemon triggers
      compaction if this threshold is satisfied.
      Compaction will not be performed when this value is set to zero or 100.

    forestdbCompaciionMinimumFilesize (uint64)
      The minimum filesize to perform compaction.

    forestdbCompactorSleepDuration (uint64)
      Duration that the compaction daemon task periodically wakes up, in the unit of
      second. This is a local config that can be configured per file.
      If the daemon compaction interval for a given file needs to be adjusted, then
      fdb_set_daemon_compaction_interval API can be used.

    forestdbNumCompactorThreads (int)
      Number of daemon compactor threads. It is set to 4 threads by default.

For example, you might launch couchbase-server with something like...

    CBFT_ENV_OPTIONS=forestdbCompactorSleepDuration=120,forestdbNumCompactorThreads=8 \
        /opt/couchbase/bin/couchbase-server ...

-------------------------------------------------
= mossStore storage engine

You can configure cbft / bleve to use the mossStore storage engine by
setting the "store" / "kvStoreName" property to "mossStore".  For
spock builds of cbft, "mossStore" is currently the default
kvStoreName, but if you wanted to be explicit, your index definition
JSON would look somewhat like...

    {
        "mapping": {
            ...
        },
        "store": {
            "kvStoreName": "mossStore",
            ...
        }
    }


The mossStore storage engine is configured by default in a "compact
all the time" mode.  Or, in moss parlance, "merge files all the time".
This behavior is controllable on a per-index basis by setting the
"store" / "mossStoreOptions" JSON sub-document in your index
definition.  For example...

    {
        "mapping": {
            ...
        },
        "store": {
            "kvStoreName": "mossStore",
            "mossStoreOptions": {
                "CompactionPercentage": 0,
            }
        }
    }

The "CompactionPercentage" is 0 by default, which effectively means
"compact or merge files all the time" whenever there's any mutation.

A "CompactionPercentage", for example, of 20, would mean that
mossStore should only compact/merge-files when the number of recently
persisted ops is greater than 20% of the number of records represented
by the previous compaction.

Let's say the compaction just finished running, and there are 100,000
records represented in the compacted mossStore file.

Next, more mutations and deletions (or "ops") are written by mossStore
to its file/disk in append-only fashion.  mossStore won't run another
compaction (or merging) until there are at least 20,000 of those
incoming, persisted ops due to the 20 (e.g., 20%) configuration.

-------------------------------------------------
= Testing ideas

== Not enough disk space

Compaction might require 2x the disk space to accommodate both the
original file, and the "result of compaction" file.

This situation should ideally be handled gracefully.

== Situations when the mutation rate is much, much faster than
compaction rate.

If compaction can't keep up, hopefully the system just devolves down
to a case of "not enough disk space" and/or backpressure from "not
enough memory" as memory resource grows limited.

== Huge mmap'ed files and OOM

If compaction is very slow (and can't keep up with incoming
mutations), then the files will continue to grow larger and larger,
meaning very large files can be mmap()'ed.  This operation might fail
(e.g., lack of memory, virtual addresss space. etc), but should
ideally be handled gracefully.

== Write amplification

mossStore's default configuration of "compaction all the time" is
simple, and avoids yet another knob or choice the user has to make.
But, it's degenerate for the case where a large index has been built,
and next a handful fo document muations trickle in.  With mossStore's
"compact all the time" approach, a huge file would be read and
rewritten out as a compaction run.

This can be a large hit to processing resources.

== Deleting many documents

After a large index has been built, one test idea would be to delete
many (or even all the documents).  After enough time has passed for
compaction(s) to run, used file space should be expected to be very
small.

== Bucket flush and deletion in the midst of compaction.

A similar case would be with handling bucket flush and bucket
deletion, even in the midst of a compaction.

== Rebalance and compaction

Similarly, rebalance (especially rebalance-out of a node) while
compaction is running can be an interesting case to test.

== Slow running queries and compaction

A slow running query might be referencing a file that the compactor
has finished compacting, and would normally have already deleted,
except it's still in-use by a slow-running reader or query operation.

If enough of those slow readers happen in the right (wrong?) way, then
large amounts of disk space and mmap() resources can be consumed.

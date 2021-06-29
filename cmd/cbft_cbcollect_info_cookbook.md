Some command-line cookbook notes and hints, which might be useful when
trying to diagnose cbcollect-info logs...

Once you're in ungzip'ed/untar'ed your cbcollect_info* directory...

    ln -s $GOPATH/src/github.com/couchbase/cbft/cmd/cbft_cbcollect_info_analyze

To dump the full /api/diag from a cbcollect-info directory...

    cat cbcollect_info*/fts_diag.json
    # OR...
    ./cbft_cbcollect_info_analyze extract /api/diag cbcollect_info*

For example, you can now pipe it to "jq ." (or "python -m json.tool") or other tools...

    cat cbcollect_info*/fts_diag.json | jq .
    # OR...
    ./cbft_cbcollect_info_analyze extract /api/diag cbcollect_info* | jq .

To see the "/api/cfg"...

    cat cbcollect_info*/fts_diag.json* | jq '.["/api/cfg"]'
    # OR...
    ./cbft_cbcollect_info_analyze extract /api/diag cbcollect_info* | jq '.["/api/cfg"]'

To see the "/api/stats"...

    cat cbcollect_info*/fts_diag.json | jq '.["/api/stats"]'
    # OR...
    ./cbft_cbcollect_info_analyze extract /api/diag cbcollect_info* | jq '.["/api/stats"]'

To see the bucketDataSourceStats from cbdatasource across all feeds...

    cat cbcollect_info*/fts_diag.json | jq '.["/api/stats"]?["feeds"]?[]?["bucketDataSourceStats"]?'
    # OR...
    ./cbft_cbcollect_info_analyze extract /api/diag cbcollect_info* | jq '.["/api/stats"]?["feeds"]?[]?["bucketDataSourceStats"]?'

To look for non-zero cbdatasource error counts...

    cat cbcollect_info*/fts_diag.json | jq '.["/api/stats"]?["feeds"]?[]?["bucketDataSourceStats"]?' | grep Err | grep -v " 0,"
    # OR...
    ./cbft_cbcollect_info_analyze extract /api/diag cbcollect_info* | jq '.["/api/stats"]?["feeds"]?[]?["bucketDataSourceStats"]?' | grep Err | grep -v " 0,"
    ...
    "TotWorkerReceiveErr": 1,
    "TotWorkerHandleRecvErr": 1,
    "TotUPRDataChangeStateErr": 1,
    "TotUPRSnapshotStateErr": 1,
    "TotWantCloseRequestedVBucketErr": 1,
    ...

To get the cbft node uuids...

    grep "\-uuid" cbcollect_info_*/*fts.log

To find the node that became the so-called "MCP" during rebalance...

    grep %%% cbc*/*.log | cut -f 1 -d \- | sort | uniq

To see what node was being removed during rebalance...

    grep nodesToRemove cbcollect_info_*/*fts.log

To see what metakv "planPIndexes" updates the cbfts might be making...

    grep metakv */*.log | grep fts | grep PUT | grep planPIndexes

Another, faster way to see the what pindex updates the cbft nodes are making through metakv from Aliaksey...

    grep PUT.*PIndexes */ns_server.http_access_internal.log | sort -k4

To fix a hash mismatch issue with planPIndexes Or
    to override a metakv "planPIndexes" with a default value.

    step1. curl -i http://<hostname:8091>/diag/eval -d 'metakv:set(<<"/fts/cbgt/cfg/curMetaKvPlanKey">>, <<"">>).'
     -u<uname>:<pwd>

    step2. curl -i http://<hostname:8091>/diag/eval -d 'metakv:set(<<"/fts/cbgt/cfg/planPIndexes">>, <<"{\"ImplVersion\":\"5.5.0\"}">>).' -u<uname>:<pwd>

To find the couchbase nodes that ns-server might know about...

    grep -h per_node_ets_tables */*.log | cut -f 1 -d , | sort | uniq

To see when rebalance was started by ns-server...

    grep -i ns_rebalancer */*.log | grep -i started
    ...
    cbcollect_info_ns_1@172.23.106.176_20160429-001333/diag.log:2016-04-28T17:07:18.198-07:00, ns_rebalancer:0:info:message(ns_1@172.23.106.139) - Started rebalancing bucket default
    ...

To find the num_bytes_used_ram by FTS...

    mortimint -emitParts=NAME cbcollect_info_* | grep num_bytes_used_ram

To see when rebalance REST API call went to ns-server...

    grep rebalance cbc*/ns_server.http_access.log | grep POST

To get the goroutine dump out of fts_diag.json in original formatting...

    cat fts_diag.json | jq -r '.["/debug/pprof/goroutine?debug=2"]'

    # The -r is the magic to get it back out of a json string,
    # removing quoting, and evaluate newlines again.

To get the heap pprof dump out of fts_diag.json in original formatting...

    cat fts_diag.json | jq -r '.["/debug/pprof/heap?debug=1"]'

As another approach, to have cbft process to log or dump goroutine info (to stdout/stderr or logs)...

    kill -s SIGUSR2 $CBFT_PID

To emit moss store histograms (pretty output) for the cbft node...

    curl http://<username>:<password>@<ip>:8094/api/stats | jq '.["pindexes"][]["bleveIndexStats"]["index"]["kv"]["store_histograms"] | split("\n")'
    cat fts_diag.json | jq '.["pindexes"][]["bleveIndexStats"]["index"]["kv"]["store_histograms"] | split("\n")'

To emit moss collection histograms (pretty output) for the cbft node...

    curl http://<username>:<password>@<ip>:8094/api/stats | jq '.["pindexes"][]["bleveIndexStats"]["index"]["kv"]["coll_histograms"] | split("\n")'
    cat fts_diag.json | jq '.["pindexes"][]["bleveIndexStats"]["index"]["kv"]["coll_histograms"] | split("\n")'

To see if FTS handled a DCP rollback...

    grep 'feed_dcp: rollback' cbc*/ns_server.fts.log

To see which pindex a document ID is in...

    curl -XPOST -u<username>:<password> http://<ip>:8094/api/index/default_index/pindexLookup -d '{"docId":"emp10000847"}' | jq .

To get all the source partition id's (e.g., vbucket id's) from across
all the pindexes, so that you can see which FTS node ended up with
which source partition (especially, when there's just a single FTS
index)...

    cat fts_diag.json | jq '.["/api/stats"]["pindexes"][]["partitions"] | keys | flatten[]'

When there's multiple FTS indexes, you'll have to specify a particular
pindex name in the above jq filter.

Couchbase Server: to find the active vbuckets from a KV stats.log...

    grep vb_ stats.log | grep active | grep -v vb_active | grep -v pcnt | cut -d : -f 1 | sort | uniq


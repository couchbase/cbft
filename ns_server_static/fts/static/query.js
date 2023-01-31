//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
import {confirmDialog, errorMessage} from "./util.js";
import {timer}   from "rxjs";
import { takeWhile } from 'rxjs/operators';
export default QueryCtrl;
export {queryMonitor};

var lastQueryIndex = null;
var lastQueryReq = null;
var lastQueryRes = null;

function PrepQueryRequest(scope) {
    let q = scope.query

    try {
        var obj = JSON.parse(scope.query);
        q = obj;
    } catch(e) {}

    let qr = {};

    if (typeof q == "object") {
        if (('query' in q) && (typeof q['query'] == "object")) {
            qr = q;
        } else {
            qr = {
                "explain": true,
                "fields": ["*"],
                "highlight": {},
                "query": q
            };
        }
    } else {
        qr = {
            "explain": true,
            "fields": ["*"],
            "highlight": {},
            "query": {
                "query": scope.query,
            },
        };
    }

    qr["size"] = scope.resultsPerPage;
    qr["from"] = (scope.page-1) * scope.resultsPerPage;

    return qr
}

QueryCtrl.$inject = ["$scope", "$http", "$routeParams", "$log", "$sce", "$location", "qwDialogService"];
function QueryCtrl($scope, $http, $routeParams, $log, $sce, $location, qwDialogService) {
    $scope.errorMessage = null;
    $scope.errorMessageFull = null;
    $scope.query = null;
    $scope.queryHelp = null;
    $scope.queryHelpSafe = null;

    $scope.page = 1;
    $scope.results = null;
    $scope.numPages = 0;
    $scope.maxPagesToShow = 5;
    $scope.resultsPerPage = 10;
    $scope.resultsSuccessPct = 100;
    $scope.timeout = 0;
    $scope.consistencyLevel = "";
    $scope.consistencyVectors = "{}";
    $scope.jsonQuery = "";

    $scope.hostPort = $location.host();
    if ($location.port()) {
        $scope.hostPort = $scope.hostPort + ":" + $location.port();
    }
    $scope.protocol = $location.protocol();

    $http.get("/api/managerMeta").
    then(function(response) {
        var data = response.data;
        $scope.meta = data;

        if (!$scope.indexDef) {
            $http.get('/api/index/' + $scope.indexName).then(function(response) {
                $scope.indexDef = response.data.indexDef;
                initQueryHelp();
            })
        } else {
            initQueryHelp();
        }

        function initQueryHelp() {
            var indexDefType = ($scope.indexDef && $scope.indexDef.type);

            $scope.queryHelp = $scope.meta.indexTypes[indexDefType].queryHelp;
            // this call to trustAsHtml is safe provided we trust
            // the registered pindex implementations
            $scope.queryHelpSafe = $sce.trustAsHtml($scope.queryHelp);
        }
    });

    function createQueryRequest() {
        var prepQueryRequestActual = $scope.prepQueryRequest;
        if (!prepQueryRequestActual) {
            prepQueryRequestActual = PrepQueryRequest;
        }

        var req = prepQueryRequestActual($scope) || {};

        var v = {};
        try {
            v = JSON.parse($scope.consistencyVectors || "{}");
        } catch(err) {
            $scope.errorMessage = "consistency vectors: " + err.message;
        } finally {
        }

        let timeout = parseInt($scope.timeout) || 0;
        if ($scope.consistencyLevel != "" || Object.keys(v).length > 0 || timeout != 0) {
            req.ctl = {}
            if ($scope.consistencyLevel != "") {
                req.ctl.consistency = {}
                req.ctl.consistency["level"] = $scope.consistencyLevel;
            }
            if (Object.keys(v).length > 0) {
                if (req.ctl.consistency == null) {
                    req.ctl.consistency = {}
                }
                req.ctl.consistency["vectors"] = v
            }
            if (timeout != 0) {
                req.ctl["timeout"] = timeout;
            }
        }

        return req;
    }

    $scope.queryChanged = function() {
        try {
            var j = JSON.stringify(createQueryRequest(), null, 2);
            $scope.jsonQuery = j;
        } finally {
        }
    };

    $scope.runQuery = function() {
        if (!$scope.query) {
            $scope.errorMessage = "please enter a query";
            return;
        }

        $location.search('q', $scope.query);
        $location.search('p', $scope.page);

        $scope.errorMessage = null;
        $scope.errorMessageFull = null;
        $scope.results = null;
        $scope.numPages = 0;
        $scope.resultsSuccessPct = 100;

        var req = createQueryRequest();
        $http.post('/api/index/' + $scope.indexName + '/query', req).
        then(function(response) {
            var data = response.data;
            lastQueryIndex = $scope.indexName;
            lastQueryReq = req;
            lastQueryRes = JSON.stringify(data);
            $scope.processResults(data);
            $scope.queryChanged()
        }, function(response) {
            var data = response.data;
            var code = response.code;
            $scope.errorMessageFull = data;
            if (data) {
                $scope.errorMessage = errorMessage(data, code);
            } else {
                $scope.errorMessage =
                    data || ("error" + (code || " accessing server"));
            }
        });
    };

    $scope.resultsAvailable = function() {
        return $scope.results !== null || $scope.errorMessage !== null;
    }

    $scope.runNewQuery = function() {
        $scope.page = 1
        $scope.runQuery()
    };

    $scope.roundTook = function(took) {
        if (!took || took <= 0) {
            return "";
        }
        if (took < 1000 * 1000) {
            return "<1ms";
        } else if (took < 1000 * 1000 * 1000) {
            return "" + Math.round(took / (1000*1000)) + "ms";
        } else {
            let roundMs = Math.round(took / (1000*1000));
            return "" + roundMs/1000 + "s";
        }
    };

  $scope.manualEscapeHtmlExceptHighlighting = function(orig) {
    // escape HTML tags
    let updated = orig.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;")
    // find escaped <mark> and </mark> and put them back
    updated = updated.replace(/&lt;mark&gt;/g, "<mark>").replace(/&lt;\/mark&gt;/g, "</mark>")
    return updated
  }

    $scope.setupPager = function(results) {
        if (!results.total_hits) {
            return;
        }

        $scope.numPages = Math.ceil(results.total_hits/$scope.resultsPerPage);
        $scope.validPages = [];
        for(var i = 1; i <= $scope.numPages; i++) {
            $scope.validPages.push(i);
        }

        // now see if we have too many pages
        if ($scope.validPages.length > $scope.maxPagesToShow) {
            var numPagesToRemove = $scope.validPages.length - $scope.maxPagesToShow;
            var frontPagesToRemove = 0
            var backPagesToRemove = 0;
            while (numPagesToRemove - frontPagesToRemove - backPagesToRemove > 0) {
                var numPagesBefore = $scope.page - 1 - frontPagesToRemove;
                var numPagesAfter =
                    $scope.validPages.length - $scope.page - backPagesToRemove;
                if (numPagesAfter > numPagesBefore) {
                    backPagesToRemove++;
                } else {
                    frontPagesToRemove++;
                }
            }

            // remove from the end first, to keep indexes simpler
            $scope.validPages.splice(-backPagesToRemove, backPagesToRemove);
            $scope.validPages.splice(0, frontPagesToRemove);
        }
        $scope.firstResult = (($scope.page-1) * $scope.resultsPerPage) + 1;
    };

    function getScopeAndCollection(docConfigMode, mapping) {
        if (docConfigMode.startsWith("scope.collection.")) {
            if (mapping.default_mapping.enabled) {
                return ["_default", "_default"];
            } else {
                for (let [key, value] of Object.entries(mapping.types)) {
                    if (value.enabled) {
                        let mappingName = key.split(".");
                        return [mappingName[0], mappingName[1]];
                    }
                }
            }
        }
        return ["_default", "_default"]
    }

    $scope.showDocument = function(hit) {
        try {
            let iDef = {};
            if ($scope.indexDef.type == "fulltext-index") {
                iDef = $scope.indexDef;
            } else if ($scope.indexDef.type == "fulltext-alias") {
                if (hit.index.length > 0) {
                    // hit.index is the pindex name of format .. <index_name>_<hash>_<hash>
                    var n = hit.index.substring(0, hit.index.lastIndexOf("_")).lastIndexOf("_");
                    let ix = hit.index.substring(0, n); // this is the index name
                    for (var indexName in $scope.indexDefs) {
                        if (indexName == ix) {
                            iDef = $scope.indexDefs[ix];
                            break;
                        }
                    }
                }
            }

            let scopeCollection =
                getScopeAndCollection(iDef.params.doc_config.mode, iDef.params.mapping);
            if (angular.isDefined(hit.fields) && angular.isDefined(hit.fields["_$c"])) {
                // in case of an index that's subscribed to multiple collections,
                // the collection that the document belongs to is available within
                // the fields section
                scopeCollection[1] = hit.fields["_$c"];
            }
            qwDialogService.getAndShowDocument(true, hit.id,
                iDef.sourceName, scopeCollection[0], scopeCollection[1], hit.id);
        } catch(e) {}
    };

    $scope.processResults = function(data) {
        $scope.results = data;
        $scope.setupPager($scope.results);

        if ($scope.results && $scope.results.status.failed > 0) {
            var successful = $scope.results.status.successful;
            var total = $scope.results.status.total;
            $scope.resultsSuccessPct = Math.round(((1.0 * successful) / total) * 10000) / 100.0;
            // set errorMessage to first error seen
            try {
                var errors = $scope.results.status.errors;
                $scope.errorMessage = errors[Object.keys(errors)[0]];
            } catch (e) {
            }
        }

        for(var i in $scope.results.hits) {
            var hit = $scope.results.hits[i];
            for(var ff in hit.fragments) {
                var fragments = hit.fragments[ff];
                var newFragments = [];
                for(var ffi in fragments) {
                    var fragment = fragments[ffi];
                    var saferFragment = $scope.manualEscapeHtmlExceptHighlighting(fragment);
                    newFragments.push(saferFragment);
                }
                hit.fragments[ff] = newFragments;
            }
            if (!hit.fragments) {
                hit.fragments = {};
            }
            for(var fv in hit.fields) {
                var fieldval = hit.fields[fv];
                if (hit.fragments[fv] === undefined) {
                    hit.fragments[fv] = [$scope.manualEscapeHtmlExceptHighlighting(""+fieldval)];
                }
            }
            if ($scope.decorateSearchHit) {
              $scope.decorateSearchHit(hit)
            }
        }
        if (data.took) {
            $scope.results.roundTook = $scope.roundTook(data.took);
        }
    };

    $scope.jumpToPage = function(pageNum, $event) {
        if ($event) {
            $event.preventDefault();
        }

        $scope.page = pageNum;
        $scope.runQuery();
    };

    if($location.search().p !== undefined) {
        var page = parseInt($location.search().p, 10);
        if (typeof page == 'number' && !isNaN(page) && isFinite(page) && page > 0 ){
            $scope.page = page;
        }
    }

    if($location.search().q !== undefined) {
        $scope.query = $location.search().q;
        $scope.runQuery();
    } else {
        if (!$scope.query &&
            lastQueryIndex == $scope.indexName &&
            lastQueryReq &&
            lastQueryRes) {
            $scope.query = lastQueryReq.q;
            $scope.errorMessage = null;
            $scope.errorMessageFull = null;
            $scope.results = null;
            $scope.numPages = 0;

            $scope.processResults(JSON.parse(lastQueryRes));

            $location.search('q', lastQueryReq.q);
        }
    }
}

function queryMonitor($scope, $uibModal, $http){
    function notHidden(el) {
        return !(el === null || (el.offsetParent === null));
    }
    $scope.startPoller = function(){
        $scope.querySupervisorMap = {};
        $scope.queryFilterVal = "5ms";
        let query_poller = timer(0,5000);
        query_poller.pipe(takeWhile(()=>{
            return notHidden(document.getElementById("monitorTab"));
        })).subscribe(() => {
            $scope.monitorQuerySupervisor();
        });
    };

    var monitoring = true;
    $scope.getToggleLabel = function(){
        return monitoring ? "Pause" : "Resume";
    }
    $scope.toggleMonitorFlag = function(){
        monitoring = monitoring ? false : true;
    }
    $scope.getMonitorFlag = function(){
        return monitoring;
    }

    function transformTime(a){
        if (a.includes('ms')){
            var ms = a.indexOf('ms');
            return parseFloat(a.substring(0,ms));
        }
        var h = a.indexOf('h');
        if (h != -1) {
            a = a.substr(0, h) + '#' + a.substr(h + 1);
        }
        var m = a.indexOf('m');
        if (m != -1) {
            a = a.substr(0, m) + '#' + a.substr(m + 1);
        }
        var s = a.indexOf('s');
        if (s != -1) {
            a = a.substr(0, s) + '#' + a.substr(s + 1);
        }
        var sl = a.split('#');
        return (h == -1 ? 0 : parseFloat(sl[0]) * 60 * 60)
            + (m == -1 ? 0 : (h == -1 ? parseFloat(sl[0]) * 60 : (parseFloat(sl[1]) * 60)))
            + (s == -1 ? 0 : (m == -1 ? (h == -1 ? parseFloat(sl[0]):parseFloat(sl[1])):parseFloat(sl[2])));
    }
    var sortFlag = {}
    $scope.ascendingOrder = function(arg){
        return (typeof(sortFlag) != "undefined") && sortFlag.col == arg && sortFlag.order;
    }
    function sortActiveQueries(){
        if (Object.keys(sortFlag).length === 0) {
            return
        }
        var clone = {};
        if(sortFlag.col.localeCompare("queryID") == 0){
            clone = Object.keys($scope.querySupervisorMap).sort(
                (a, b) => sortFlag.order ? a.localeCompare(b) : b.localeCompare(a)
            ).reduce((r, k) => (r[k] = $scope.querySupervisorMap[k], r), {});
        }
        if(sortFlag.col.localeCompare("duration") == 0){
            clone = Object.fromEntries(
                Object.entries($scope.querySupervisorMap).sort(([,a],[,b]) =>
                sortFlag.order ? transformTime(a["executionTime"]) - transformTime(b["executionTime"]) :
                transformTime(b["executionTime"]) - transformTime(a["executionTime"])
            ));
        }
        $scope.querySupervisorMap = JSON.parse(JSON.stringify(clone));
    }
    $scope.updateSortFlag = function(arg){
        if (Object.keys(sortFlag).length === 0){
            sortFlag = {
                col: arg,
                order: true,
            }
        } else {
            sortFlag = {
                col: arg,
                order: arg == sortFlag.col ? !sortFlag.order : true
            }
        }
        sortActiveQueries()
    }

    $scope.noSlowRunningQueries = function(){
        return typeof($scope.querySupervisorMap) != "undefined" &&
                Object.keys($scope.querySupervisorMap).length == 0;
    };

    $scope.filterQueries = function(arg){
        $scope.queryFilterVal = arg;
        $scope.monitorQuerySupervisor();
    }

    $scope.monitorQuerySupervisor = function(){
        $http.get('/api/query/index/'+$scope.indexName+'?longerThan='+$scope.queryFilterVal).
        then(function(response) {
            var respMap = response.data;
            var totalActives = respMap["filteredActiveQueries"]["queryCount"];
            if (!monitoring){
                return
            }
            $scope.querySupervisorMap = {};
            if (totalActives == 0) {
                return
            }
            var queryMap = respMap["filteredActiveQueries"]["queryMap"];
            for (let key of Object.keys(queryMap)){
                var uuid = key.split('-')[0];
                $scope.querySupervisorMap[key] = queryMap[key];
                $scope.querySupervisorMap[key]["hostPort"] = $scope.nodeDefsByUUID[uuid]["hostPort"];
            }
            sortActiveQueries()
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };
    $scope.killQuery = function(ID){
        confirmDialog(
            $scope, $uibModal,
            "Confirm Abort Query",
            "Are you sure you want to abort this query?",
            "Abort Query"
        ).then(function success() {
            var queryID = ID.split('-')[1];
            var body = {
                uuid: ID.split('-')[0],
            };
            $http.post('/api/query/'+queryID+'/cancel', body).
            then(function(){
                delete $scope.querySupervisorMap[ID];
            }, function(response){
                $scope.errorMessage = errorMessage(response.data, response.code);
                $scope.errorMessageFull = response.data;
            });
        });
    }
}

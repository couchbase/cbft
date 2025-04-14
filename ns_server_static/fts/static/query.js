//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
import {confirmDialog, errorMessage, obtainBucketScopeUndecoratedIndexName} from "./util.js";
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
        if ((('query' in q) && (typeof q['query'] == "object")) ||
            (('knn' in q) && (typeof q['knn'] == "object"))) {
            q['explain'] = true;
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

    var hasSize = "size" in qr && typeof(qr.size) === "number"  && qr.size > 0
    var hasLimit = "limit" in qr && typeof(qr.limit) === "number"  && qr.limit > 0
    var hasFrom = "from" in qr && typeof(qr.from) === "number"  && qr.from >= 0
    var hasOffset = "offset" in qr && typeof(qr.offset) === "number"  && qr.offset >= 0
    scope.size = -1
    scope.from = -1
    scope.offset = 0


    if (hasSize) {
        scope.size = qr["size"]
    } else if (!hasLimit) {
        qr["size"] = scope.resultsPerPage
    }

    if (hasFrom) {
        scope.from = qr["from"]
    } else if (hasOffset) {
        scope.offset = qr["offset"]
    } else {
        qr["from"] = (scope.page-1) * scope.resultsPerPage
    }

    if (!hasSize && !hasFrom && !hasLimit && !hasOffset) {
        scope.showPaginationOnRefresh = true
    } else {
        scope.showPaginationOnRefresh = false
        scope.showPagination = false
    }

    return qr
}

QueryCtrl.$inject = ["$scope", "$http", "$routeParams", "$log", "$sce", "$location", "qwDialogService"];
function QueryCtrl($scope, $http, $routeParams, $log, $sce, $location, qwDialogService) {
    $scope.errorMessage = null;
    $scope.errorMessageFull = null;
    $scope.editorError = null

    $scope.query = null;
    $scope.queryHelp = null;
    $scope.queryHelpSafe = null;

    $scope.page = 1;
    $scope.results = null;
    $scope.numPages = 0;
    $scope.maxPagesToShow = 5;
    $scope.resultsPerPage = 10;
    $scope.size = -1;
    $scope.from = -1;
    $scope.resultsSuccessPct = 100;
    $scope.timeout = 0;
    $scope.consistencyLevel = "";
    $scope.consistencyVectors = "{}";
    $scope.jsonQuery = "";
    $scope.queryString = null;
    $scope.queryEditorInitValue = "";
    $scope.queryEditMode = false;
    $scope.editor = null;
    $scope.queryTab = 1
    $scope.showPagination = true;
    $scope.showPaginationOnRefresh = true;
    $scope.queryEditMode = false;
    $scope.editor = null;

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
            if (!angular.isDefined(req.ctl)) {
                req.ctl = {}
            }
            if ($scope.consistencyLevel != "") {
                if (!angular.isDefined(req.ctl.consistency)) {
                    req.ctl.consistency = {}
                }
                req.ctl.consistency["level"] = $scope.consistencyLevel;
            }
            if (Object.keys(v).length > 0) {
                if (!angular.isDefined(req.ctl.consistency)) {
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
            var sr = createQueryRequest();
            var j = JSON.stringify(sr, null, 2);
            $scope.jsonQuery = j;
            $scope.queryString = $scope.getQueryStringFromRequest(sr);
        } finally {
        }
    };

    $scope.getQueryStringFromRequest = function(searchRequest) {
        var qsq = null;
        if (searchRequest &&
            searchRequest.query &&
            searchRequest.query.query &&
            typeof searchRequest.query.query === "string") {

            qsq = searchRequest.query.query;
        }
        return qsq;
    };

    $scope.parseQueryStringQuery = async function() {
        let response = await $http.post('/api/_dumpQuery', {
            query: $scope.queryString,
            mapping: $scope.indexMapping
        });
        return response.data;
    };

    $scope.parseQueryString = function() {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;
        $scope.parseQueryStringQuery().then(function(response) {
            var result = response.query;
            var queryPrev = $scope.query;
            $scope.query = result;
            var req = createQueryRequest();
            if (req && req.query && req.query.query) {
                req.query.query = result;
            }
            $scope.query = queryPrev;
            $scope.queryEditorInitValue = JSON.stringify(req, null, 2);
            $scope.queryEditMode = true;
        }).catch(function(errorResponse) {
            $scope.errorMessage = errorMessage(errorResponse.data, errorResponse.status);
            $scope.errorMessageFull = errorResponse.data;
        });
    }

    $scope.runQuery = function() {
        if (!$scope.query) {
            $scope.errorMessage = "please enter a query";
            return;
        }

        if ($scope.showPaginationOnRefresh) {
            $scope.showPagination = true
        } else {
            $scope.showPagination = false
        }
        $location.search('q', $scope.query);
        $location.search('p', $scope.page);

        $scope.errorMessage = null;
        $scope.errorMessageFull = null;
        $scope.results = null;
        $scope.numPages = 0;
        $scope.resultsSuccessPct = 100;

        var req = createQueryRequest();

        let reqUrl = "/api/index/" + $scope.indexName + "/query";
        if ($scope.isScopedIndexName &&
            angular.isDefined($scope.indexBucketName) &&
            angular.isDefined($scope.indexScopeName) &&
            angular.isDefined($scope.undecoratedIndexName)) {
            reqUrl = "/api/bucket/" + $scope.indexBucketName +
                "/scope/" + $scope.indexScopeName +
                "/index/" + $scope.undecoratedIndexName + "/query";
        }

        $http.post(reqUrl, req).
        then(function(response) {
            var data = response.data;
            lastQueryIndex = $scope.indexName;
            lastQueryReq = req;
            lastQueryRes = JSON.stringify(data);
            $scope.processResults(data);
            $scope.queryChanged()
        }, function(response) {
            var data = response.data;
            var code = response.status;
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
        if (/<mark>.*<\/mark>/.test(orig)) {
            // If <mark> ...</mark> tags are present, return the original input as-is
            // as the string already contains escaped HTML characters
            return orig;
        }
        // escape HTML tags
        let updated = orig.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;")
        // find escaped <mark> and </mark> and put them back
        return updated
    }

    $scope.setupPager = function(results) {
        if (!results.total_hits) {
            return;
        }

        var size
        if ($scope.size == -1) {
            size = $scope.resultsPerPage
        } else {
            size = $scope.size
        }

        if ($scope.from != -1) {
            $scope.page = 1
        }

        $scope.numPages = Math.ceil(results.total_hits/size);

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

        if ($scope.from == -1) {
            $scope.firstResult = (($scope.page-1) * size) + 1 + $scope.offset;
        } else {
            $scope.firstResult = $scope.from + 1 + $scope.offset;
        }
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

        if ($scope.results.facets) {
            $scope.facets = []
            for (const [key, value] of Object.entries($scope.results.facets)) {
                $scope.facets.push(key)
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
                    // Check if fieldval is an array
                    if (Array.isArray(fieldval)) {
                        // Join array elements with a comma and space, and then escape HTML
                        hit.fragments[fv] = [$scope.manualEscapeHtmlExceptHighlighting(fieldval.join(', '))];
                    } else {
                        // Handle single string case
                        hit.fragments[fv] = [$scope.manualEscapeHtmlExceptHighlighting(""+fieldval)];
                    }
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

    $scope.scopedIndexBucketName = function(name) {
        return obtainBucketScopeUndecoratedIndexName(name)[0];
    };

    $scope.scopedIndexScopeName = function(name) {
        return obtainBucketScopeUndecoratedIndexName(name)[1];
    }

    $scope.scopedIndexUndecoratedName = function(name) {
        return obtainBucketScopeUndecoratedIndexName(name)[2];
    };

    $scope.aceLoaded = function(editor) {
        editor.renderer.setPrintMarginColumn(false);
        editor.setSelectionStyle("json");
        editor.getSession().on("changeAnnotation", function() {
            var annot_list = editor.getSession().getAnnotations();
            if (annot_list && annot_list.length) for (var i=0; i < annot_list.length; i++)
              if (annot_list[i].type == "error") {
                $scope.editorError = "Error on row: " + annot_list[i].row + ": " + annot_list[i].text;
                return;
              }
            if (editor) {
                $scope.editorError = null;
            }
          });
        if (/^((?!chrome).)*safari/i.test(navigator.userAgent))
          editor.renderer.scrollBarV.width = 20; // fix for missing scrollbars in Safari
        editor.setValue($scope.queryEditorInitValue, -1) // moves cursor to the start
        $scope.editor = editor;
    }

    $scope.editQuery = function(){
        $scope.queryEditorInitValue = $scope.jsonQuery;
        $scope.queryEditMode = true;
    }

    $scope.executeQuery = function(){
        var editorValue = $scope.editor.getValue();
        let qr = JSON.parse(editorValue || "{}");
        $scope.query = editorValue;
        $scope.timeout = qr.ctl && qr.ctl.timeout ? qr.ctl.timeout : 0;
        $scope.consistencyLevel = qr.ctl && qr.ctl.consistency && qr.ctl.consistency.level ? qr.ctl.consistency.level : "";
        $scope.consistencyVectors = qr.ctl && qr.ctl.consistency && qr.ctl.consistency.vectors ? JSON.stringify(qr.ctl.consistency.vectors) : "{}";
        var queryReq = createQueryRequest()
        var j = JSON.stringify(queryReq, null, 2);
        $scope.jsonQuery = j;
        $scope.queryString = $scope.getQueryStringFromRequest(queryReq)
        $scope.runNewQuery($scope.query);
        $scope.queryEditMode = false;
    }

    $scope.cancelQuery = function(){
        $scope.queryEditMode = false;
    }

    $scope.setQueryTab = function(newTab) {
        $scope.queryTab = newTab;
    }

    $scope.isQueryTab = function(tabNum) {
        return $scope.queryTab === tabNum
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
            $scope.errorMessage = errorMessage(response.data, response.status);
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
                $scope.errorMessage = errorMessage(response.data, response.status);
                $scope.errorMessageFull = response.data;
            });
        });
    }
}

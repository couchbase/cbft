//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

var lastQueryIndex = null;
var lastQueryReq = null;
var lastQueryRes = null;

function PrepQueryRequest(scope) {
    qr = {
        "explain": true,
        "fields": ["*"],
        "highlight": {},
        "query": {
            "query": scope.query,
        },
    };
    if (scope.resultsPerPage != 10) {
      qr["size"] = scope.resultsPerPage;
    }
    from = (scope.page-1) * scope.resultsPerPage;
    if (from != 0) {
      qr["from"] = from
    }
    return qr
}

function QueryCtrl($scope, $http, $routeParams, $log, $sce, $location) {
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

        timeout = parseInt($scope.timeout) || 0;
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
            roundMs = Math.round(took / (1000*1000));
            return "" + roundMs/1000 + "s";
        }
    };

  $scope.manualEscapeHtmlExceptHighlighting = function(orig) {
    // escape HTML tags
    updated = orig.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;")
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

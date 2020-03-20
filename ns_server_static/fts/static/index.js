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

var indexStatsPrevs = {};
var indexStatsAggsPrevs = {};

var indexStatsLabels = {
    "pindexes": "index partition", "feeds": "datasource"
}

function IndexesCtrl($scope, $http, $routeParams, $log, $sce, $location) {
    $scope.data = null;
    $scope.indexNames = [];
    $scope.indexNamesReady = false;
    $scope.errorMessage = null;
    $scope.errorMessageFull = null;

    $scope.refreshIndexNames = function() {
        $http.get('/api/index').then(function(response) {
            var data = response.data;

            $scope.data = data;

            var indexNames = [];
            if (data.indexDefs) {
                $scope.indexDefs = data.indexDefs.indexDefs;
                for (var indexName in data.indexDefs.indexDefs) {
                    indexNames.push(indexName);

                    var indexDef = data.indexDefs.indexDefs[indexName];
                    if (indexDef) {
                        indexDef.paramsObj =
                            JSON.parse(JSON.stringify(indexDef.params));
                    }
                }
            }
            indexNames.sort();

            $scope.indexNames = indexNames;
            $scope.indexNamesReady = true;
        }, function(response) {
            var data = response.data;

            $scope.errorMessage = errorMessage(data, response.code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.deleteIndex = function(name) {
        if (!confirm("Are you sure you want to permanenty delete the index '"
                     + name + "'?")) {
            return;
        }

        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.delete('/api/index/' + name).then(function(response) {
            $scope.refreshIndexNames();
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };

    $scope.editIndex = function(name) {
        $location.path('/indexes/' + name + '/_edit');
    }

    $scope.cloneIndex = function(name) {
        $location.path('/indexes/' + name + '/_clone');
    }

    $scope.refreshIndexNames();
}

function IndexCtrl($scope, $http, $route, $routeParams, $location, $log, $sce, $uibModal) {

    $scope.errorMessage = null;
    $scope.errorMessageFull = null;

    $scope.nodeDefsByUUID = null;
    $scope.nodeDefsByAddr = null;
    $scope.nodeAddrsArr = null;

    $scope.indexName = $routeParams.indexName;
    $scope.indexDocCount = 0;
    $scope.indexDefStr = "";
    $scope.indexParamsStr = "";
    $scope.indexStats = null;
    $scope.planPIndexes = null;
    $scope.planPIndexesStr = ""

    $scope.statsRefresh = null;
    $scope.warnings = null;

    $scope.tab = $routeParams.tabName;
    if ($scope.tab === undefined || $scope.tab === "") {
        $scope.tab = "summary";
    }
    $scope.tabPath = '/static/partials/index/tab-' + $scope.tab + '.html';

    $scope.hostPort = $location.host();
    if ($location.port()) {
        $scope.hostPort = $scope.hostPort + ":" + $location.port();
    }

    $scope.meta = null;
    $http.get('/api/managerMeta').
    then(function(response) {
        var data = response.data;
        var meta = $scope.meta = data;

        $http.get('/api/cfg').
        then(function(response) {
            var data = response.data;
            $scope.nodeDefsByUUID = {}
            $scope.nodeDefsByAddr = {}
            $scope.nodeAddrsArr = []
            for (var k in data.nodeDefsWanted.nodeDefs) {
                var nodeDef = data.nodeDefsWanted.nodeDefs[k]
                $scope.nodeDefsByUUID[nodeDef.uuid] = nodeDef
                $scope.nodeDefsByAddr[nodeDef.hostPort] = nodeDef
                $scope.nodeAddrsArr.push(nodeDef.hostPort);
            }
            $scope.nodeAddrsArr.sort();
            $scope.loadIndexDetails()
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });

        $scope.loadIndexDetails = function() {
            $scope.errorMessage = null;
            $scope.errorMessageFull = null;

            $http.get('/api/index/' + $scope.indexName).
            then(function(response) {
                var data = response.data;

                $scope.indexDef = data.indexDef
                $scope.indexDefStr =
                    JSON.stringify(data.indexDef, undefined, 2);
                $scope.indexParamsStr =
                    JSON.stringify(data.indexDef.params, undefined, 2);
                $scope.sourceParamsStr =
                    JSON.stringify(data.indexDef.sourceParams, undefined, 2);
                $scope.planPIndexesStr =
                    JSON.stringify(data.planPIndexes, undefined, 2);
                $scope.planPIndexes = data.planPIndexes;
                if ($scope.planPIndexes) {
                    $scope.planPIndexes.sort(
                        function(x, y) {
                            if (x.name > y.name) { return 1; }
                            if (y.name > x.name) { return -1; }
                            return 0;
                        }
                    );
                }
                for (var k in $scope.planPIndexes) {
                    var planPIndex = $scope.planPIndexes[k];
                    planPIndex.sourcePartitionsArr =
                        planPIndex.sourcePartitions.split(",").sort(
                            function(x, y) { return parseInt(x) - parseInt(y); }
                        );
                    planPIndex.sourcePartitionsStr =
                        collapseNeighbors(planPIndex.sourcePartitionsArr).join(", ");
                }
                $scope.warnings = data.warnings;

                $scope.indexCanCount =
                    meta &&
                    meta.indexTypes &&
                    meta.indexTypes[data.indexDef.type] &&
                    meta.indexTypes[data.indexDef.type].canCount;

                $scope.indexCanQuery =
                    meta &&
                    meta.indexTypes &&
                    meta.indexTypes[data.indexDef.type] &&
                    meta.indexTypes[data.indexDef.type].canQuery;

                $scope.indexCanWrite =
                    !data.indexDef ||
                    !data.indexDef.planParams ||
                    !data.indexDef.planParams.nodePlanParams ||
                    !data.indexDef.planParams.nodePlanParams[''] ||
                    !data.indexDef.planParams.nodePlanParams[''][''] ||
                    data.indexDef.planParams.nodePlanParams[''][''].canWrite;

                $scope.indexCanRead =
                    !data.indexDef ||
                    !data.indexDef.planParams ||
                    !data.indexDef.planParams.nodePlanParams ||
                    !data.indexDef.planParams.nodePlanParams[''] ||
                    !data.indexDef.planParams.nodePlanParams[''][''] ||
                    data.indexDef.planParams.nodePlanParams[''][''].canRead;

                $scope.indexPlanFrozen =
                    data.indexDef &&
                    data.indexDef.planParams &&
                    data.indexDef.planParams.planFrozen;

                var indexUI =
                    (meta &&
                     meta.indexTypes &&
                     meta.indexTypes[data.indexDef.type] &&
                     meta.indexTypes[data.indexDef.type].ui);
                if (indexUI &&
                    indexUI.controllerInitName &&
                    typeof(window[indexUI.controllerInitName]) == "function") {
                    window[indexUI.controllerInitName](
                        "view", data.indexDef.params, indexUI,
                        $scope, $http, $route, $routeParams,
                        $location, $log, $sce, $uibModal);
                }
            }, function(response) {
                $scope.errorMessage = errorMessage(response.data, response.code);
                $scope.errorMessageFull = response.data;
            });
        };
    });

    $scope.loadIndexDocCount = function() {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/index/' + $scope.indexName + '/count').
        then(function(response) {
            $scope.indexDocCount = response.data.count;
        }, function(response) {
            $scope.indexDocCount = "..."
        });
    };

    $scope.loadIndexStats = function() {
        $scope.statsRefresh = "refreshing...";
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/stats/index/' + $scope.indexName).
        then(function(response) {
            var data = response.data;

            $scope.statsRefresh = null;
            $scope.indexStats = data;

            var indexStatsPrev = indexStatsPrevs[$scope.indexName];
            var indexStatsAggsPrev = indexStatsAggsPrevs[$scope.indexName];

            var errors = [];
            var stats = [];
            var aggs = {};

            var kinds = ["pindexes", "feeds"];
            for (var a in kinds) {
                var aa = kinds[a];

                // The k is pindexName / feedName.
                for (var k in data[aa]) {
                    var kk = data[aa][k];
                    // The j is category of stats, like basic,
                    // pindexStoreStats, bucketDataSourceStats, destStats, etc.
                    for (var j in kk) {
                        if (j == "partitions") { // Skip partition seq's/uuid's.
                            continue
                        }

                        var jj = data[aa][k][j];
                        errors = errors.concat(jj.Errors || []);

                        for (var s in jj) {
                            var ss = jj[s];
                            if (ss instanceof Array) {
                                continue;
                            }
                            if (typeof(ss) == "number") {
                                ss = jj[s] = {
                                    count: ss,
                                    advanced: j != "basic"
                                };
                            }
                            ss.prev = ss;
                            if (indexStatsPrev) {
                                ss.prev =
                                    ((indexStatsPrev[aa][k] || {})[j] || {})[s];
                            }
                            ss.sourceName = k;
                            ss.statKind = indexStatsLabels[aa];
                            ss.statName = s;
                            ss.label = s.replace(/^Timer/, "");

                            stats.push(ss);

                            var agg = aggs[s] = aggs[s] || {
                                advanced: ss.advanced,
                                count: 0,
                                label: ss.label
                            };
                            agg.count = agg.count + (ss.count || 0);
                            if (indexStatsAggsPrev) {
                                agg.prev = indexStatsAggsPrev[s];
                            }
                        }
                    }
                }
            }

            indexStatsPrevs[$scope.indexName] = data;
            indexStatsAggsPrevs[$scope.indexName] = aggs;

            stats.sort(compareStats);
            errors.sort(compareTime);

            $scope.indexErrors = errors;
            $scope.indexStatsFlat = stats;
            $scope.indexStatsAgg = aggs;
        }, function(response) {
            $scope.statsRefresh = "error";
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };

    // tab specific loading
    if ($scope.tab === "summary") {
        $scope.loadIndexDocCount();
    }
    if ($scope.tab === "monitor") {
        $scope.loadIndexStats();
    }

    $scope.indexDocument = function(id, body) {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.put('/api/index/' + $scope.indexName + "/doc/" + id, body).
        then(function(response) {
            $scope.successMessage = "Indexed Document: " + id;
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };

    $scope.deleteDocument = function(id) {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.delete('/api/index/' + $scope.indexName + "/" + id).
        then(function(response) {
            $scope.successMessage = "Deleted Document: " + id;
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };

    $scope.indexControl = function(indexName, what, op) {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        if (!confirm("Are you sure you want to change this setting?")) {
            return;
        }

        $http.post('/api/index/' + indexName + "/" +  what + "Control/" + op).
        then(function(response) {
            $scope.loadIndexDetails();
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };
}

function IndexNewCtrl($scope, $http, $route, $routeParams, $location, $log, $sce, $uibModal) {
    $scope.advancedFields = {
        "store": true
    };

    $scope.errorFields = {};
    $scope.errorMessage = null;
    $scope.errorMessageFull = null;

    $scope.newIndexName = "";
    $scope.newIndexType = $routeParams.indexType || "";
    $scope.newIndexParams = {};
    $scope.newSourceType = $routeParams.sourceType || "";
    $scope.newSourceName = "";
    $scope.newSourceUUID = "";
    $scope.newSourceParams = {};
    $scope.newPlanParams = "";
    $scope.prevIndexUUID = "";
    $scope.paramNumLines = {};

    $scope.mapping = null;

    $scope.isEdit = $location.path().match(/_edit$/);
    $scope.isClone = $location.path().match(/_clone$/);

    origIndexName = $routeParams.indexName;

    $http.get('/api/managerMeta').
    then(function(response) {
        var data = response.data;
        var meta = $scope.meta = data;

        $scope.newPlanParams =
            JSON.stringify(data.startSamples["planParams"], undefined, 2);
        $scope.paramNumLines["planParams"] =
            $scope.newPlanParams.split("\n").length + 1;

        var sourceTypesArr = []
        for (var k in data.sourceTypes) {
            sourceTypesArr.push(data.sourceTypes[k]);

            var parts = data.sourceTypes[k].description.split("/");
            data.sourceTypes[k].category = parts.length > 1 ? parts[0] : "";
            data.sourceTypes[k].label = parts[parts.length - 1];
            data.sourceTypes[k].sourceType = k;

            $scope.newSourceParams[k] =
                JSON.stringify(data.sourceTypes[k].startSample, undefined, 2);
            $scope.paramNumLines[k] =
                $scope.newSourceParams[k].split("\n").length + 1;
        }
        sourceTypesArr.sort(compareCategoryLabel);
        $scope.sourceTypesArr = sourceTypesArr;

        var indexTypesArr = [];
        for (var k in data.indexTypes) {
            indexTypesArr.push(data.indexTypes[k]);

            var parts = data.indexTypes[k].description.split("/");
            data.indexTypes[k].category = parts.length > 1 ? parts[0] : "";
            data.indexTypes[k].label = parts[parts.length - 1];
            data.indexTypes[k].indexType = k;

            $scope.newIndexParams[k] = {};
            for (var j in data.indexTypes[k].startSample) {
                $scope.newIndexParams[k][j] =
                    JSON.stringify(data.indexTypes[k].startSample[j], undefined, 2);
                $scope.paramNumLines[j] =
                    $scope.newIndexParams[k][j].split("\n").length + 1;
            }

            var indexUI = data.indexTypes[k].ui;
            if (indexUI &&
                indexUI.controllerInitName &&
                typeof(window[indexUI.controllerInitName]) == "function") {
                window[indexUI.controllerInitName](
                    "create", null, indexUI,
                    $scope, $http, $route, $routeParams,
                    $location, $log, $sce, $uibModal);
            }
        }
        indexTypesArr.sort(compareCategoryLabel);
        $scope.indexTypesArr = indexTypesArr;

        if (origIndexName &&
            origIndexName.length > 0) {
            $http.get('/api/index/' + origIndexName).
            then(function(response) {
                var data = response.data;

                $scope.newIndexName = data.indexDef.name;
                if ($scope.isClone) {
                    $scope.newIndexName = data.indexDef.name + "-copy";
                }

                $scope.newIndexType = data.indexDef.type;
                $scope.newIndexParams[data.indexDef.type] =
                    JSON.parse(JSON.stringify(data.indexDef.params));
                for (var j in $scope.newIndexParams[data.indexDef.type]) {
                    $scope.newIndexParams[data.indexDef.type][j] =
                        JSON.stringify($scope.newIndexParams[data.indexDef.type][j],
                                       undefined, 2);
                }
                $scope.newSourceType = data.indexDef.sourceType;
                $scope.newSourceName = data.indexDef.sourceName;
                $scope.newSourceUUID = "";
                $scope.newSourceParams[data.indexDef.sourceType] =
                    JSON.stringify(data.indexDef.sourceParams,
                                   undefined, 2);
                $scope.newPlanParams =
                    JSON.stringify(data.indexDef.planParams,
                                   undefined, 2);
                $scope.paramNumLines["planParams"] =
                    $scope.newPlanParams.split("\n").length + 1;

                $scope.prevIndexUUID = "";
                if ($scope.isEdit) {
                    $scope.prevIndexUUID = data.indexDef.uuid;
                }

                var indexUI =
                    meta &&
                    meta.indexTypes &&
                    meta.indexTypes[data.indexDef.type] &&
                    meta.indexTypes[data.indexDef.type].ui;
                if (indexUI &&
                    indexUI.controllerInitName &&
                    typeof(window[indexUI.controllerInitName]) == "function") {
                    window[indexUI.controllerInitName](
                        "edit", data.indexDef.params, indexUI,
                        $scope, $http, $route, $routeParams,
                        $location, $log, $sce, $uibModal);
                }
            }, function(response) {
                $scope.errorMessage = errorMessage(response.data, response.code);
                $scope.errorMessageFull = response.data;
            })
        }
    });

    $scope.prepareIndex = function(indexName, indexType, indexParams,
                                   sourceType, sourceName,
                                   sourceUUID, sourceParams,
                                   planParams, prevIndexUUID) {
        var errorFields = {};
        var errorMessage = null;
        var errorMessageFull = null;

        function errorResult() {
            return {
                errorFields: errorFields,
                errorMessage: errorMessage,
                errorMessageFull: errorMessageFull
            }
        }

        var errs = [];
        if (!indexName) {
            errorFields["indexName"] = true;
            errs.push("index name is required");
        } else if ($scope.meta &&
                   $scope.meta.indexNameRE &&
                   !indexName.match($scope.meta.indexNameRE)) {
            errorFields["indexName"] = true;
            errs.push("index name '" + indexName + "'" +
                      " does not pass validation regexp (" +
                      $scope.meta.indexNameRE + ")");
        }
        if (!indexType) {
            errorFields["indexType"] = true;
            errs.push("index type is required");
        }
        if (!sourceType) {
            errorFields["sourceType"] = true;
            errs.push("source type is required");
        }
        if (errs.length > 0) {
            errorMessage =
                (errs.length > 1 ? "errors: " : "error: ") + errs.join("; ");
            return errorResult()
        }

        var indexParamsObj = {};
        for (var k in indexParams[indexType]) {
            try {
                indexParamsObj[k] = JSON.parse(indexParams[indexType][k]);
            } catch (e) {
                errorFields["indexParams"] = {};
                errorFields["indexParams"][indexType] = {};
                errorFields["indexParams"][indexType][k] = true;
                errorMessage =
                    "error: could not JSON parse index parameter: " + k;
                return errorResult()
            }
        }

        var indexUI =
            $scope.meta &&
            $scope.meta.indexTypes &&
            $scope.meta.indexTypes[indexType] &&
            $scope.meta.indexTypes[indexType].ui;
        if (indexUI &&
            indexUI.controllerDoneName &&
            typeof(window[indexUI.controllerDoneName]) == "function") {
            if (window[indexUI.controllerDoneName](
                "done", indexParamsObj, indexUI,
                $scope, $http, $route, $routeParams,
                $location, $log, $sce, $uibModal)) {
                return errorResult() // Possibly due to validation error.
            }
        }

        var sourceParamsObj = JSON.parse(sourceParams[sourceType] || "{}");

        // NOTE: Special case to auto-fill-in authUser with bucket name.
        // TODO: Doesn't handle bucket password, though.
        if (sourceType == "couchbase" &&
            sourceParamsObj &&
            sourceParamsObj.authUser == "" &&
            sourceParamsObj.authPassword == "") {
            sourceParamsObj.authUser = sourceName;
        }

        var planParamsObj = {};
        try {
            planParamsObj = JSON.parse(planParams);
        } catch (e) {
            errorFields["planParams"] = true;
            errorMessage =
                "error: could not JSON parse plan params";
            return errorResult();
        }

        return {
            indexDef: {
                name: indexName,
                type: indexType,
                params: indexParamsObj,
                sourceType: sourceType,
                sourceName: sourceName,
                sourceUUID: sourceUUID || "",
                sourceParams: sourceParamsObj,
                planParams: planParamsObj,
                uuid: prevIndexUUID
            }
        }
    }

    $scope.putIndex = function(indexName, indexType, indexParams,
                               sourceType, sourceName,
                               sourceUUID, sourceParams,
                               planParams, prevIndexUUID) {
        $scope.errorFields = {};
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        var rv = $scope.prepareIndex(indexName, indexType, indexParams,
                                     sourceType, sourceName,
                                     sourceUUID, sourceParams,
                                     planParams, prevIndexUUID);
        if (!rv.indexDef) {
            $scope.errorFields = rv.errorFields;
            $scope.errorMessage = rv.errorMessage;
            $scope.errorMessageFull = rv.errorMessageFull;
            return
        }

        $http.put('/api/index/' + indexName, rv.indexDef).
        then(function(response) {
            $location.path('/indexes/' + indexName);
        }, function(response) {
            $scope.errorMessage = errorMessage(response.data, response.code);
            $scope.errorMessageFull = response.data;
        });
    };

    $scope.labelize = function(s) {
        return s.replace(/_/g, ' ');
    };
}

function collapseNeighbors(arr) {
    var prevBeg = null;
    var prevEnd = null;
    var r = [];
    for (var i = 0; i < arr.length; i++) {
        var v = parseInt(arr[i]);
        if (v == NaN) {
            return arr;
        }
        if (prevEnd != null && prevEnd + 1 == v) {
            prevEnd = v;
            r[r.length - 1] = prevBeg + '-' + prevEnd;
        } else {
            r.push(arr[i])
            prevBeg = v;
            prevEnd = v;
        }
    }
    return r;
}

function compareCategoryLabel(a, b) {
    if (a.category < b.category) {
        return 1;
    }
    if (a.category > b.category) {
        return -1;
    }
    if (a.sourceType < b.sourceType) {
        return -1;
    }
    if (a.sourceType > b.sourceType) {
        return 1;
    }
    return 0;
}

function compareStats(a, b) {
    if (a.statName < b.statName) {
        return -1;
    }
    if (a.statName > b.statName) {
        return 1;
    }
    if (a.sourceName < b.sourceName) {
        return -1;
    }
    if (a.sourceName > b.sourceName) {
        return 1;
    }
    return 0;
}

function compareTime(a, b) { // More recent first.
    if (a.Time < b.Time) {
        return 1;
    }
    if (a.Time > b.Time) {
        return -1;
    }
    return 0;
}

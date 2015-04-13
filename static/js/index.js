var indexStatsPrevs = {};

var indexStatsLabels = {
    "pindexes": "index partition", "feeds": "datasource"
}

function IndexesCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.indexNames = [];
    $scope.errorMessage = null;
    $scope.errorMessageFull = null;

    $scope.refreshIndexNames = function() {
        $http.get('/api/index').success(function(data) {
            var indexNames = [];
            for (var indexName in data.indexDefs.indexDefs) {
                indexNames.push(indexName)
            }
            $scope.indexNames = indexNames;
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
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

        $http.delete('/api/index/' + name).success(function(data) {
            $scope.refreshIndexNames();
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
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

function IndexCtrl($scope, $http, $route, $routeParams, $log, $sce) {

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
    $scope.warnings = null;

    $scope.tab = $routeParams.tabName;
    if ($scope.tab === undefined || $scope.tab === "") {
        $scope.tab = "summary";
    }
    $scope.tabPath = '/static/partials/index/tab-' + $scope.tab + '.html';

    $http.get('/api/cfg').success(function(data) {
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
    }).
    error(function(data, code) {
        $scope.errorMessage = errorMessage(data, code);
        $scope.errorMessageFull = data;
    });

    $scope.loadIndexDetails = function() {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/index/' + $scope.indexName).
        success(function(data) {
            data.indexDef.params = JSON.parse(data.indexDef.params);
            data.indexDef.sourceParams = JSON.parse(data.indexDef.sourceParams);
            $scope.indexDef = data.indexDef
            $scope.indexDefStr = JSON.stringify(data.indexDef, undefined, 2);
            $scope.indexParamsStr = JSON.stringify(data.indexDef.params, undefined, 2);
            $scope.planPIndexesStr = JSON.stringify(data.planPIndexes, undefined, 2);
            $scope.planPIndexes = data.planPIndexes;
            $scope.planPIndexes.sort(
                function(x, y) {
                    if (x.name > y.name) { return 1; }
                    if (y.name > x.name) { return -1; }
                    return 0;
                }
            );
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
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.loadIndexDocCount = function() {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/index/' + $scope.indexName + '/count').
        success(function(data) {
            $scope.indexDocCount = data.count;
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.loadIndexStats = function() {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/stats/index/' + $scope.indexName).
        success(function(data) {
            $scope.indexStats = data;

            var indexStatsPrev = indexStatsPrevs[$scope.indexName];
            indexStatsPrevs[$scope.indexName] = data;

            var errors = [];
            var stats = [];
            var kinds = ["pindexes", "feeds"];
            for (var a in kinds) {
                var aa = kinds[a];

                // The k is pindexName / feedName.
                for (var k in data[aa]) {
                    var kk = data[aa][k];
                    // The j is category of stats, like bleveKVStoreStats,
                    // pindexStoreStats, destStats, bucketDataSourceStats.
                    for (var j in kk) {
                        if (j == "bucketDataSourceStats") {
                            continue;
                        }

                        var jj = data[aa][k][j];
                        errors = errors.concat(jj.Errors || []);
                        for (var s in jj) {
                            var ss = jj[s];
                            console.log(s, ss)
                            if (typeof(ss) != "object" || ss instanceof Array) {
                                continue;
                            }
                            ss.prev = ss;
                            if (indexStatsPrev) {
                                ss.prev = ((indexStatsPrev[aa][k] || {})[j] || {})[s];
                            }
                            ss.source = k;
                            ss.statKind = indexStatsLabels[aa];
                            ss.statName = s;
                            stats.push(ss);
                        }
                    }
                }
            }

            stats.sort(function(a, b) {
                if (a.statKind < b.statKind) {
                    return 1;
                }
                if (a.statKind > b.statKind) {
                    return -1;
                }
                if (a.statName < b.statName) {
                    return -1;
                }
                if (a.statName > b.statName) {
                    return 1;
                }
                if (a.pindexName < b.pindexName) {
                    return -1;
                }
                if (a.pindexName > b.pindexName) {
                    return 1;
                }
                return 0;
            });

            errors.sort(function(a, b) { // More recent errors sort first.
                if (a.Time < b.Time) {
                    return 1;
                }
                if (a.Time > b.Time) {
                    return -1;
                }
                return 0;
            });

            $scope.indexErrors = errors;
            $scope.indexStatsFlat = stats;
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
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
        success(function(data) {
            $scope.successMessage = "Indexed Document: " + id;
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.deleteDocument = function(id) {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.delete('/api/index/' + $scope.indexName + "/" + id).
        success(function(data) {
            $scope.successMessage = "Deleted Document: " + id;
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.indexControl = function(indexName, what, op) {
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.post('/api/index/' + indexName + "/" +  what + "Control/" + op).
        success(function(data) {
            alert("index " + what + " " + op);
            $scope.loadIndexDetails();
        }).
        error(function(data, code) {
            alert("index " + what + " " + op + " error: " + data);
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.refresh = function() {
        $route.reload();
    }
}

function IndexNewCtrl($scope, $http, $routeParams, $log, $sce, $location) {
    $scope.errorFields = {};
    $scope.errorMessage = null;
    $scope.errorMessageFull = null;

    $scope.newIndexName = "";
    $scope.newIndexType = "";
    $scope.newIndexParams = {};
    $scope.newSourceType = "";
    $scope.newSourceName = "";
    $scope.newSourceUUID = "";
    $scope.newSourceParams = {};
    $scope.newPlanParams = "";
    $scope.prevIndexUUID = "";
    $scope.paramNumLines = {};

    $http.get('/api/managerMeta').success(function(data) {
        $scope.meta = data;

        for (var k in data.sourceTypes) {
            $scope.newSourceParams[k] =
                JSON.stringify(data.sourceTypes[k].startSample, undefined, 2);
            $scope.paramNumLines[k] = $scope.newSourceParams[k].split("\n").length + 1;
        }

        for (var k in data.indexTypes) {
            $scope.newIndexParams[k] = {};
            for (var j in data.indexTypes[k].startSample) {
                $scope.newIndexParams[k][j] =
                    JSON.stringify(data.indexTypes[k].startSample[j], undefined, 2);
                $scope.paramNumLines[j] =
                    $scope.newIndexParams[k][j].split("\n").length + 1;
            }
        }

        $scope.newPlanParams =
            JSON.stringify(data.startSamples["planParams"], undefined, 2);
        $scope.paramNumLines["planParams"] =
            $scope.newPlanParams.split("\n").length + 1;

        origIndexName = $routeParams.indexName;
        if (origIndexName && origIndexName.length > 0) {
            $scope.isEdit = $location.path().match(/_edit$/);
            $scope.isClone = $location.path().match(/_clone$/);

            $http.get('/api/index/' + origIndexName).
            success(function(data) {
                $scope.newIndexName = data.indexDef.name;
                if ($scope.isClone) {
                    $scope.newIndexName = data.indexDef.name + "-copy";
                }

                $scope.newIndexType = data.indexDef.type;
                $scope.newIndexParams[data.indexDef.type] =
                    JSON.parse(data.indexDef.params);
                for (var j in $scope.newIndexParams[data.indexDef.type]) {
                    $scope.newIndexParams[data.indexDef.type][j] =
                        JSON.stringify($scope.newIndexParams[data.indexDef.type][j],
                                       undefined, 2);
                }
                $scope.newSourceType = data.indexDef.sourceType;
                $scope.newSourceName = data.indexDef.sourceName;
                $scope.newSourceUUID = data.indexDef.sourceUUID;
                $scope.newSourceParams[data.indexDef.sourceType] = data.indexDef.sourceParams;
                $scope.newPlanParams = JSON.stringify(data.indexDef.planParams,
                                                      undefined, 2);

                $scope.prevIndexUUID = "";
                if ($scope.isEdit) {
                    $scope.prevIndexUUID = data.indexDef.uuid;
                }
            }).
            error(function(data, code) {
                $scope.errorMessage = errorMessage(data, code);
                $scope.errorMessageFull = data;
            })
        }
    })

    $scope.putIndex = function(indexName, indexType, indexParams,
                               sourceType, sourceName,
                               sourceUUID, sourceParams,
                               planParams, prevIndexUUID) {
        $scope.errorFields = {};
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        var errs = [];
        if (!indexName) {
            $scope.errorFields["indexName"] = true;
            errs.push("index name is required");
        } else if ($scope.meta &&
            $scope.meta.indexNameRE &&
            !indexName.match($scope.meta.indexNameRE)) {
            $scope.errorFields["indexName"] = true;
            errs.push("index name '" + indexName + "'" +
                      " does not pass validation regexp (" +
                      $scope.meta.indexNameRE + ")");
        }
        if (!indexType) {
            $scope.errorFields["indexType"] = true;
            errs.push("index type is required");
        }
        if (!sourceType) {
            $scope.errorFields["sourceType"] = true;
            errs.push("source type is required");
        }
        if (errs.length > 0) {
            $scope.errorMessage =
                (errs.length > 1 ? "errors: " : "error: ") + errs.join("; ");
            return
        }

        var indexParamsObj = {};
        for (var k in indexParams[indexType]) {
            try {
                indexParamsObj[k] = JSON.parse(indexParams[indexType][k]);
            } catch (e) {
                $scope.errorFields["indexParams"] = {};
                $scope.errorFields["indexParams"][indexType] = {};
                $scope.errorFields["indexParams"][indexType][k] = true;
                $scope.errorMessage =
                    "error: could not JSON parse index parameter: " + k;
                return
            }
        }

        if (sourceParams[sourceType]) {
            try {
                JSON.parse(sourceParams[sourceType]);
            } catch (e) {
                $scope.errorFields["sourceParams"] = {};
                $scope.errorFields["sourceParams"][sourceType] = true;
                $scope.errorMessage = "error: could not JSON parse source params";
                return
            }
        }

        try {
            JSON.parse(planParams);
        } catch (e) {
            $scope.errorFields["planParams"] = true;
            $scope.errorMessage = "error: could not JSON parse plan params";
            return
        }

        $http.put('/api/index/' + indexName, "", {
            params: {
                indexName: indexName,
                indexType: indexType,
                indexParams: JSON.stringify(indexParamsObj),
                sourceType: sourceType,
                sourceName: sourceName,
                sourceUUID: sourceUUID || "",
                sourceParams: sourceParams[sourceType],
                planParams: planParams,
                prevIndexUUID: prevIndexUUID
            }
        }).
        success(function(data) {
            $location.path('/indexes/' + indexName);
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };
}

function errorMessage(errorMessageFull, code) {
    console.log("errorMessageFull", errorMessageFull, code);
    var a = (errorMessageFull || (code + "")).split("err: ");
    return a[a.length - 1];
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

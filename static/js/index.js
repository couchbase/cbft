function IndexesCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.indexNames = [];
    $scope.errorMessage = null;

    $scope.clearErrorMessage = function() {
        $scope.errorMessage = null;
    };

    $scope.refreshIndexNames = function() {
        $http.get('/api/index').success(function(data) {
            var indexNames = [];
            for (var indexName in data.indexDefs.indexDefs) {
                indexNames.push(indexName)
            }
            $scope.indexNames = indexNames;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.deleteIndex = function(name) {
        if(!confirm("Are you sure you want to permanenty delete the index '"
                    + name + "'?")) {
            return;
        }
        $scope.clearErrorMessage();
        $http.delete('/api/index/' + name).success(function(data) {
            $scope.refreshIndexNames();
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.cloneIndex = function(name) {
        cloneName = prompt("Please enter a name for the new index" +
                           " whose definition will be cloned" +
                           " from the definition of index '" +
                           name + "':");
        if (!cloneName) {
            return;
        }
        $scope.clearErrorMessage();
        $http.get('/api/index/' + name).
        success(function(data) {
            $http.put('/api/index/' + cloneName, "", {
                params: {
                    indexName: cloneName,
                    indexType: data.indexDef.type,
                    indexParams: data.indexDef.params,
                    sourceType: data.indexDef.sourceType,
                    sourceName: data.indexDef.sourceName,
                    sourceUUID: data.indexDef.sourceUUID,
                    sourceParams: data.indexDef.sourceParams,
                    planParams: data.indexDef.planParams,
                }
            }).
            success(function(data) {
                $location.path('/indexes/' + cloneName);
            }).
            error(function(data, code) {
                $scope.errorMessage = data;
            })
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        })
    };

    $scope.refreshIndexNames();
}

function IndexCtrl($scope, $http, $routeParams, $log, $sce) {

    $scope.nodeDefsByUUID = null;
    $scope.nodeDefsByAddr = null;
    $scope.nodeAddrsArr = null;

    $scope.indexName = $routeParams.indexName;
    $scope.indexDocCount = 0;
    $scope.indexDefStr = "";
    $scope.indexParamsStr = "";
    $scope.planPIndexes = null;
    $scope.planPIndexesStr = ""
    $scope.warnings = null;
    $scope.tab = $routeParams.tabName;
    if($scope.tab === undefined || $scope.tab === "") {
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
    })

    $scope.loadIndexDetails = function() {
        $http.get('/api/index/' + $scope.indexName).success(function(data) {
            data.indexDef.params = JSON.parse(data.indexDef.params)
            data.indexDef.sourceParams = JSON.parse(data.indexDef.sourceParams)
            $scope.indexDefStr = JSON.stringify(data.indexDef, undefined, 2)
            $scope.indexParamsStr = JSON.stringify(data.indexDef.params, undefined, 2)
            $scope.planPIndexesStr = JSON.stringify(data.planPIndexes, undefined, 2)
            $scope.planPIndexes = data.planPIndexes
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
            $scope.errorMessage = data;
        });
    };

    $scope.loadIndexDocCount = function() {
        $http.get('/api/index/' + $scope.indexName + '/count').
        success(function(data) {
            $scope.indexDocCount = data.count;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    // tab specific loading
    if($scope.tab === "summary") {
        $scope.loadIndexDocCount();
    }

    $scope.indexDocument = function(id, body) {
        $scope.clearErrorMessage();
        $http.put('/api/index/' + $scope.indexName + "/doc/" + id, body).
        success(function(data) {
            $scope.successMessage = "Indexed Document: " + id;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.deleteDocument = function(id) {
        $scope.clearErrorMessage();
        $http.delete('/api/index/' + $scope.indexName + "/" + id).
        success(function(data) {
            $scope.successMessage = "Deleted Document: " + id;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.clearErrorMessage = function() {
        $scope.errorMessage = null;
    };
}

function IndexNewCtrl($scope, $http, $routeParams, $log, $sce, $location) {
    $scope.newSourceParams = {};
    $scope.newIndexParams = {};
    $scope.newPlanParams = "";
    $scope.paramNumLines = {};

    $http.get('/api/managerMeta').success(function(data) {
        $scope.meta = data;

        for (var k in data.sourceTypes) {
            $scope.newSourceParams[k] =
                JSON.stringify(data.sourceTypes[k].startSample, undefined, 2);
            $scope.paramNumLines[k] = $scope.newSourceParams[k].split("\n").length;
        }

        for (var k in data.indexTypes) {
            $scope.newIndexParams[k] =
                JSON.stringify(data.indexTypes[k].startSample, undefined, 2);
            $scope.paramNumLines[k] = $scope.newIndexParams[k].split("\n").length;
        }

        $scope.newPlanParams =
            JSON.stringify(data.startSamples["planParams"], undefined, 2);
        $scope.paramNumLines["planParams"] =
            $scope.newPlanParams.split("\n").length;
    })

    $scope.errorMessage = null;

    $scope.newIndex = function(indexName, indexType, indexParams,
                               sourceType, sourceName,
                               sourceUUID, sourceParams,
                               planParams) {
        $scope.clearErrorMessage();
        $http.put('/api/index/' + indexName, "", {
            params: {
                indexName: indexName,
                indexType: indexType || "bleve",
                indexParams: indexParams[indexType],
                sourceType: sourceType,
                sourceName: sourceName,
                sourceUUID: sourceUUID || "",
                sourceParams: sourceParams[sourceType],
                planParams: planParams,
            }
        }).
        success(function(data) {
            $location.path('/indexes/' + indexName);
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.clearErrorMessage = function() {
        $scope.errorMessage = null;
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

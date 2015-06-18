var indexStatsPrevs = {};
var indexStatsAggsPrevs = {};

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
            if (data.indexDefs) {
                for (var indexName in data.indexDefs.indexDefs) {
                    indexNames.push(indexName);
                }
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

function IndexCtrl($scope, $http, $route, $routeParams, $location, $log, $sce) {

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
    success(function(data) {
        var meta = $scope.meta = data;

        $http.get('/api/cfg').
        success(function(data) {
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
                $scope.indexDef = data.indexDef
                $scope.indexDefStr = JSON.stringify(data.indexDef, undefined, 2);

                try {
                    if (typeof(data.indexDef.params) == "string") {
                        data.indexDef.params = JSON.parse(data.indexDef.params);
                    }
                } catch (e) {
                }

                try {
                    if (typeof(data.indexDef.sourceParams) == "string") {
                        data.indexDef.sourceParams = JSON.parse(data.indexDef.sourceParams);
                    }
                } catch (e) {
                }

                $scope.indexParamsStr = JSON.stringify(data.indexDef.params, undefined, 2);
                $scope.planPIndexesStr = JSON.stringify(data.planPIndexes, undefined, 2);
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
                    meta.indexTypes[data.indexDef.type].canCount

                $scope.indexCanQuery =
                    meta &&
                    meta.indexTypes &&
                    meta.indexTypes[data.indexDef.type] &&
                    meta.indexTypes[data.indexDef.type].canQuery

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
            }).
            error(function(data, code) {
                $scope.errorMessage = errorMessage(data, code);
                $scope.errorMessageFull = data;
            });
        };
    });

    $scope.loadIndexDocCount = function() {
        $scope.indexDocCount = "..."
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/index/' + $scope.indexName + '/count').
        success(function(data) {
            $scope.indexDocCount = data.count;
        }).
        error(function(data, code) {
            $scope.indexDocCount = "error"
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };

    $scope.loadIndexStats = function() {
        $scope.statsRefresh = "refreshing...";
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        $http.get('/api/stats/index/' + $scope.indexName).
        success(function(data) {
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
                    // The j is category of stats, like
                    // bleveKVStoreStats, pindexStoreStats,
                    // bucketDataSourceStats, destStats.
                    for (var j in kk) {
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
        }).
        error(function(data, code) {
            $scope.statsRefresh = "error";
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

        if (!confirm("Are you sure you want to change this setting?")) {
            return;
        }

        $http.post('/api/index/' + indexName + "/" +  what + "Control/" + op).
        success(function(data) {
            $scope.loadIndexDetails();
        }).
        error(function(data, code) {
            $scope.errorMessage = errorMessage(data, code);
            $scope.errorMessageFull = data;
        });
    };
}

function IndexNewCtrl($scope, $http, $routeParams, $log, $sce, $location) {
    $scope.advancedFields = {
        "store": true
    };

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

    $scope.mapping = null;

    $http.get('/api/managerMeta').
    success(function(data) {
        $scope.meta = data;

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
        }
        indexTypesArr.sort(compareCategoryLabel);
        $scope.indexTypesArr = indexTypesArr;

        $scope.newPlanParams =
            JSON.stringify(data.startSamples["planParams"], undefined, 2);
        $scope.paramNumLines["planParams"] =
            $scope.newPlanParams.split("\n").length + 1;
    });

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
            $scope.newSourceParams[data.indexDef.sourceType] =
                data.indexDef.sourceParams;
            $scope.newPlanParams =
                JSON.stringify(data.indexDef.planParams,
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

    $scope.putIndex = function(indexName, indexType, indexParams,
                               sourceType, sourceName,
                               sourceUUID, sourceParams,
                               planParams, prevIndexUUID,
                               bleveMapping) {
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

        // Special case for bleve/http/mapping UI editor.
        if (indexType == "bleve" &&
            bleveMapping != null &&
            !$scope.isEdit &&
            !$scope.isClone) {
            indexParamsObj.mapping = fixupMapping(bleveMapping);
        }

        if (sourceParams[sourceType]) {
            try {
                var s = JSON.parse(sourceParams[sourceType]);

                // NOTE: Special case to auto-fill-in authUser with bucket name.
                // TODO: Doesn't handle bucket password, though.
                if (sourceType == "couchbase" &&
                    s &&
                    s.authUser == "" &&
                    s.authPassword == "") {
                    s.authUser = sourceName;
                    sourceParams[sourceType] = JSON.stringify(s);
                }
            } catch (e) {
                $scope.errorFields["sourceParams"] = {};
                $scope.errorFields["sourceParams"][sourceType] = true;
                $scope.errorMessage =
                    "error: could not JSON parse source params";
                return
            }
        }

        try {
            JSON.parse(planParams);
        } catch (e) {
            $scope.errorFields["planParams"] = true;
            $scope.errorMessage =
                "error: could not JSON parse plan params";
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

function fixupEmptyFields(m) { // Originally from bleve-explorer.
	var keepFields = [];
	for (var fieldIndex in m.fields) {
		if (m.fields[fieldIndex].type !== '') {
			keepFields.push(m.fields[fieldIndex]);
		}
	}
	m.fields = keepFields;

	for (var propertyName in m.properties) {
		fixupEmptyFields(m.properties[propertyName]);
	}
}

function fixupMapping(m) { // Originally from bleve-explorer.
	var newMapping = JSON.parse(JSON.stringify(m));
    if (newMapping.default_mapping) {
	    fixupEmptyFields(newMapping.default_mapping);
	    for (var typeName in newMapping.types) {
		    fixupEmptyFields(newmapping.types[typeName]);
	    }
    }

	return newMapping;
}

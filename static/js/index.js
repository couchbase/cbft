function IndexesCtrl($scope, $http, $routeParams, $log, $sce) {

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

	$scope.deleteIndexNamed = function(name) {
		if(!confirm("Are you sure you want to permanenty delete the index '" + name + "'?")) {
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

	$scope.refreshIndexNames();
}

function IndexCtrl($scope, $http, $routeParams, $log, $sce) {

	$scope.indexName = $routeParams.indexName;
	$scope.indexDocCount = 0;
	$scope.indexDefStr = "";
	$scope.indexParamsStr = "";
	$scope.tab = $routeParams.tabName;
	if($scope.tab === undefined || $scope.tab === "") {
		$scope.tab = "summary";
	}
	$scope.tabPath = '/static/partials/index/tab-' + $scope.tab + '.html';

	$scope.loadIndexDetails = function() {
		$http.get('/api/index/' + $scope.indexName).success(function(data) {
            data.indexDef.params = JSON.parse(data.indexDef.params)
            data.indexDef.sourceParams = JSON.parse(data.indexDef.sourceParams)
            $scope.indexDefStr = JSON.stringify(data.indexDef, undefined, 2);
            $scope.indexParamsStr = JSON.stringify(data.indexParams, undefined, 2);
        }).
        error(function(data, code) {
			$scope.errorMessage = data;
        });
	};

	$scope.loadIndexDocCount = function() {
		$http.get('/api/index/' + $scope.indexName + '/count').success(function(data) {
            $scope.indexDocCount = data.count;
        }).
        error(function(data, code) {
			$scope.errorMessage = data;
        });
	};

	// always load the details
	$scope.loadIndexDetails();
	// tab specific loading
	if($scope.tab === "summary") {
		$scope.loadIndexDocCount();
	}

	$scope.indexDocument = function(id, body) {
		$scope.clearErrorMessage();
		$http.put('/api/index/' + $scope.indexName + "/doc/" + id, body).success(function(data) {
			$scope.successMessage = "Indexed Document: " + id;
		}).
		error(function(data, code) {
			$scope.errorMessage = data;
		});
	};

	$scope.deleteDocument = function(id) {
		$scope.clearErrorMessage();
		$http.delete('/api/index/' + $scope.indexName + "/" + id).success(function(data) {
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

        $scope.newPlanParams = JSON.stringify(data.startSamples["planParams"], undefined, 2);
        $scope.paramNumLines["planParams"] = $scope.newPlanParams.split("\n").length;
	})

	$scope.errorMessage = null;

	$scope.newIndexNamed = function(sourceType, sourceName, sourceUUID, sourceParams,
									indexType, indexName, indexParams,
									planParams) {
		$scope.clearErrorMessage();
		$http.put('/api/index/' + indexName, "", {
			params: {
			    sourceType: sourceType,
			    sourceName: sourceName,
			    sourceUUID: sourceUUID || "",
			    sourceParams: sourceParams[sourceType],
			    indexType: indexType || "bleve",
			    indexName: indexName,
                indexParams: indexParams[indexType],
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
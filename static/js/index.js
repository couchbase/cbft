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
	$scope.mappingFormatted = "";
	$scope.tab = $routeParams.tabName;
	if($scope.tab === undefined || $scope.tab === "") {
		$scope.tab = "summary";
	}
	$scope.tabPath = '/static/partials/index/tab-' + $scope.tab + '.html';
	$scope.indexDetails = null;

	$scope.loadIndexDetails = function() {
		$http.get('/api/index/' + $scope.indexName).success(function(data) {
            $scope.indexDetails = data.indexDef;
            $scope.mappingFormatted = JSON.stringify(data.indexMapping, undefined, 2);
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
	} else if ($scope.tab === "mapping") {
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

	$scope.errorMessage = null;

	$scope.newIndexNamed = function(sourceType, sourceName, sourceUUID, sourceParams,
									indexType, indexName, indexMapping,
									planParams) {
		$scope.clearErrorMessage();
		// TODO: looks like indexMapping isn't getting propagated here.
		$http.put('/api/index/' + indexName, "", { params: {
			sourceType: sourceType,
			sourceName: sourceName,
			sourceUUID: sourceUUID || "",
			sourceParams: sourceParams,
			indexType: indexType || "bleve",
			indexName: indexName,
			planParams: planParams,
		}}).
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
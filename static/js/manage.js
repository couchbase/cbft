function ManageCtrl($scope, $http, $routeParams, $log, $sce, $location) {

	$scope.resultMessage = null;
    $scope.resultCfg = null;

	$scope.managerKick = function(managerKickMsg) {
		$scope.resultMessage = null;
		$http.post('/api/managerKick?msg=' + managerKickMsg).
		success(function(data) {
            $scope.resultMessage = data.status;
		}).
		error(function(data, code) {
			$scope.resultMessage = data.status;
		});
	};

	$scope.refreshCfg = function(name, mapping) {
        $scope.resultCfg = null;
		$http.get('/api/cfg').success(function(data) {
            $scope.resultCfg = data;
		}).
		error(function(data, code) {
            $scope.resultCfg = data;
		});
	};

    $scope.refreshCfg();
}

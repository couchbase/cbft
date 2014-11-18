function ManageCtrl($scope, $http, $routeParams, $log, $sce, $location) {

	$scope.resultMessage = null;

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
}

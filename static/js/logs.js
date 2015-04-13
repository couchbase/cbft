function LogsCtrl($scope, $http, $routeParams, $log, $sce, $location) {

	$scope.errorMessage = null;
	$scope.logMessages = "";

	$scope.updateLogs = function(name, mapping) {
		$scope.clearErrorMessage();
		$scope.clearLogMessages();
		$http.get('/api/log').success(function(data) {
			for(var i in data.messages) {
				var message = data.messages[i];
				$scope.logMessages += $sce.trustAsHtml(message);
			}
            $scope.events = data.events;
		}).
		error(function(data, code) {
			$scope.errorMessage = data;
		});
	};

	$scope.clearErrorMessage = function() {
		$scope.errorMessage = null;
	};

	$scope.clearLogMessages = function() {
		$scope.logMessages = "";
	};

	$scope.updateLogs();
}
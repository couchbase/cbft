function LogsCtrl($scope, $http, $routeParams, $log, $sce, $location) {

	$scope.errorMessage = null;
	$scope.logMessages = "";

	$scope.updateLogs = function(name, mapping) {
		$scope.clearErrorMessage();
		$scope.clearLogMessages();
		$http.get('/api/logs').success(function(data) {
			for(var i in data.messages) {
				message = data.messages[i];
				$scope.logMessages += $sce.trustAsHtml(message);
			}
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
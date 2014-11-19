function ManageCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.resultCfg = null;
    $scope.resultCfgJSON = null;
    $scope.resultManagerKick = null;

    $scope.managerKick = function(managerKickMsg) {
        $scope.resultManagerKick = null;
        $http.post('/api/managerKick?msg=' + managerKickMsg).
        success(function(data) {
            $scope.resultManagerKick = data.status;
        }).
        error(function(data, code) {
            $scope.resultManagerKick = data.status;
        });
    };

    $scope.refreshCfg = function(name, mapping) {
        $scope.resultCfg = null;
        $scope.resultCfgJSON = null;
        $http.get('/api/cfg').success(function(data) {
            $scope.resultCfg = data;
            $scope.resultCfgJSON = JSON.stringify(data, undefined, 2);
        }).
        error(function(data, code) {
            $scope.resultCfg = data;
            $scope.resultCfgJSON = JSON.stringify(data, undefined, 2);
        });
    };

    $scope.refreshCfg();
}

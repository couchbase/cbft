function NodeCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.resultCfg = null;
    $scope.resultCfgJSON = null;

    $scope.nodeAddr = $routeParams.nodeAddr;
    $scope.tab = $routeParams.tabName;
    if($scope.tab === undefined || $scope.tab === "") {
        $scope.tab = "summary";
    }
    $scope.tabPath = '/static/partials/node/tab-' + $scope.tab + '.html';

    $scope.cfgGet = function(name, mapping) {
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

    $scope.cfgGet();
}

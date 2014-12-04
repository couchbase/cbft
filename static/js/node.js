function NodeCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.resultCfg = null;
    $scope.resultCfgJSON = null;

    $scope.nodeAddr = $routeParams.nodeAddr;
    $scope.tab = $routeParams.tabName;
    if($scope.tab === undefined || $scope.tab === "") {
        $scope.tab = "summary";
    }
    $scope.tabPath = '/static/partials/node/tab-' + $scope.tab + '.html';

    $scope.containerPartColor = function(s) {
        var r = 3.14159;
        for (var i = 0; i < s.length; i++) {
            r = r ^ (r * s.charCodeAt(i));
        }
        v = Math.abs(r).toString(16);
        v1 = v.slice(0, 1);
        v2 = v.slice(1, 3);
        return ('#1' + v1 + v2 + v2);
    }

    $scope.cfgGet = function(name, mapping) {
        $scope.resultCfg = null;
        $scope.resultCfgJSON = null;
        $http.get('/api/cfg').success(function(data) {
            for (var nodeAddr in data.nodeDefsKnown.nodeDefs) {
                var nodeDef = data.nodeDefsKnown.nodeDefs[nodeAddr];
                nodeDef.containerArr = (nodeDef.container || "").split('/');
            }
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

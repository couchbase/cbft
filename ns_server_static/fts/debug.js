function DebugCtrl($scope, $http, $routeParams, $log) {
    $scope.pindexName = "";
    $scope.docId = "";
    $scope.docResults = null;
    $scope.docDebugResults = null;
    $scope.fieldsResults = null;
    $scope.errorMessage = null;
    $scope.maxKLen = 0;
    $scope.maxVLen = 0;

    $scope.getFields = function() {
        $scope.fieldsResults = null;
        $scope.errorMessage = null;

        $http.get('/api/pindex-bleve/'+$scope.pindexName+'/fields/').
        then(function(response) {
            var data = response.data;
            $scope.fieldsResults = data;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    }

    $scope.debugDoc = function() {
        $scope.docResults = null;
        $scope.docDebugResults = null;
        $scope.errorMessage = null;
        $scope.maxKLen = 0;
        $scope.maxVLen = 0;

        $http.get('/api/pindex-bleve/'+$scope.pindexName+'/doc/'+
                  encodeURIComponent($scope.docId)).
        then(function(response) {
            var data = response.data;
            $scope.docResults = data;

            $http.get('/api/pindex-bleve/'+$scope.pindexName+'/docDebug/'+
                      encodeURIComponent($scope.docId)).
            then(function(response) {
                var data = response.data;
                $scope.processDocDebugResults(data);
            }, function(response) {
                var data = response.data;
                $scope.errorMessage = data;
            });
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    $scope.processDocDebugResults = function(data) {
        $scope.docDebugResults = data;

        for(var i in $scope.docDebugResults) {
            var row = $scope.docDebugResults[i];
            row.k = atob(row.key).split('');
            if (row.k.length > $scope.maxKLen) {
                $scope.maxKLen = row.k.length;
            }
            row.ki = base64DecToArr(row.key);
            row.v = atob(row.val).split('');
            if (row.v.length > $scope.maxVLen) {
                $scope.maxVLen = row.v.length;
            }
        }

        $scope.klentimes = new Array($scope.maxKLen);
        $scope.vlentimes = new Array($scope.maxVLen);
    };
}
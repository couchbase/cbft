//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
import {bleveIndexMappingScrub} from "./index-mapping.js";
export default BleveAnalyzerModalCtrl;
BleveAnalyzerModalCtrl.$inject = ["$scope", "$modalInstance", "$http",
                                  "name", "value", "mapping", "static_prefix"];
function BleveAnalyzerModalCtrl($scope, $modalInstance, $http,
                                name, value, mapping, static_prefix) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.analyzer = {};
    $scope.analyzer.token_filters = [];
    $scope.analyzer.char_filters = [];
    // copy in value for editing
    for (var k in value) {
        // need deeper copy of nested arrays
        if (k == "char_filters") {
            let newcharfilters = [];
            for (var cfi in value.char_filters) {
                newcharfilters.push(value.char_filters[cfi]);
            }
            $scope.analyzer.char_filters = newcharfilters;
        } else if (k == "token_filters") {
            let newtokenfilters = [];
            for (var tfi in value.token_filters) {
                newtokenfilters.push(value.token_filters[tfi]);
            }
            $scope.analyzer.token_filters = newtokenfilters;
        } else {
            $scope.analyzer[k] = value[k];
        }
    }

    $scope.tokenizerNames = [];

    $scope.loadTokenizerNames = function() {
        $http.post('/api/_tokenizerNames', bleveIndexMappingScrub(mapping)).
        then(function(response) {
            var data = response.data;
            $scope.tokenizerNames = data.tokenizers;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    $scope.loadTokenizerNames();

    $scope.charFilterNames = [];

    $scope.loadCharFilterNames = function() {
        $http.post('/api/_charFilterNames', bleveIndexMappingScrub(mapping)).
        then(function(response) {
            var data = response.data;
            $scope.charFilterNames = data.char_filters;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    $scope.loadCharFilterNames();

    $scope.addCharFilter = function(scope) {
        let filter = scope.addCharacterFilterName;
        if (filter !== undefined && filter !== "") {
            $scope.selectedAnalyzer.char_filters.push(filter);
        }
    };

    $scope.removeCharFilter = function(index) {
        $scope.selectedAnalyzer.char_filters.splice(index, 1);
    };

    $scope.tokenFilterNames = [];

    $scope.loadTokenFilterNames = function() {
        $http.post('/api/_tokenFilterNames', bleveIndexMappingScrub(mapping)).
        then(function(response) {
            var data = response.data;
            $scope.tokenFilterNames = data.token_filters;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    $scope.loadTokenFilterNames();

    $scope.addCharFilter = function(scope) {
        let filter = scope.addCharacterFilterName;
        if (filter !== undefined && filter !== "") {
            $scope.analyzer.char_filters.indexOf(filter) === -1 ?
            $scope.analyzer.char_filters.push(filter):
            $scope.errorMessage = "Character Filter '" + filter + "' already exists";
        }
    };

    $scope.removeCharFilter = function(index) {
        $scope.analyzer.char_filters.splice(index, 1);
    };

    $scope.addTokenFilter = function(scope) {
        let filter = scope.addTokenFilterName;
        if (filter !== undefined && filter !== "") {
            $scope.analyzer.token_filters.indexOf(filter) === -1 ?
            $scope.analyzer.token_filters.push(filter):
            $scope.errorMessage = "Tokenfilter '" + filter + "' already exists";
        }
    };

    $scope.removeTokenFilter = function(index) {
        $scope.analyzer.token_filters.splice(index, 1);
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.build = function(name) {
        if (!name) {
            $scope.errorMessage = "Name is required";
            return;
        }

        // name must not already be used
        if (name != $scope.origName &&
            $scope.mapping.analysis.analyzers[name]) {
            $scope.errorMessage = "Analyzer named '" + name + "' already exists";
            return;
        }

        // ensure that this new mapping component is valid
        let analysis = {};
        for (var ak in $scope.mapping.analysis) {
            analysis[ak] = $scope.mapping.analysis[ak];
        }
        let analyzers = {};
        analyzers[name] = $scope.analyzer;
        analysis["analyzers"] = analyzers;

        let testMapping = {
            "analysis": analysis
        };

        $http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
        then(function() {
            // if its valid return it
            let result = {};
            result[name] = $scope.analyzer;
            $modalInstance.close(result);
        }, function(response) {
            // otherwise display error
            var data = response.data;
            $scope.errorMessage = data;
        });
    };
}

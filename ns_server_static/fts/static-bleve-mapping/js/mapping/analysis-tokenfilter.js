//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
import {bleveIndexMappingScrub} from "./index-mapping.js";
export default BleveTokenFilterModalCtrl;
function BleveTokenFilterModalCtrl($scope, $modalInstance, $http,
                                   name, value, mapping, static_prefix) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.tokenfilter = {};
    // copy in value for editing
    for (var k in value) {
        $scope.tokenfilter[k] = value[k];
    }

    $scope.tokenMapNames = [];

    $scope.loadTokenMapNames = function() {
        $http.post('/api/_tokenMapNames', bleveIndexMappingScrub(mapping)).
        then(function(response) {
            var data = response.data;
            $scope.tokenMapNames = data.token_maps;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    $scope.loadTokenMapNames();

    var sp = ($scope.static_prefix || '/static-bleve-mapping');

    $scope.unknownTokenFilterTypeTemplate =
        sp + "/partials/analysis/tokenfilters/generic.html";

    $scope.tokenFilterTypeTemplates = {
        "dict_compound": sp + "/partials/analysis/tokenfilters/dict_compound.html",
        "edge_ngram": sp + "/partials/analysis/tokenfilters/edge_ngram.html",
        "elision": sp + "/partials/analysis/tokenfilters/elision.html",
        "keyword_marker": sp + "/partials/analysis/tokenfilters/keyword_marker.html",
        "length": sp + "/partials/analysis/tokenfilters/length.html",
        "ngram": sp + "/partials/analysis/tokenfilters/ngram.html",
        "normalize_unicode": sp + "/partials/analysis/tokenfilters/normalize_unicode.html",
        "shingle": sp + "/partials/analysis/tokenfilters/shingle.html",
        "stop_tokens": sp + "/partials/analysis/tokenfilters/stop_tokens.html",
        "truncate_token": sp + "/partials/analysis/tokenfilters/truncate_token.html",
    };

    $scope.tokenFilterTypeDefaults = {
        "dict_compound": function() {
            return {
                "dict_token_map": $scope.tokenMapNames[0]
            };
        },
        "edge_ngram": function() {
            return {
                "back": "false",
                "min": 3,
                "max": 3,
            };
        },
        "elision": function() {
            return {
                "articles_token_map": $scope.tokenMapNames[0]
            };
        },
        "keyword_marker": function() {
            return {
                "keywords_token_map": $scope.tokenMapNames[0]
            };
        },
        "length": function() {
            return {
                "min": 3,
                "max": 255
            };
        },
        "ngram": function() {
            return {
                "min": 3,
                "max": 3
            };
        },
        "normalize_unicode": function() {
            return {
                "form": "nfc"
            };
        },
        "shingle": function() {
            return {
                "min": 2,
                "max": 2,
                "output_original": false,
                "separator": "",
                "filler": ""
            };
        },
        "stop_tokens": function() {
            return {
                "stop_token_map": $scope.tokenMapNames[0]
            };
        },
        "truncate_token": function() {
            return {
                "length": 25
            };
        },
    };

    $scope.tokenFilterTypes = [];

    let updateTokenFilterTypes = function() {
        $http.get('/api/_tokenFilterTypes').
        then(function(response) {
            var data = response.data;
            $scope.tokenFilterTypes = data.token_filter_types;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    updateTokenFilterTypes();

    if (!$scope.tokenfilter.type) {
        let defaultType = "length";
        if ($scope.tokenFilterTypeDefaults[defaultType]) {
            $scope.tokenfilter = $scope.tokenFilterTypeDefaults[defaultType]();
        }
        else {
            $scope.tokenfilter = {};
        }
        $scope.tokenfilter.type = defaultType;
    }
    $scope.formpath = $scope.tokenFilterTypeTemplates[$scope.tokenfilter.type];

    $scope.tokenFilterTypeChange = function() {
        let newType = $scope.tokenfilter.type;
        if ($scope.tokenFilterTypeDefaults[$scope.tokenfilter.type]) {
            $scope.tokenfilter = $scope.tokenFilterTypeDefaults[$scope.tokenfilter.type]();
        } else {
            $scope.tokenfilter = {};
        }
        $scope.tokenfilter.type = newType;
        if ($scope.tokenFilterTypeTemplates[$scope.tokenfilter.type]) {
            $scope.formpath = $scope.tokenFilterTypeTemplates[$scope.tokenfilter.type];
        } else {
            $scope.formpath = $scope.unknownTokenFilterTypeTemplate;
        }
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
            $scope.mapping.analysis.token_filters[name]) {
            $scope.errorMessage = "Token filter named '" + name + "' already exists";
            return;
        }

        switch ($scope.tokenfilter.type) {
            case "edge_ngram":
            case "length":
            case "ngram":
            case "shingle":
                // Max >= Min for length
                if ($scope.tokenfilter.min > $scope.tokenfilter.max) {
                    $scope.errorMessage = "Min should be <= Max";
                    return;
                }
                break;
            default:
                break;
        }

        // ensure that this new mapping component is valid
        let tokenfilters = {};
        tokenfilters[name] = $scope.tokenfilter;

        let testMapping = {
            "analysis": {
                "token_filters": tokenfilters,
                "token_maps": $scope.mapping.analysis.token_maps
            }
        };

        $http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
        then(function() {
            // if its valid return it
            let result = {};
            result[name] = $scope.tokenfilter;
            $modalInstance.close(result);
        }, function(response) {
            // otherwise display error
            var data = response.data;
            $scope.errorMessage = data;
        });
    };
}

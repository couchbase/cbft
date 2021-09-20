//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
import {bleveIndexMappingScrub} from "./index-mapping.js";
export default BleveCharFilterModalCtrl;
function BleveCharFilterModalCtrl($scope, $modalInstance, $http,
                                  name, value, mapping, static_prefix) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.charfilter = {};
    // copy in value for editing
    for (var k in value) {
        $scope.charfilter[k] = value[k];
    }

    var sp = ($scope.static_prefix || '/static-bleve-mapping');

    $scope.unknownCharFilterTypeTemplate =
        sp + "/partials/analysis/charfilters/generic.html";

    $scope.charFilterTypeTemplates = {
        "regexp": sp + "/partials/analysis/charfilters/regexp.html",
    };

    $scope.charFilterTypeDefaults = {
        "regexp": function() {
            return {
                "regexp": "",
                "replace": ""
            };
        }
    };

    $scope.charFilterTypes = [];

    var updateCharFilterTypes = function() {
        $http.get('/api/_charFilterTypes').
        then(function(response) {
            var data = response.data;
            $scope.charFilterTypes = data.char_filter_types;
        }, function(response) {
            var data = response.data;
            $scope.errorMessage = data;
        });
    };

    updateCharFilterTypes();

    if (!$scope.charfilter.type) {
        let defaultType = "regexp";
        if ($scope.charFilterTypeDefaults[defaultType]) {
            $scope.charfilter = $scope.charFilterTypeDefaults[defaultType]();
        }
        else {
            $scope.charfilter = {};
        }
        $scope.charfilter.type = defaultType;
    }

    $scope.formpath = $scope.charFilterTypeTemplates[$scope.charfilter.type];

    $scope.charFilterTypeChange = function() {
        let newType = $scope.charfilter.type;
        if ($scope.charFilterTypeDefaults[$scope.charfilter.type]) {
            $scope.charfilter = $scope.charFilterTypeDefaults[$scope.charfilter.type]();
        } else {
            $scope.charfilter = {};
        }
        $scope.charfilter.type = newType;
        if ($scope.charFilterTypeTemplates[$scope.charfilter.type]) {
            $scope.formpath = $scope.charFilterTypeTemplates[$scope.charfilter.type];
        } else {
            $scope.formpath = $scope.unknownCharFilterTypeTemplate;
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
            $scope.mapping.analysis.char_filters[name]) {
            $scope.errorMessage = "Character filter named '" + name + "' already exists";
            return;
        }

        // ensure that this new mapping component is valid
        let charFilters = {};
        charFilters[name] = $scope.charfilter;

        let testMapping = {
            "analysis": {
                "char_filters": charFilters
            }
        };

        $http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
        then(function() {
            // if its valid return it
            let result = {};
            result[name] = $scope.charfilter;
            $modalInstance.close(result);
        }, function(response) {
            // otherwise display error
            var data = response.data;
            $scope.errorMessage = data;
        });
    };
}

//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

function BleveDatetimeParserModalCtrl($scope, $modalInstance,
                                      name, layouts, mapping, static_prefix) {
    $scope.name = name;
    $scope.origName = name;
    $scope.errorMessage = "";
    $scope.formdata = {};
    $scope.layouts = layouts.slice(0); // create copy
    $scope.selectedLayouts = [];
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.addLayout = function() {
        if ($scope.formdata.newLayout) {
            for (var i = 0; i < $scope.layouts.length; i++) {
                if ($scope.layouts[i] == $scope.formdata.newLayout) {
                    return;
                }
            }

            $scope.layouts.push($scope.formdata.newLayout);
            $scope.formdata.newLayout = "";
        }
    };

    $scope.removeLayouts = function(selectedLayouts) {
        // sort the selected layout indexes into descending order
        // so we can delete items without having to adjust indexes
        selectedLayouts.sort(function(a, b) { return b - a; });
        for (var index in selectedLayouts) {
            $scope.layouts.splice(selectedLayouts[index], 1);
        }
        $scope.selectedLayouts = [];
    };

    $scope.build = function(name) {
        if (!name) {
            $scope.errorMessage = "Name is required";
            return;
        }

        // name must not already be used
        if (name != $scope.origName &&
            $scope.mapping.analysis.date_time_parsers[name]) {
            $scope.errorMessage = "Date time parser named '" + name + "' already exists";
            return;
        }

        result = {};
        result[name] = {
            "type": "flexiblego",
            "layouts": $scope.layouts
        };

        $modalInstance.close(result);
    };
};

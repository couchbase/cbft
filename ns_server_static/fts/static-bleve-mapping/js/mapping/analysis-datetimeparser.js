//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
export default BleveDatetimeParserModalCtrl;
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

        let result = {};
        result[name] = {
            "type": "flexiblego",
            "layouts": $scope.layouts
        };

        $modalInstance.close(result);
    };
}

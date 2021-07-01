//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
export default BleveWordListModalCtrl;
function BleveWordListModalCtrl($scope, $modalInstance,
                                name, words, mapping, static_prefix) {
    $scope.name = name;
    $scope.origName = name;
    $scope.errorMessage = "";
    $scope.formdata = {};
    $scope.words = words.slice(0); // create copy
    $scope.selectedWords = [];
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.addWord = function() {
        if ($scope.formdata.newWord) {
            for (var i = 0; i < $scope.words.length; i++) {
                if ($scope.words[i] == $scope.formdata.newWord) {
                    return;
                }
            }

            $scope.words.push($scope.formdata.newWord);
            $scope.formdata.newWord = "";
        }
    };

    $scope.removeWords = function(selectedWords) {
        // sort the selected word indexes into descending order
        // so we can delete items without having to adjust indexes
        selectedWords.sort(function(a, b) { return b - a; });
        for (var index in selectedWords) {
            $scope.words.splice(selectedWords[index], 1);
        }
        $scope.selectedWords = [];
    };

    $scope.build = function(name) {
        if (!name) {
            $scope.errorMessage = "Name is required";
            return;
        }

        // name must not already be used
        if (name != $scope.origName &&
            $scope.mapping.analysis.token_maps[name]) {
            $scope.errorMessage = "Word list named '" + name + "' already exists";
            return;
        }

        let result = {};
        result[name] = {
            "type": "custom",
            "tokens": $scope.words
        };

        $modalInstance.close(result);
    };
}

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

        result = {};
        result[name] = {
            "type": "custom",
            "tokens": $scope.words
        };

        $modalInstance.close(result);
    };
};

//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
import angular from "angular";
export default BleveSynonymSourceModalCtrl;
BleveSynonymSourceModalCtrl.$inject = ["$scope", "$modalInstance",
                                        "name", "value", "mapping", "languages", "static_prefix"];
function BleveSynonymSourceModalCtrl($scope, $modalInstance,
                                      name, value, mapping, languages, static_prefix) {
    $scope.name = name;
    $scope.origName = name;
    $scope.errorMessage = "";
    $scope.formdata={};
    $scope.formdata.bucket = $scope.newSourceName;
    $scope.formdata.scope = $scope.newScopeName;
    $scope.formdata.collection = value.collection;
    $scope.formdata.analyzer = value.analyzer;
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.getLanguageFromAnalyzer = function(analyzer) {
        if (!$scope.languages || !$scope.languages.length || !analyzer) {
            return analyzer;
        }
        var selectedItem = $scope.languages.find(function(item) {
            return item.id === analyzer;
        });
        return selectedItem ? selectedItem.label : analyzer;
    };

    if (languages) {
        $scope.languages = languages;
        $scope.languageNames = languages.map(lang => lang.label);
        $scope.languageNameSelected = value.analyzer ? $scope.getLanguageFromAnalyzer(value.analyzer) : "";
    }

    $scope.getAnalyzerFromLanguage = function(language) {
        if (!$scope.languages || !$scope.languages.length || !language) {
            return language;
        }
        var selectedItem = $scope.languages.find(function(item) {
            return item.label === language;
        });
        return selectedItem ? selectedItem.id : language;
    };

    $scope.updateCollection = function(i) {
        $scope.formdata.collection = i;
    }

    $scope.updateAnalyzer = function(i) {
        $scope.formdata.analyzer = i;
    }

    $scope.updateLanguage = function(i) {
        $scope.formdata.analyzer = $scope.getAnalyzerFromLanguage(i);
    }

    $scope.build = function(name) {
        if (!name) {
            $scope.errorMessage = "Name is required";
            return;
        }

        // name must not already be used
        if (name != $scope.origName &&
            angular.isDefined($scope.mapping.analysis.synonym_sources) &&
            $scope.mapping.analysis.synonym_sources[name]) {
            $scope.errorMessage = "Synonym source named '" + name + "' already exists";
            return;
        }

        if (!$scope.formdata.collection) {
            $scope.errorMessage = "Collection is required";
            return;
        }

        if (!$scope.formdata.analyzer) {
            if (languages) {
                $scope.errorMessage = "Language is required";
            } else {
                $scope.errorMessage = "Analyzer is required";
            }
            return;
        }

        let result = {};
        result[name] = {
            "collection": $scope.formdata.collection,
            "analyzer": $scope.formdata.analyzer
        };

        $modalInstance.close(result);
    };
}

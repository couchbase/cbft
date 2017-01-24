function BleveTokenizerModalCtrl($scope, $modalInstance, $http,
                                 name, value, mapping, static_prefix) {
    $scope.origName = name;
    $scope.name = name;
    $scope.errorMessage = "";
    $scope.formpath = "";
    $scope.mapping = mapping;
    $scope.static_prefix = static_prefix;

    $scope.tokenizer = {};
    // copy in value for editing
    for (var k in value) {
        $scope.tokenizer[k] = value[k];
    }

    $scope.tokenizerNames = [];

    $scope.loadTokenizerNames = function() {
        $http.post('/api/_tokenizerNames',mapping).success(function(data) {
            $scope.tokenizerNames = data.tokenizers;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    $scope.loadTokenizerNames();

    var sp = ($scope.static_prefix || '/static-bleve-mapping');

    $scope.unknownTokenizerTypeTemplate =
        sp + "/partials/analysis/tokenizers/generic.html";

    $scope.tokenizerTypeTemplates = {
        "regexp": sp + "/partials/analysis/tokenizers/regexp.html",
        "exception": sp + "/partials/analysis/tokenizers/exception.html"
    };

    $scope.tokenizerTypeDefaults = {
        "regexp": function() {
            return {
                "regexp": ""
            };
        },
        "exception": function() {
            return {
                "exceptions": [],
                "tokenizer": "unicode"
            };
        }
    };

    $scope.tokenizerTypes = [];

    updateTokenizerTypes = function() {
        $http.get('/api/_tokenizerTypes').success(function(data) {
            $scope.tokenizerTypes = data.tokenizer_types;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    updateTokenizerTypes();

    if (!$scope.tokenizer.type) {
        defaultType = "regexp";
        if ($scope.tokenizerTypeDefaults[defaultType]) {
            $scope.tokenizer = $scope.tokenizerTypeDefaults[defaultType]();
        }
        else {
            $scope.tokenizer = {};
        }
        $scope.tokenizer.type = defaultType;
    }
    $scope.formpath = $scope.tokenizerTypeTemplates[$scope.tokenizer.type];

    $scope.tokenizerTypeChange = function() {
        newType = $scope.tokenizer.type;
        if ($scope.tokenizerTypeDefaults[$scope.tokenizer.type]) {
            $scope.tokenizer = $scope.tokenizerTypeDefaults[$scope.tokenizer.type]();
        } else {
            $scope.tokenizer = {};
        }
        $scope.tokenizer.type = newType;
        if ($scope.tokenizerTypeTemplates[$scope.tokenizer.type]) {
            $scope.formpath = $scope.tokenizerTypeTemplates[$scope.tokenizer.type];
        } else {
            $scope.formpath = $scope.unknownTokenizerTypeTemplate;
        }
    };

    $scope.addException = function(scope) {
        if (scope.newregexp) {
            $scope.tokenizer.exceptions.push(scope.newregexp);
            scope.newregexp = "";
        }
    };

    $scope.removeException = function(index) {
        $scope.tokenizer.exceptions.splice(index, 1);
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
            $scope.mapping.analysis.tokenizers[name]) {
            $scope.errorMessage = "Tokenizer named '" + name + "' already exists";
            return;
        }

        // ensure that this new mapping component is valid
        tokenizers = {};
        tokenizers[name] = $scope.tokenizer;
        // add in all the existing tokenizers, since we might be referencing them
        for (var t in $scope.mapping.analysis.tokenizers) {
            tokenizers[t] = $scope.mapping.analysis.tokenizers[t];
        }

        testMapping = {
            "analysis": {
                "tokenizers": tokenizers
            }
        };

        $http.post('/api/_validateMapping',testMapping).success(function(data) {
            // if its valid return it
            result = {};
            result[name] = $scope.tokenizer;
            $modalInstance.close(result);
        }).
        error(function(data, code) {
            // otherwise display error
            $scope.errorMessage = data;
        });
    };
};

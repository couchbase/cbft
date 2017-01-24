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
        $http.post('/api/_tokenMapNames',mapping).success(function(data) {
            $scope.tokenMapNames = data.token_maps;
        }).
        error(function(data, code) {
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

    updateTokenFilterTypes = function() {
        $http.get('/api/_tokenFilterTypes').success(function(data) {
            $scope.tokenFilterTypes = data.token_filter_types;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    updateTokenFilterTypes();

    if (!$scope.tokenfilter.type) {
        defaultType = "length";
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
        newType = $scope.tokenfilter.type;
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

        // ensure that this new mapping component is valid
        tokenfilters = {};
        tokenfilters[name] = $scope.tokenfilter;

        testMapping = {
            "analysis": {
                "token_filters": tokenfilters,
                "token_maps": $scope.mapping.analysis.token_maps
            }
        };

        $http.post('/api/_validateMapping',testMapping).success(function(data) {
            // if its valid return it
            result = {};
            result[name] = $scope.tokenfilter;
            $modalInstance.close(result);
        }).
        error(function(data, code) {
            // otherwise display error
            $scope.errorMessage = data;
        });
    };
};

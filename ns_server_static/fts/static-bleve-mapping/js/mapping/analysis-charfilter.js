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

    updateCharFilterTypes = function() {
        $http.get('/api/_charFilterTypes').success(function(data) {
            $scope.charFilterTypes = data.char_filter_types;
        }).
        error(function(data, code) {
            $scope.errorMessage = data;
        });
    };

    updateCharFilterTypes();

    if (!$scope.charfilter.type) {
        defaultType = "regexp";
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
        newType = $scope.charfilter.type;
        if ($scope.charFilterTypeDefaults[$scope.charfilter.type]) {
            $scope.charfilter = $scope.charFilterTypeDefaults[$scope.charfilter.type]();
        } else {
            $scope.charfilter = {};
        }
        $scope.charfilter.type = newType;
        if ($scope.charFilterTypeTemplates[$scope.charfilter.type]) {
            $scope.formpath = $scope.charFilterTypeTemplates[$scope.charfilter.type];
        } else {
            $scope.formpath = unknownCharFilterTypeTemplate;
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
        charFilters = {};
        charFilters[name] = $scope.charfilter;

        testMapping = {
            "analysis": {
                "char_filters": charFilters
            }
        };

        $http.post('/api/_validateMapping',testMapping).success(function(data) {
            // if its valid return it
            result = {};
            result[name] = $scope.charfilter;
            $modalInstance.close(result);
        }).
        error(function(data, code) {
            // otherwise display error
            $scope.errorMessage = data;
        });
    };
};

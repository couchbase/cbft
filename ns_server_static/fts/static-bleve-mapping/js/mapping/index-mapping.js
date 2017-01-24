function initBleveIndexMappingController(
    $scope, $http, $log, $uibModal, indexMappingIn, options) {
    options = options || {};

    $scope.static_prefix = $scope.static_prefix || 'static-bleve-mapping';

	var indexMapping =
        $scope.indexMapping = JSON.parse(JSON.stringify(indexMappingIn));

    indexMapping.types =
        indexMapping.types || {};
    indexMapping.analysis =
        indexMapping.analysis || {};
	indexMapping.analysis.analyzers =
        indexMapping.analysis.analyzers || {};
	indexMapping.analysis.char_filters =
        indexMapping.analysis.char_filters || {};
	indexMapping.analysis.tokenizers =
        indexMapping.analysis.tokenizers ||{};
	indexMapping.analysis.token_filters =
        indexMapping.analysis.token_filters || {};
	indexMapping.analysis.token_maps =
        indexMapping.analysis.token_maps || {};

    if (indexMapping["default_mapping"]) {
        indexMapping.types[""] = indexMapping["default_mapping"];
    }

    var tmc = initBleveTypeMappingController($scope, indexMapping.types, options);

    $scope.isValid = function() { return tmc.isValid(); };

    $scope.indexMappingResult = indexMappingResult;

    BleveAnalysisCtrl($scope, $http, $log, $uibModal);

    // ------------------------------------------------

    $scope.analyzerNames = options.analyzerNames || [];
	$scope.loadAnalyzerNames = function() {
        $http.post('/api/_analyzerNames', $scope.indexMappingResult()).
        success(function(data) {
            $scope.analyzerNames = data.analyzers;
        }).
        error(function(data, code) {
			$scope.errorMessage = data;
        });
	};
    if (options.analyzerNames == null) {
	    $scope.loadAnalyzerNames();
    }

    $scope.dateTimeParserNames = options.dateTimeParserNames || [];
	$scope.loadDatetimeParserNames = function() {
        $http.post('/api/_datetimeParserNames', $scope.indexMappingResult()).
        success(function(data) {
            $scope.dateTimeParserNames = data.datetime_parsers;
        }).
        error(function(data, code) {
			$scope.errorMessage = data;
        });
	};
    if (options.dateTimeParserNames == null) {
	    $scope.loadDatetimeParserNames();
    }

    // ------------------------------------------------

    return {
        // Allows the caller to determine if there's a valid index mapping.
        isValid: $scope.isValid,

        // Allows the caller to retrieve the current index mapping,
        // perhaps in response to a user's Create/Done/OK button click;
        // or, the user wanting to see the index mapping JSON.
        indexMapping: $scope.indexMappingResult
    }

    // ------------------------------------------------

    function indexMappingResult() {
        if (!$scope.isValid()) {
            return null;
        }

        var r = JSON.parse(JSON.stringify($scope.indexMapping));

        r.types = tmc.typeMapping();
        r.default_mapping = r.types[""];
        delete r.types[""];

        return JSON.parse(JSON.stringify(scrub(r)));
    }

    // Recursively remove every entry with '$' prefix, which might be
    // due to angularjs metadata.
    function scrub(m) {
        if (typeof(m) == "object") {
            for (var k in m) {
                if (typeof(k) == "string" && k.charAt(0) == "$") {
                    delete m[k];
                    continue;
                }

                m[k] = scrub(m[k]);

                if (typeof(m[k]) == "object" && isEmpty(m[k])) {
                    delete m[k];
                }
            }
        }

        return m;
    }

    function isEmpty(obj) {
        for (var k in obj) {
            return false;
        }
        return true;
    }
}

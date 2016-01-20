var ftsAppName = 'fts';
var ftsPrefix = 'fts';

// -------------------------------------------------------

(function () {
  "use strict";

  angular
    .module(ftsAppName,
            ["ui.router", "mnPluggableUiRegistry", "mnJquery", "ngRoute", "ui.tree"])
    .config(function($stateProvider, mnPluggableUiRegistryProvider) {
      $stateProvider
            .state('app.admin.fts_list', {
                url: '/fts_list',
                controller: 'IndexesCtrlFT_NS',
                templateUrl: '/_p/ui/fts/fts_list.html'
            })
            .state('app.admin.fts_view', {
                url: '/fts_view/:indexName?tabName',
                controller: 'IndexCtrlFT_NS',
                templateUrl: '/_p/ui/fts/fts_view.html'
            })
            .state('app.admin.fts_new', {
                url: '/fts_new/?indexType&sourceType',
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '/_p/ui/fts/fts_new.html'
            })
            .state('app.admin.fts_edit', {
                url: '/fts_edit/:indexName/_edit',
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '/_p/ui/fts/fts_new.html'
            })
            .state('app.admin.fts_clone', {
                url: '/fts_clone/:indexName/_clone',
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '/_p/ui/fts/fts_new.html'
            })
            .state('app.admin.fts_search', {
                url: '/fts_search/:indexName/_search?query',
                controller: 'IndexSearchCtrlFT_NS',
                templateUrl: '/_p/ui/fts/fts_search.html'
            });

        mnPluggableUiRegistryProvider.registerConfig({
            name: 'Full Text',
            state: 'app.admin.fts_list',
            plugIn: 'indexesTab'
        });
    });

  angular.module('mnAdmin').requires.push('fts');

  angular.module(ftsAppName).
        controller('IndexesCtrlFT_NS', IndexesCtrlFT_NS).
        controller('IndexCtrlFT_NS', IndexCtrlFT_NS).
        controller('IndexNewCtrlFT_NS', IndexNewCtrlFT_NS).
        controller('IndexSearchCtrlFT_NS', IndexSearchCtrlFT_NS);
}());

// ----------------------------------------------

function IndexesCtrlFT_NS($scope, $http, $stateParams,
                          $log, $sce, $location) {
    var $routeParams = $stateParams;
    return IndexesCtrl($scope,
                       prefixedHttp($http, '/_p/' + ftsPrefix),
                       $routeParams, $log, $sce, $location);
}

function IndexCtrlFT_NS($scope, $http, $route, $stateParams,
                        $location, $log, $sce, $uibModal) {
    var $routeParams = $stateParams;
    return IndexCtrl($scope,
                     prefixedHttp($http, '/_p/' + ftsPrefix),
                     $route, $routeParams,
                     $location, $log, $sce, $uibModal);
}

function IndexNewCtrlFT_NS($scope, $http, $route, $stateParams,
                           $location, $log, $sce, $uibModal,
                           $q, mnBucketsService) {
    mnBucketsService.getBucketsByType(true).then(function(buckets) {
        $scope.buckets = buckets;
        $scope.bucketNames = [];
        for (var i = 0; i < buckets.length; i++) {
            $scope.bucketNames.push(buckets[i].name);
        }

        var $routeParams = $stateParams;

        var $locationRewrite = {
            host: $location.host,
            path: function(p) {
                if (!p) {
                    return $location.path();
                }
                return $location.path(p.replace(/^\/indexes\//, "/fts_view/"))
            }
        }

        IndexNewCtrlFT($scope,
                       prefixedHttp($http, '/_p/' + ftsPrefix),
                       $route, $routeParams,
                       $locationRewrite, $log, $sce, $uibModal,
                       finishIndexNewCtrlFTInit)

        function finishIndexNewCtrlFTInit() {
            var putIndexOrig = $scope.putIndex;

            $scope.putIndex = function(newIndexName,
                                       newIndexType, newIndexParams,
                                       newSourceType, newSourceName,
                                       newSourceUUID, newSourceParams,
                                       newPlanParams, prevIndexUUID) {
                if (!newSourceUUID) {
                    for (var i = 0; i < buckets.length; i++) {
                        if (newSourceName == buckets[i].name) {
                            newSourceUUID = buckets[i].uuid;
                        }
                    }
                }

                return putIndexOrig(newIndexName,
                                    newIndexType, newIndexParams,
                                    newSourceType, newSourceName,
                                    newSourceUUID, newSourceParams,
                                    newPlanParams, prevIndexUUID);
            };
        }
    });
}

function IndexSearchCtrlFT_NS($scope, $http, $stateParams, $log, $sce, $location) {
    var $routeParams = $stateParams;

    $scope.indexName = $stateParams.indexName;

    $scope.static_base = "/_p/ui/fts";

    QueryCtrl($scope,
              prefixedHttp($http, '/_p/' + ftsPrefix, true),
              $routeParams, $log, $sce, $location);

    $scope.query = $routeParams.query || "";
    $scope.page = $routeParams.page || "";

    if (!$location.search().q) {
        $location.search("q", $scope.query);
    }
    if (!$location.search().page) {
        $location.search("p", $scope.page);
    }
}

// -------------------------------------------------------

function bleveNewIndexMapping() {
    return {
        "types": {},
        "default_mapping": {
            "enabled": true,
            "dynamic": true
        },
        "type_field": "_type",
        "default_type": "_default",
        "default_analyzer": "standard",
        "default_datetime_parser": "dateTimeOptional",
        "default_field": "_all",
        "byte_array_converter": "json",
        "analysis": {
            "analyzers": {},
            "char_filters": {},
            "tokenizers": {},
            "token_filters": {},
            "token_maps": {}
        }
    }
};

function blevePIndexInitController(initKind, indexParams, indexUI,
    $scope, $http, $route, $routeParams, $location, $log, $sce, $uibModal) {
    if (initKind == "view") {
        $scope.viewOnly = true;
    }

    $scope.static_prefix = "/_p/ui/fts/static-bleve-mapping";

    $scope.indexTemplates = $scope.indexTemplates || {};
    $scope.indexTemplates["fulltext-index"] =
        $scope.static_prefix + "/partials/mapping/index-mapping.html";

    var mapping = bleveNewIndexMapping();
    if (indexParams &&
        indexParams.mapping) {
        mapping = indexParams.mapping;
    }

    var imc = initBleveIndexMappingController(
        $scope, $http, $log, $uibModal, mapping,
        {
            analyzerNames: null,
            dateTypeParserNames: null,
            byteArrayConverterNames: null,
            defaultFieldStore: false
        });

    $scope.bleveIndexMapping = function() {
        return imc.indexMapping();
    }
}

function blevePIndexDoneController(doneKind, indexParams, indexUI,
    $scope, $http, $route, $routeParams, $location, $log, $sce, $uibModal) {
    if (indexParams) {
        indexParams.mapping = $scope.bleveIndexMapping();
    }
}

angular.module(ftsAppName).
    controller('BleveAnalyzerModalCtrl',
               BleveAnalyzerModalCtrl_NS).
    controller('BleveCharFilterModalCtrl',
               BleveCharFilterModalCtrl_NS).
    controller('BleveTokenizerModalCtrl',
               BleveTokenizerModalCtrl_NS).
    controller('BleveTokenFilterModalCtrl',
               BleveTokenFilterModalCtrl_NS).
    controller('BleveWordListModalCtrl',
               BleveWordListModalCtrl_NS);

// ----------------------------------------------

function BleveAnalyzerModalCtrl_NS($scope, $uibModalInstance, $http,
                                   name, value, mapping, static_prefix) {
    return BleveAnalyzerModalCtrl($scope, $uibModalInstance,
                                  prefixedHttp($http, '/_p/' + ftsPrefix),
                                  name, value, mapping, static_prefix);
}

function BleveCharFilterModalCtrl_NS($scope, $uibModalInstance, $http,
                                     name, value, mapping, static_prefix) {
    return BleveCharFilterModalCtrl($scope, $uibModalInstance,
                                    prefixedHttp($http, '/_p/' + ftsPrefix),
                                    name, value, mapping, static_prefix);
}

function BleveTokenFilterModalCtrl_NS($scope, $uibModalInstance, $http,
                                      name, value, mapping, static_prefix) {
    return BleveTokenFilterModalCtrl($scope, $uibModalInstance,
                                     prefixedHttp($http, '/_p/' + ftsPrefix),
                                     name, value, mapping, static_prefix);
}

function BleveTokenizerModalCtrl_NS($scope, $uibModalInstance, $http,
                                    name, value, mapping, static_prefix) {
    return BleveTokenizerModalCtrl($scope, $uibModalInstance,
                                   prefixedHttp($http, '/_p/' + ftsPrefix),
                                   name, value, mapping, static_prefix);
}

function BleveWordListModalCtrl_NS($scope, $uibModalInstance,
                                   name, words, mapping, static_prefix) {
    return BleveWordListModalCtrl($scope, $uibModalInstance,
                                  name, words, mapping, static_prefix);
}

// ----------------------------------------------

function IndexNewCtrlFT($scope, $http, $route, $routeParams,
                        $location, $log, $sce, $uibModal, andThen) {
    $scope.indexDefs = null;

    $http.get('/api/index').
    success(function(data) {
        var indexDefs = $scope.indexDefs =
            data && data.indexDefs && data.indexDefs.indexDefs;

        var origIndexDef = indexDefs && indexDefs[$routeParams.indexName];

        var isAlias =
            ($routeParams.indexType == 'fulltext-alias') ||
            (origIndexDef && origIndexDef.type == 'fulltext-alias');
        if (isAlias) {
            // The aliasTargets will be the union of currently available
            // indexes (and aliases) and the existing targets of the alias.
            // Note that existing targets may have been deleted, but we
            // we will still offer them as options.
            $scope.aliasTargets = [];
            for (var indexName in indexDefs) {
                $scope.aliasTargets.push(indexName);
            }

            $scope.selectedTargetIndexes = [];
            if (origIndexDef) {
                var origTargets = (JSON.parse(origIndexDef.params) || {}).targets;
                for (var origTarget in origTargets) {
                    if (!indexDefs[origTarget]) {
                        $scope.aliasTargets.push(origTarget);
                    }

                    $scope.selectedTargetIndexes.push(origTarget);
                }
            }

            $scope.putIndexAlias =
                function(newIndexName,
                         newIndexType, newIndexParams,
                         newSourceType, newSourceName,
                         newSourceUUID, newSourceParams,
                         newPlanParams, prevIndexUUID,
                         selectedTargetIndexes) {
                    var aliasTargets = {};

                    for (var i = 0; i < selectedTargetIndexes.length; i++) {
                        var selectedTargetIndex = selectedTargetIndexes[i];

                        aliasTargets[selectedTargetIndex] = {};

                        var targetIndexDef = $scope.indexDefs[selectedTargetIndex];
                        if (targetIndexDef) {
                            aliasTargets[selectedTargetIndex].indexUUID =
                                targetIndexDef.uuid;
                        }
                    }

                    newIndexParams["fulltext-alias"] = {
                        "targets": JSON.stringify(aliasTargets)
                    };

                    $scope.putIndex(newIndexName,
                                    newIndexType, newIndexParams,
                                    newSourceType, newSourceName,
                                    newSourceUUID, newSourceParams,
                                    newPlanParams, prevIndexUUID);
                };
        }

        IndexNewCtrl($scope, $http, $route, $routeParams,
                     $location, $log, $sce, $uibModal);

        if (andThen) {
            andThen();
        }
    });
}

// ----------------------------------------------

function prefixedHttp($http, prefix, dataNoJSONify) {
    var needDecorate = {
        "get": true, "put": true, "post": true, "delete": true
    };

    var rv = {}
    for (var k in $http) {
        rv[k] = $http[k];
        if (needDecorate[k]) {
            decorate(k);
        }
    }
    return rv;

    function decorate(k) {
        var orig = rv[k];

        rv[k] = function(path, data, config) {
            config = config || {};
            config.mnHttp = config.mnHttp || {};
            config.mnHttp.isNotForm = true;

            return orig.call($http, prefix + path, data, config);
        }
    }
}

function errorMessage(errorMessageFull, code) {
    console.log("errorMessageFull", errorMessageFull, code);
    var a = (errorMessageFull || (code + "")).split("err: ");
    return a[a.length - 1];
}

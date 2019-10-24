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

var ftsAppName = 'fts';
var ftsPrefix = 'fts';

// -------------------------------------------------------

(function () {
  "use strict";

  angular
    .module(ftsAppName,
            ["ui.router", "mnPluggableUiRegistry", "mnJquery", "ngRoute", "ui.tree", "ngclipboard", "mnPermissions", "ng-sortable", "mnDetailStats"])
    .config(function($stateProvider, mnPluggableUiRegistryProvider, mnPermissionsProvider) {
      addFtsStates("app.admin.search");

      function addFtsStates(parent) {
        $stateProvider
          .state(parent, {
            abstract: true,
            views: {
              "main@app.admin": {
                controller: "IndexesCtrlFT_NS",
                templateUrl: '../_p/ui/fts/uia/fts_list.html'
              }
            },
            data: {
              title: "Full Text Search"
            }
          })
          .state(parent + '.fts_list', {
            url: '/fts_list?open',
            controller: 'IndexesCtrlFT_NS',
            templateUrl: '../_p/ui/fts/uia/fts_list.html'
          })
          .state(parent + '.fts_new', {
            url: '/fts_new/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_new.html'
              }
            },
            data: {
              title: "Add Index",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_new_alias', {
            url: '/fts_new_alias/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_new.html'
              }
            },
            data: {
              title: "Add Alias",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_edit', {
            url: '/fts_edit/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_new.html'
              }
            },
            data: {
              title: "Edit Index",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_edit_alias', {
            url: '/fts_edit_alias/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_new.html'
              }
            },
            data: {
              title: "Edit Alias",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_clone', {
            url: '/fts_clone/:indexName/_clone',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_new.html'
              }
            },
            data: {
              title: "Clone Index",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_clone_alias', {
            url: '/fts_clone_alias/:indexName/_clone',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_new.html'
              }
            },
            data: {
              title: "Clone Alias",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_search', {
            url: '/fts_search/:indexName/_search?q&p',
            reloadOnSearch: false,
            views: {
              "main@app.admin": {
                controller: 'IndexSearchCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_search.html'
              }
            },
            data: {
              title: "Search Results",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_details', {
            url: '/fts_details/:indexName',
            views: {
              "main@app.admin": {
                controller: 'IndexDetailsCtrlFT_NS',
                templateUrl: '../_p/ui/fts/uia/fts_details.html'
              }
            },
            data: {
              title: "Index Details",
              child: parent + '.fts_list'
            }
          });
      }

      mnPluggableUiRegistryProvider.registerConfig({
          name: 'Search',
          state: 'app.admin.search.fts_list',
          plugIn: 'workbenchTab',
          index: 2,
          responsiveHide: true,
          includedByState: 'app.admin.search',
          ngShow: 'rbac.cluster.settings.fts.read'
      });

      (["cluster.settings.fts!read", "cluster.settings.fts!write"])
        .forEach(mnPermissionsProvider.set);

      mnPermissionsProvider.setBucketSpecific(function(name) {
        return [
          "cluster.bucket[" + name + "].fts!write",
          "cluster.bucket[" + name + "].data!read"
        ];
      });
    });

  angular.module('mnAdmin').requires.push('fts');

  angular.module(ftsAppName).
        controller('IndexesCtrlFT_NS', IndexesCtrlFT_NS).
        controller('IndexCtrlFT_NS', IndexCtrlFT_NS).
        controller('IndexNewCtrlFT_NS', IndexNewCtrlFT_NS).
        controller('IndexSearchCtrlFT_NS', IndexSearchCtrlFT_NS).
        controller('IndexDetailsCtrlFT_NS', IndexDetailsCtrlFT_NS);
}());

// ----------------------------------------------

var updateDocCountIntervalMS = 5000;

function IndexesCtrlFT_NS($scope, $http, $state, $stateParams,
                          $log, $sce, $location, mnPoolDefault, mnPermissions) {
    var $routeParams = $stateParams;

    var http = prefixedHttp($http, '../_p/' + ftsPrefix);

    $scope.ftsChecking = true;
    $scope.ftsAvailable = false;
    $scope.ftsCheckError = "";
    $scope.ftsNodes = [];

    mnPermissions.check()

    try {
        $scope.permsCluster = mnPermissions.export.cluster;
    } catch (e) {
    }

    http.get("/api/runtime").
    then(function(response) {
      $scope.ftsAvailable = true;
      $scope.ftsChecking = false;
    }, function(response) {
      $scope.ftsChecking = false;
      // if we got a 404, there is no fts service on this node.
      // let's go through the list of nodes
      // and see which ones have a fts service
      if (response.status == 404) {
        mnPoolDefault.get().then(function(value){
          $scope.ftsNodes = mnPoolDefault.getUrlsRunningService(value.nodes, "fts");
        });
      } else {
        // some other error to show
        $scope.ftsCheckError = response.data;
      }
    });

    $scope.detailsOpened = {};
    if ($state.params && $state.params.open) {
        $scope.detailsOpened[$state.params.open] = true;
    }

    $scope.detailsOpenedJSON = {};
    $scope.detailsOpenedJSONCurl = {};

    $scope.escapeCmdLineParam = function(s) {
        // Transform single quotes (') into '"'"', so...
        //   curl http://foo -d '{{escapeCmdLineParam(stringWithQuotes)}}'
        // where...
        //   stringWithQuotes == "he said 'hi' twice"
        // will result in...
        //   curl http://foo -d 'he said '"'"'hi'"'"' twice'
        return s.replace(/\'/g, '\'"\'"\'');
    }

    $scope.searchInputs = {};

    var done = false;

    $scope.indexViewController = function($scope, $route, $state, $uibModal) {
        var stateParams = {indexName: $scope.indexName};

        $scope.jsonDetails = false;
        $scope.curlDetails = false;

        var rv = IndexCtrlFT_NS($scope, $http, $route, stateParams, $state,
                                $location, $log, $sce, $uibModal);

        var loadDocCount = $scope.loadDocCount;

        function checkRetStatus(code) {
            if (code == 403) {
                // http status for FORBIDDEN
                return false;
            } else if (code == 400) {
                // http status for BAD REQUEST
                return false;
            }
            return true;
        }

        function updateDocCount() {
            if (!done) {
                if (loadDocCount(checkRetStatus)) {
                    setTimeout(updateDocCount, updateDocCountIntervalMS);
                }
            }
        }

        setTimeout(updateDocCount, updateDocCountIntervalMS);

        if ($scope.indexDef.sourceName) {
            mnPermissions.check()
        }

        return rv;
    }

    var rv = IndexesCtrl($scope, http, $routeParams, $log, $sce, $location);

    $scope.$on('$stateChangeStart', function() {
        done = true;
    });

    $scope.expando = function(indexName) {
        $scope.detailsOpened[indexName] = !$scope.detailsOpened[indexName];

        // The timeout gives angular some time to create the input control.
        if ($scope.detailsOpened[indexName]) {
            setTimeout(function() {
                document.getElementById('query_bar_input_' + indexName).focus()
            }, 100);
        }
    }

    return rv;
}

// -------------------------------------------------------

function IndexCtrlFT_NS($scope, $http, $route, $stateParams, $state,
                        $location, $log, $sce, $uibModal) {
    var $routeParams = $stateParams;

    var http = prefixedHttp($http, '../_p/' + ftsPrefix);

    $scope.progressPct = "";
    $scope.sourceDocCount = "";
    $scope.httpStatus = 200;    // OK
    $scope.showHiddenUI = false;

    $scope.loadDocCount = function(callback) {
        $scope.loadIndexDocCount();
        $scope.loadSourceDocCount();
        if (callback) {
            return callback($scope.httpStatus);
        }
        return true;
    }

    $scope.loadSourceDocCount = function() {
        $scope.sourceDocCount = "...";
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        http.get('/api/stats/sourceStats/' + $scope.indexName).
        then(function(response) {
            var data = response.data;
            if (data) {
                $scope.sourceDocCount = data.docCount;
                updateProgressPct();
            }
        }, function(response) {
            $scope.sourceDocCount = "error"
            $scope.httpStatus = response.status;
            updateProgressPct();
        });
    }

    $scope.checkHiddenUI = function() {
        $scope.showHiddenUI = false;

        http.get('/api/conciseOptions').
        then(function(response) {
            hideUI = response.data.hideUI;
            if (hideUI === "false") {
                $scope.showHiddenUI = true;
            }
        });
    }

    http.get("/api/managerMeta").
    then (function(response) {
        var data = response.data;
        $scope.meta = data;

        if (!$scope.indexDef) {
            $http.get('/api/index/' + $scope.indexName).then(function(response) {
                $scope.indexDef = response.data.indexDef;
                initQueryHelp();
            })
        } else {
            initQueryHelp();
        }

        function initQueryHelp() {
            var indexDefType = ($scope.indexDef && $scope.indexDef.type);

            $scope.queryHelp = $scope.meta.indexTypes[indexDefType].queryHelp;
            // this call to trustAsHtml is safe provided we trust
            // the registered pindex implementations
            $scope.queryHelpSafe = $sce.trustAsHtml($scope.queryHelp);
        }
    });

    function updateProgressPct() {
        var i = parseInt($scope.indexDocCount);
        var s = parseInt($scope.sourceDocCount);
        if (s > 0) {
            if (i >= 0) {
                var prog = ((1.0 * i) / s) * 100.0;
                $scope.progressPct = prog.toPrecision(4);
            } else {
                $scope.progressPct = 0.0;
            }
        } else if (s == 0 && i == 0) {
            $scope.progressPct = 100.0;
        } else {
            $scope.progressPct = "--";
        }
    }

    IndexCtrl($scope, http, $route, $routeParams,
              $location, $log, $sce, $uibModal);

    if ($scope.tab === "summary") {
        $scope.loadDocCount();
        $scope.checkHiddenUI();
    }

    ftsServiceHostPort($scope, $http, $location);
}

// -------------------------------------------------------

function IndexNewCtrlFT_NS($scope, $http, $route, $state, $stateParams,
                           $location, $log, $sce, $uibModal,
                           $q, mnBucketsService, mnPoolDefault) {
    mnBucketsService.getBucketsByType(true).
        then(initWithBuckets,
             function(err) {
                 // Possible RBAC issue listing buckets or other error.
                 console.log("mnBucketsService.getBucketsByType failed", err);
                 initWithBuckets([]);
             });

    function initWithBuckets(buckets) {
        $scope.ftsDocConfig = {};
        $scope.ftsStore = {};

        $scope.ftsNodes = [];
        mnPoolDefault.get().then(function(value){
          $scope.ftsNodes = mnPoolDefault.getUrlsRunningService(value.nodes, "fts");
        });

        $scope.buckets = buckets;
        $scope.bucketNames = [];
        for (var i = 0; i < buckets.length; i++) {
            // Add membase buckets only to list of index-able buckets
            if (buckets[i].bucketType === "membase") {
                $scope.bucketNames.push(buckets[i].name);
            }
        }

        $scope.indexEditorPreview = {};
        $scope.indexEditorPreview["fulltext-index"] = null;
        $scope.indexEditorPreview["fulltext-alias"] = null;

        var $routeParams = $stateParams;

        var $locationRewrite = {
            host: $location.host,
            path: function(p) {
                if (!p) {
                    return $location.path();
                }
                var newIndexName = p.replace(/^\/indexes\//, "");
                $state.go("app.admin.search.fts_list", { open: newIndexName });
            }
        }

        $scope.indexStoreOptions = ["Version 5.0 (Moss)", "Version 6.0 (Scorch)"];
        $scope.indexStoreOption = $scope.indexStoreOptions[1];

        IndexNewCtrlFT($scope,
                       prefixedHttp($http, '../_p/' + ftsPrefix),
                       $route, $routeParams,
                       $locationRewrite, $log, $sce, $uibModal,
                       finishIndexNewCtrlFTInit);

        function finishIndexNewCtrlFTInit() {
            var putIndexOrig = $scope.putIndex;

            $scope.typeIdentifierChanged = function() {
              if ($scope.ftsDocConfig.mode == "type_field" && !$scope.ftsDocConfig.type_field) {
                $scope.ftsDocConfig.type_field = "type";
              }
            }

            $scope.indexStoreOptionChanged = function() {
                if ($scope.newIndexParams) {
                    switch($scope.indexStoreOption) {
                        case $scope.indexStoreOptions[0]:
                            $scope.ftsStore.kvStoreName = "mossStore";
                            $scope.ftsStore.indexType = "upside_down";
                            break;
                        case $scope.indexStoreOptions[1]:
                            $scope.ftsStore.indexType = "scorch";
                            delete $scope.ftsStore.kvStoreName;
                            break;
                    }
                }
            }

            $scope.prepareFTSIndex = function(newIndexName,
                                              newIndexType, newIndexParams,
                                              newSourceType, newSourceName,
                                              newSourceUUID, newSourceParams,
                                              newPlanParams, prevIndexUUID,
                                              readOnly) {
                var errorFields = {};
                var errs = [];

                // type identifier validation/cleanup
                switch ($scope.ftsDocConfig.mode) {
                  case "type_field":
                    if (!$scope.ftsDocConfig.type_field) {
                        errs.push("type field is required");
                    }
                    if (!readOnly) {
                        delete $scope.ftsDocConfig.docid_prefix_delim;
                        delete $scope.ftsDocConfig.docid_regexp;
                    }
                  break;

                  case "docid_prefix":
                    if (!$scope.ftsDocConfig.docid_prefix_delim) {
                        errs.push("Doc ID separator is required");
                    }
                    if (!readOnly) {
                        delete $scope.ftsDocConfig.type_field;
                        delete $scope.ftsDocConfig.docid_regexp;
                    }
                  break;

                  case "docid_regexp":
                    if (!$scope.ftsDocConfig.docid_regexp) {
                        errs.push("Doc ID regexp is required");
                    }
                    if (!readOnly) {
                        delete $scope.ftsDocConfig.type_field;
                        delete $scope.ftsDocConfig.docid_prefix_delim;
                    }
                  break;
                }

                // stringify our doc_config and set that into newIndexParams
                try {
                    newIndexParams['fulltext-index'].doc_config = JSON.stringify($scope.ftsDocConfig);
                } catch (e) {}

                try {
                    newIndexParams['fulltext-index'].store = JSON.stringify($scope.ftsStore);
                } catch (e) {}

                if (!newIndexName) {
                    errorFields["indexName"] = true;
                    errs.push("index name is required");
                } else if ($scope.meta &&
                           $scope.meta.indexNameRE &&
                           !newIndexName.match($scope.meta.indexNameRE)) {
                    errorFields["indexName"] = true;
                    errs.push("index name '" + newIndexName + "'" +
                              " must start with an alphabetic character, and" +
                              " must only use alphanumeric or '-' or '_' characters");
                }

                if (!newSourceName) {
                    errorFields["sourceName"] = true;
                    if (newIndexType == "fulltext-index") {
                        errs.push("source (bucket) needs to be selected");
                    }
                }

                if (newIndexType != "fulltext-alias") {
                    if (newPlanParams) {
                        try {
                            var numReplicas = $scope.numReplicas;
                            if (numReplicas >= 0 ) {
                                var newPlanParamsObj = JSON.parse(newPlanParams);
                                newPlanParamsObj["numReplicas"] = numReplicas;
                                newPlanParams = JSON.stringify(newPlanParamsObj, undefined, 2);
                            }

                            var numPIndexes = $scope.numPIndexes;
                            if (numPIndexes > 0) {
                                var newPlanParamsObj = JSON.parse(newPlanParams);
                                newPlanParamsObj["indexPartitions"] = numPIndexes;
                                newPlanParamsObj["maxPartitionsPerPIndex"] = Math.ceil($scope.vbuckets / numPIndexes);
                                newPlanParams = JSON.stringify(newPlanParamsObj, undefined, 2);
                            } else {
                                errs.push("Index Partitions cannot be less than 1");
                            }
                        } catch (e) {
                            errs.push("exception: " + e);
                        }
                    }
                }

                if (errs.length > 0) {
                    var errorMessage =
                        (errs.length > 1 ? "errors: " : "error: ") + errs.join("; ");
                    return {
                        errorFields: errorFields,
                        errorMessage: errorMessage,
                    }
                }

                if (!newSourceUUID) {
                    for (var i = 0; i < buckets.length; i++) {
                        if (newSourceName == buckets[i].name) {
                            newSourceUUID = buckets[i].uuid;
                        }
                    }
                }

                return {
                    newSourceUUID: newSourceUUID,
                    newPlanParams: newPlanParams
                }
            }

            $scope.putIndex = function(newIndexName,
                                       newIndexType, newIndexParams,
                                       newSourceType, newSourceName,
                                       newSourceUUID, newSourceParams,
                                       newPlanParams, prevIndexUUID) {
                $scope.errorFields = {};
                $scope.errorMessage = null;
                $scope.errorMessageFull = null;

                var rv = $scope.prepareFTSIndex(newIndexName,
                                                newIndexType, newIndexParams,
                                                newSourceType, newSourceName,
                                                newSourceUUID, newSourceParams,
                                                newPlanParams, prevIndexUUID)
                if (rv.errorFields || rv.errorMessage) {
                    $scope.errorFields = rv.errorFields;
                    $scope.errorMessage = rv.errorMessage;
                    return
                }

                newSourceUUID = rv.newSourceUUID;
                newPlanParams = rv.newPlanParams;

                return putIndexOrig(newIndexName,
                                    newIndexType, newIndexParams,
                                    newSourceType, newSourceName,
                                    newSourceUUID, newSourceParams,
                                    newPlanParams, prevIndexUUID);
            };
        }
    }
}

// -------------------------------------------------------

function IndexSearchCtrlFT_NS($scope, $http, $stateParams, $log, $sce,
                              $location, mnPermissions) {
  var $httpPrefixed = prefixedHttp($http, '../_p/' + ftsPrefix, true);

  var $routeParams = $stateParams;

  $scope.indexName = $stateParams.indexName;

  $httpPrefixed.get('/api/index/' + $scope.indexName).then(function(response) {
    $scope.indexDef = response.data.indexDef;
    if ($scope.indexDef &&
        $scope.indexDef.sourceType == "couchbase") {
      mnPermissions.check()
    }

    try {
        $scope.permsCluster = mnPermissions.export.cluster;
    } catch (e) {
    }

    $scope.decorateSearchHit = function(hit) {
      hit.docIDLink = null;
      try {
        if (($scope.indexDef &&
             $scope.indexDef.sourceType == "couchbase" &&
             $scope.permsCluster.bucket[$scope.indexDef.sourceName] &&
             ($scope.permsCluster.bucket[$scope.indexDef.sourceName].data.read ||
              $scope.permsCluster.bucket[$scope.indexDef.sourceName].data.docs.read)) ||
            $scope.permsCluster.bucket["*"].data.read) {
          hit.docIDLink = "../_p/fts/api/nsSearchResultRedirect/" + hit.index + "/" + hit.id;
        }
      } catch (e) {}
    };

    $scope.static_base = "../_p/ui/fts";

    $scope.query = $routeParams.q || "";
    $scope.page = $routeParams.p || 1;

    if (!$location.search().q) {
        $location.search("q", $scope.query);
    }
    if (!$location.search().p) {
        $location.search("p", $scope.page);
    }

    QueryCtrl($scope, $httpPrefixed, $routeParams, $log, $sce, $location);

    ftsServiceHostPort($scope, $http, $location);

    setTimeout(function() {
        document.getElementById("query_bar_input").focus();
    }, 100);
  })
}

// -------------------------------------------------------

function IndexDetailsCtrlFT_NS($scope, $http, $route, $stateParams,
                               $location, $log, $sce, $uibModal) {
    var $routeParams = $stateParams;

    var http = prefixedHttp($http, '/../_p/' + ftsPrefix);

    $scope.indexName = $stateParams.indexName;

    $scope.jsonDetails = false;
    $scope.curlDetails = false;

    $scope.indexTab = 1;

    $scope.setIndexTab = function(newTab) {
        $scope.indexTab = newTab;
    }

    $scope.isIndexTab = function(tabNum) {
        return $scope.indexTab === tabNum
    }

    IndexCtrl($scope, http, $route, $routeParams,
              $location, $log, $sce, $uibModal);
}

// -------------------------------------------------------

function bleveNewIndexMapping() {
    return {
        "types": {},
        "default_mapping": {
            "enabled": true,
            "dynamic": true
        },
        "default_type": "_default",
        "default_analyzer": "standard",
        "default_datetime_parser": "dateTimeOptional",
        "default_field": "_all",
        "analysis": {
            "analyzers": {},
            "char_filters": {},
            "tokenizers": {},
            "token_filters": {},
            "token_maps": {},
            "date_time_parsers": {}
        },
        "store_dynamic": false,
        "index_dynamic": true
    }
};

function blevePIndexInitController(initKind, indexParams, indexUI,
    $scope, $http, $route, $routeParams, $location, $log, $sce, $uibModal) {

    if (initKind == "edit" || initKind == "create") {
        $scope.replicaOptions = [0];
        $scope.numReplicas = $scope.replicaOptions[0];
        $scope.vbuckets = 1024;
        $scope.numPIndexes = 0;
        $http.get('/api/conciseOptions').
        then(function(response) {
            var maxReplicasAllowed = parseInt(response.data.maxReplicasAllowed);
            $scope.replicaOptions = [];
            for (var i = 0; i <= maxReplicasAllowed; i++) {
                $scope.replicaOptions.push(i);
            }

            if (response.data.bucketTypesAllowed != "") {
                var bucketTypesAllowed = response.data.bucketTypesAllowed.split(":");
                var bucketNamesAllowed = [];
                for (var i = 0; i < $scope.buckets.length; i++) {
                    if (bucketTypesAllowed.includes($scope.buckets[i].bucketType)) {
                        bucketNamesAllowed.push($scope.buckets[i].name);
                    }
                }
                // Update bucketNames based on what's supported.
                $scope.bucketNames = bucketNamesAllowed;

                if ($scope.bucketNames.length == 0) {
                    $scope.errorMessage = "No buckets available to access!";
                }
            }

            if (response.data.vbuckets != "") {
                $scope.vbuckets = parseInt(response.data.vbuckets)
            }

            if ($scope.newIndexType != "fulltext-alias") {
                if ($scope.newPlanParams) {
                    try {
                        var newPlanParamsObj = JSON.parse($scope.newPlanParams);

                        $scope.numReplicas = $scope.replicaOptions[newPlanParamsObj["numReplicas"] || 0];
                        delete newPlanParamsObj["numReplicas"];
                        $scope.newPlanParams = JSON.stringify(newPlanParamsObj, undefined, 2);

                        if (angular.isDefined(newPlanParamsObj["indexPartitions"])) {
                            $scope.numPIndexes = newPlanParamsObj["indexPartitions"];
                        }
                        if ($scope.numPIndexes == 0) {
                            var maxPartitionsPerPIndex = newPlanParamsObj["maxPartitionsPerPIndex"];
                            $scope.numPIndexes = Math.ceil($scope.vbuckets / maxPartitionsPerPIndex);
                        }
                    } catch (e) {
                        console.log("blevePIndexInitController numPlanParams", initKind, e)
                    }
                }
            } else {
                $scope.newPlanParams = "{}"
            }
        });
    }

    try {
        $scope.ftsDocConfig = JSON.parse(JSON.stringify($scope.meta.sourceTypes["fulltext-index"].startSample.doc_config))
    } catch (e) {}

    try {
        $scope.ftsDocConfig = JSON.parse($scope.newIndexParams['fulltext-index'].doc_config)
    } catch (e) {}

    try {
        $scope.ftsStore = JSON.parse($scope.newIndexParams['fulltext-index'].store);
        if ($scope.ftsStore.kvStoreName == "mossStore" &&
                $scope.ftsStore.indexType == "upside_down") {
            $scope.indexStoreOption = $scope.indexStoreOptions[0];
        } else if ($scope.ftsStore.indexType == "scorch") {
            $scope.indexStoreOption = $scope.indexStoreOptions[1];
        }
    } catch (e) {}

    if (initKind == "view") {
        $scope.viewOnly = true;
    }

    $scope.static_prefix = "../_p/ui/fts/static-bleve-mapping";

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

    var done = false;
    var previewPrev = "";

    function updatePreview() {
        if (done) {
            return;
        }

        if ($scope.prepareIndex &&
            $scope.prepareFTSIndex &&
            $scope.indexEditorPreview) {
            if ($scope.newIndexType == "fulltext-alias") {
                var aliasTargets = {};

                for (var i = 0; i < $scope.selectedTargetIndexes.length; i++) {
                    var selectedTargetIndex = $scope.selectedTargetIndexes[i];

                    aliasTargets[selectedTargetIndex] = {};
                }

                $scope.newIndexParams["fulltext-alias"] = {
                    "targets": JSON.stringify(aliasTargets)
                };
            }

            var rv = $scope.prepareFTSIndex(
                $scope.newIndexName,
                $scope.newIndexType, $scope.newIndexParams,
                $scope.newSourceType, $scope.newSourceName, $scope.newSourceUUID, $scope.newSourceParams,
                $scope.newPlanParams, $scope.prevIndexUUID,
                true);
            if (!rv.errorFields && !rv.errorMessage) {
                var newSourceUUID = rv.newSourceUUID;
                var newPlanParams = rv.newPlanParams;

                var rv = $scope.prepareIndex(
                    $scope.newIndexName,
                    $scope.newIndexType, $scope.newIndexParams,
                    $scope.newSourceType, $scope.newSourceName, newSourceUUID, $scope.newSourceParams,
                    newPlanParams, $scope.prevIndexUUID);
                if (rv.indexDef) {
                    var preview = JSON.stringify(rv.indexDef, null, 1);
                    if (preview != previewPrev) {
                        $scope.indexEditorPreview[$scope.newIndexType] = preview;
                        previewPrev = preview;
                    }
                }
            }

            setTimeout(updatePreview, bleveUpdatePreviewTimeoutMS);
        }
    }

    setTimeout(updatePreview, bleveUpdatePreviewTimeoutMS);

    $scope.indexDefChanged = function(origIndexDef) {
        var rv = $scope.prepareFTSIndex(
            $scope.newIndexName,
            $scope.newIndexType, $scope.newIndexParams,
            $scope.newSourceType, $scope.newSourceName, $scope.newSourceUUID, $scope.newSourceParams,
            $scope.newPlanParams, $scope.prevIndexUUID,
            true);
        if (!rv.errorFields && !rv.errorMessage) {
            var newSourceUUID = rv.newSourceUUID;
            var newPlanParams = rv.newPlanParams;
            var rv = $scope.prepareIndex(
                $scope.newIndexName,
                $scope.newIndexType, $scope.newIndexParams,
                $scope.newSourceType, $scope.newSourceName, newSourceUUID, $scope.newSourceParams,
                newPlanParams, $scope.prevIndexUUID);

            try {
                // Add an empty "analysis" section if no analysis elements defined.
                if (!angular.isDefined(rv.indexDef["params"]["mapping"]["analysis"])) {
                    rv.indexDef["params"]["mapping"]["analysis"] = {};
                }
                // Delete "numReplicas" if set to 0.
                if (angular.isDefined(rv.indexDef["planParams"]["numReplicas"]) &&
                    rv.indexDef["planParams"]["numReplicas"] == 0) {
                    delete rv.indexDef["planParams"]["numReplicas"]
                }
            } catch (e) {
            }

            if (angular.equals(origIndexDef, rv.indexDef)) {
                return false;
            }
        } // Else could not retrieve the index definition, permit the update.
        return true;
    };

    $scope.$on('$stateChangeStart', function() {
        done = true;
    });
}

var bleveUpdatePreviewTimeoutMS = 1000;

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
               BleveWordListModalCtrl_NS).
    controller('BleveDatetimeParserModalCtrl',
               BleveDatetimeParserModalCtrl_NS);

// ----------------------------------------------

function BleveAnalyzerModalCtrl_NS($scope, $uibModalInstance, $http,
                                   name, value, mapping, static_prefix) {
    return BleveAnalyzerModalCtrl($scope, $uibModalInstance,
                                  prefixedHttp($http, '../_p/' + ftsPrefix),
                                  name, value, mapping, static_prefix);
}

function BleveCharFilterModalCtrl_NS($scope, $uibModalInstance, $http,
                                     name, value, mapping, static_prefix) {
    return BleveCharFilterModalCtrl($scope, $uibModalInstance,
                                    prefixedHttp($http, '../_p/' + ftsPrefix),
                                    name, value, mapping, static_prefix);
}

function BleveTokenFilterModalCtrl_NS($scope, $uibModalInstance, $http,
                                      name, value, mapping, static_prefix) {
    return BleveTokenFilterModalCtrl($scope, $uibModalInstance,
                                     prefixedHttp($http, '../_p/' + ftsPrefix),
                                     name, value, mapping, static_prefix);
}

function BleveTokenizerModalCtrl_NS($scope, $uibModalInstance, $http,
                                    name, value, mapping, static_prefix) {
    return BleveTokenizerModalCtrl($scope, $uibModalInstance,
                                   prefixedHttp($http, '../_p/' + ftsPrefix),
                                   name, value, mapping, static_prefix);
}

function BleveWordListModalCtrl_NS($scope, $uibModalInstance,
                                   name, words, mapping, static_prefix) {
    return BleveWordListModalCtrl($scope, $uibModalInstance,
                                  name, words, mapping, static_prefix);
}

function BleveDatetimeParserModalCtrl_NS($scope, $uibModalInstance,
                                         name, layouts, mapping, static_prefix) {
    return BleveDatetimeParserModalCtrl($scope, $uibModalInstance,
                                        name, layouts, mapping, static_prefix);
}

// ----------------------------------------------

function IndexNewCtrlFT($scope, $http, $route, $routeParams,
                        $location, $log, $sce, $uibModal, andThen) {
    $scope.indexDefs = null;
    $scope.origIndexDef = null;

    $http.get('/api/index').
    then(function(response) {
        var data = response.data;

        var indexDefs = $scope.indexDefs =
            data && data.indexDefs && data.indexDefs.indexDefs;

        var origIndexDef = $scope.origIndexDef =
            indexDefs && indexDefs[$routeParams.indexName];

        var isAlias =
            ($routeParams.indexType == 'fulltext-alias') ||
            (origIndexDef && origIndexDef.type == 'fulltext-alias');
        if (isAlias) {
            var isEdit = $location.path().match(/_edit$/);

            // The aliasTargets will be the union of currently available
            // indexes (and aliases) and the existing targets of the alias.
            // Note that existing targets may have been deleted, but we
            // we will still offer them as options.
            $scope.aliasTargets = [];
            for (var indexName in indexDefs) {
                if (!isEdit || indexName != $routeParams.indexName) {
                    $scope.aliasTargets.push(indexName);
                }
            }

            $scope.selectedTargetIndexes = [];
            if (origIndexDef) {
                var origTargets = (origIndexDef.params || {}).targets;
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
    }, function(response) {
        $scope.errorMessage = errorMessage(response.data, response.code);
        $scope.errorMessageFull = response.data;
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
    if (typeof errorMessageFull == "object") {
        if (code == 403) {
            rv = errorMessageFull.message + ": ";
            for (var x in errorMessageFull.permissions) {
                rv += errorMessageFull.permissions[x];
            }
            return rv;
        }
        errorMessageFull = errorMessageFull.error
    }
    console.log("errorMessageFull", errorMessageFull, code);
    var a = (errorMessageFull || (code + "")).split("err: ");
    return a[a.length - 1];
}

// -------------------------------------------------------

function ftsServiceHostPort($scope, $http, $location) {
    $scope.hostPort = $location.host() + ":FTS_PORT";

    $http.get("/pools/default/nodeServices").then(function(resp) {
        var nodes = resp.data.nodesExt;
        for (var i = 0; i < nodes.length; i++) {
            if (nodes[i].services && nodes[i].services.fts) {
                var host = $location.host();
                if (angular.isDefined(nodes[i].hostname)) {
                    host = nodes[i].hostname;
                }
                $scope.hostPort = host + ":" + nodes[i].services.fts;
                return;
            }
        }
    });
}

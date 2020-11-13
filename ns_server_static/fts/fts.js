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
import angular from "/ui/web_modules/angular.js";

import uiRouter from "/ui/web_modules/@uirouter/angularjs.js";
import uiCodemirror from "/ui/libs/angular-ui-codemirror.js";
import CodeMirror from "/ui/web_modules/codemirror.js";
import ngClipboard from "/ui/libs/ngclipboard.js";
import ngSortable from "/ui/libs/angular-legacy-sortable.js";
import mnPermissions from "/ui/app/components/mn_permissions.js";
import mnFooterStatsController from "/ui/app/mn_admin/mn_gsi_footer_controller.js";
import mnStatisticsNewService from "/ui/app/mn_admin/mn_statistics_service.js";
import mnDocumentsEditingService from "/ui/app/mn_admin/mn_documents_editing_service.js";
import mnDocumentsListService from "/ui/app/mn_admin/mn_documents_list_service.js";

import BleveAnalyzerModalCtrl from "/_p/ui/fts/static-bleve-mapping/js/mapping/analysis-analyzer.js";
import BleveCharFilterModalCtrl from "/_p/ui/fts/static-bleve-mapping/js/mapping/analysis-charfilter.js";
import BleveDatetimeParserModalCtrl from "/_p/ui/fts/static-bleve-mapping/js/mapping/analysis-datetimeparser.js";
import BleveTokenFilterModalCtrl from "/_p/ui/fts/static-bleve-mapping/js/mapping/analysis-tokenfilter.js";
import BleveTokenizerModalCtrl from "/_p/ui/fts/static-bleve-mapping/js/mapping/analysis-tokenizer.js";
import BleveWordListModalCtrl from "/_p/ui/fts/static-bleve-mapping/js/mapping/analysis-wordlist.js";
import initBleveIndexMappingController from "/_p/ui/fts/static-bleve-mapping/js/mapping/index-mapping.js";

import {IndexesCtrl, IndexCtrl, IndexNewCtrl} from "/_p/ui/fts/static/index.js";
import QueryCtrl from "/_p/ui/fts/static/query.js";
import uiTree from "/ui/web_modules/angular-ui-tree.js";

import qwDocEditorService from "/_p/ui/query/qw_doc_editor_service.js";

import {initCodeMirrorActiveLine} from "/_p/ui/fts/codemirror-active-line.js";
import {newParsedDocs} from "/_p/ui/fts/fts_easy_parse.js";
import {newEditFields, newEditField} from "/_p/ui/fts/fts_easy_field.js";
import {newEasyMappings, newEasyMapping} from "/_p/ui/fts/fts_easy_mapping.js";

export default ftsAppName;
export {errorMessage, blevePIndexInitController, blevePIndexDoneController};

angular
    .module(ftsAppName,
            [uiRouter, ngClipboard, mnPermissions, uiTree, ngSortable, mnStatisticsNewService, qwDocEditorService,
                uiCodemirror, mnDocumentsEditingService, mnDocumentsListService])
    .config(function($stateProvider) {
      addFtsStates("app.admin.search");

      function addFtsStates(parent) {
        $stateProvider
          .state(parent, {
            url: '/fts',
            abstract: true,
            views: {
              "main@app.admin": {
                controller: "IndexesCtrlFT_NS",
                templateUrl: '../_p/ui/fts/fts_list.html'
              }
            },
            params: {
              footerBucket: {
                value: null,
                dynamic: true
              }
            },
            data: {
              title: "Full Text Search"
            }
          })
          .state(parent + '.fts_list', {
            url: '/fts_list?open',
            controller: 'IndexesCtrlFT_NS',
            templateUrl: '../_p/ui/fts/fts_list.html'
          })
          .state(parent + '.fts_new', {
            url: '/fts_new/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/fts_new.html'
              }
            },
            data: {
              title: "Add Index",
              child: parent + '.fts_list'
            }
          })
            .state(parent + '.fts_new_easy', {
                url: '/fts_new_easy/?indexType&sourceType',
                views: {
                    "main@app.admin": {
                        controller: 'IndexNewCtrlFTEasy_NS',
                        templateUrl: '../_p/ui/fts/fts_new_easy.html'
                    }
                },
                data: {
                    title: "Quick Index",
                    child: parent + '.fts_list'
                }
            })
          .state(parent + '.fts_new_alias', {
            url: '/fts_new_alias/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/fts_new.html'
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
                templateUrl: '../_p/ui/fts/fts_new.html'
              }
            },
            data: {
              title: "Edit Index",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_edit_easy', {
            url: '/fts_edit_easy/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFTEasy_NS',
                templateUrl: '../_p/ui/fts/fts_new_easy.html'
              }
            },
            data: {
              title: "Edit Quick Index",
              child: parent + '.fts_list'
            }
          })
          .state(parent + '.fts_edit_alias', {
            url: '/fts_edit_alias/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                templateUrl: '../_p/ui/fts/fts_new.html'
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
                templateUrl: '../_p/ui/fts/fts_new.html'
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
                templateUrl: '../_p/ui/fts/fts_new.html'
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
                templateUrl: '../_p/ui/fts/fts_search.html'
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
                templateUrl: '../_p/ui/fts/fts_details.html'
              }
            },
            data: {
              title: "Index Details",
              child: parent + '.fts_list'
            }
          });
      }
    });

  angular.module(ftsAppName).
        controller('mnFooterStatsController', mnFooterStatsController).
        controller('IndexesCtrlFT_NS', IndexesCtrlFT_NS).
        controller('IndexCtrlFT_NS', IndexCtrlFT_NS).
        controller('IndexNewCtrlFT_NS', IndexNewCtrlFT_NS).
        controller('IndexNewCtrlFTEasy_NS', IndexNewCtrlFTEasy_NS).
        controller('IndexSearchCtrlFT_NS', IndexSearchCtrlFT_NS).
        controller('IndexDetailsCtrlFT_NS', IndexDetailsCtrlFT_NS);

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

    $scope.indexViewController = function($scope, $state, $uibModal) {
        var stateParams = {indexName: $scope.indexName};

        $scope.jsonDetails = false;
        $scope.curlDetails = false;

        var rv = IndexCtrlFT_NS($scope, $http, stateParams, $state,
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

    $scope.showEasyMode = function(name, params) {
        let prevHash = params.easy_mode_hash;
        let newHash = hashParams(params);
        return prevHash == newHash;
    }

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

function IndexCtrlFT_NS($scope, $http, $stateParams, $state,
                        $location, $log, $sce, $uibModal) {
    var $routeParams = $stateParams;

    var http = prefixedHttp($http, '../_p/' + ftsPrefix);

    $scope.progressPct = "--";
    $scope.numMutationsToIndex = "";
    $scope.currSeqReceived = "";
    $scope.lastCurrSeqReceived = "";
    $scope.httpStatus = 200;    // OK
    $scope.showHiddenUI = false;

    $scope.loadDocCount = function(callback) {
        $scope.loadIndexDocCount();
        $scope.loadNumMutationsToIndex();
        if (callback) {
            return callback($scope.httpStatus);
        }
        return true;
    }

    $scope.loadNumMutationsToIndex = function() {
        $scope.numMutationsToIndex = "...";
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        http.get('/api/nsstats').
        then(function(response) {
            var data = response.data;
            if (data) {
                $scope.numMutationsToIndex =
                    data[$scope.indexDef.sourceName + ":" + $scope.indexName + ":num_mutations_to_index"];
                $scope.currSeqReceived =
                    data[$scope.indexDef.sourceName + ":" + $scope.indexName + ":curr_seq_received"];
                updateProgressPct();
            }
        }, function(response) {
            $scope.numMutationsToIndex = "error";
            $scope.httpStatus = response.Status;
            updateProgressPct();
        });
    }

    $scope.checkHiddenUI = function() {
        $scope.showHiddenUI = false;

        http.get('/api/conciseOptions').
        then(function(response) {
            let hideUI = response.data.hideUI;
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
        var x = parseInt($scope.numMutationsToIndex);

        if (i == 0 && x == 0) {
            let prog = 100.0;
            $scope.progressPct = prog.toPrecision(4);
        } else if (x > 0 && i >= 0) {
            let prog = ((1.0 * i) / (i + x)) * 100.0;
            $scope.progressPct = prog.toPrecision(4);
        } else {
            // num_mutations_to_index either zero or unavailable!
            var lastS = parseInt($scope.lastCurrSeqReceived);
            if (lastS >= 0) {
                // determine progressPct relative to the current
                // currSeqReceived and the lastCurrSeqReceived.
                var currS = parseInt($scope.currSeqReceived);
                if (currS > 0) {
                    let prog = ((1.0 * lastS) / currS) * 100.0;
                    $scope.progressPct = prog.toPrecision(4);
                }
            }
        }
        $scope.lastCurrSeqReceived = $scope.currSeqReceived
    }

    IndexCtrl($scope, http, $routeParams,
              $location, $log, $sce, $uibModal);

    if ($scope.tab === "summary") {
        $scope.loadDocCount();
        $scope.checkHiddenUI();
    }

    ftsServiceHostPort($scope, $http, $location);
}

// -------------------------------------------------------

function IndexNewCtrlFT_NS($scope, $http, $state, $stateParams,
                           $location, $log, $sce, $uibModal,
                           $q, mnBucketsService, mnPoolDefault) {
    mnBucketsService.getBucketsByType(true).
        then(initWithBuckets,
             function(err) {
                 // Possible RBAC issue listing buckets or other error.
                 console.log("mnBucketsService.getBucketsByType failed", err);
                 initWithBuckets([]);
             });

    function getCollections(bucket) {
        return $http({
            method: "GET",
            url: "/pools/default/buckets/" + encodeURIComponent(bucket) + "/collections"
        });
    }

    function listScopesForBucket(bucket) {
        return getCollections(bucket).then(function (resp) {
            var scopes = [];
            for (var scopeI in resp.data.scopes) {
                let scope = resp.data.scopes[scopeI];
                scopes.push(scope.name);
            }
            return scopes;
        });
    }

    function listCollectionsForBucketScope(bucket, scopeName) {
        return getCollections(bucket).then(function (resp) {
            var collections = [];
            for (var scopeI in resp.data.scopes) {
                let scope = resp.data.scopes[scopeI];
                if (scope.name == scopeName) {
                    for ( var collI in scope.collections) {
                        let collection = scope.collections[collI];
                        collections.push(collection.name);
                    }
                }
            }
            return collections;
        });
    }

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
        $scope.scopeNames = [];
        $scope.collectionNames = [];

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

        $scope.docConfigCollections = false;
        $scope.docConfigMode = "type_field";

        $scope.updateBucketDetails = function() {
            listScopesForBucket($scope.newSourceName).then(function (scopes) {
                $scope.scopeNames = scopes;
                if ($scope.newScopeName == "") {
                    if ($scope.scopeNames.length > 0) {
                        $scope.newScopeName = $scope.scopeNames[0];
                    }
                }
                $scope.updateScopeDetails($scope.newScopeName);
            });
        };

        $scope.updateScopeDetails = function(newScopeName) {
            if (!angular.isDefined(newScopeName) ||
                newScopeName == "" ||
                newScopeName == null) {
                $scope.newScopeName = "";
                $scope.collectionNames = [];
                return;
            }
            $scope.newScopeName = newScopeName;
            listCollectionsForBucketScope($scope.newSourceName, $scope.newScopeName).then(function (collections) {
                $scope.collectionNames = collections;
            });
        };

        IndexNewCtrlFT($scope,
                       prefixedHttp($http, '../_p/' + ftsPrefix),
                       $routeParams,
                       $locationRewrite, $log, $sce, $uibModal,
                       finishIndexNewCtrlFTInit);

        function finishIndexNewCtrlFTInit() {
            var putIndexOrig = $scope.putIndex;

            $scope.typeIdentifierChanged = function() {
                if ($scope.docConfigMode == "type_field" ||
                    $scope.docConfigMode == "docid_prefix" ||
                    $scope.docConfigMode == "docid_regexp") {
                    if ($scope.docConfigCollections) {
                        $scope.ftsDocConfig.mode = "scope.collection." + $scope.docConfigMode;
                    } else {
                        $scope.ftsDocConfig.mode = $scope.docConfigMode;
                    }

                    if (!$scope.ftsDocConfig.type_field) {
                        $scope.ftsDocConfig.type_field = "type";
                    }
                } else if ($scope.docConfigMode == "scope.collection.type_field" ||
                    $scope.docConfigMode == "scope.collection.docid_prefix" ||
                    $scope.docConfigMode == "scope.collection.docid_regexp") {
                    $scope.ftsDocConfig.mode = $scope.docConfigMode;
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
                  case "scope.collection.type_field":
                    if (!$scope.ftsDocConfig.type_field) {
                        errs.push("type field is required");
                    }
                    if (!readOnly) {
                        delete $scope.ftsDocConfig.docid_prefix_delim;
                        delete $scope.ftsDocConfig.docid_regexp;
                    }
                  break;

                  case "docid_prefix":
                  case "scope.collection.docid_prefix":
                    if (!$scope.ftsDocConfig.docid_prefix_delim) {
                        errs.push("Doc ID separator is required");
                    }
                    if (!readOnly) {
                        delete $scope.ftsDocConfig.type_field;
                        delete $scope.ftsDocConfig.docid_regexp;
                    }
                  break;

                  case "docid_regexp":
                  case "scope.collection.docid_regexp":
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
                                let newPlanParamsObj = JSON.parse(newPlanParams);
                                newPlanParamsObj["numReplicas"] = numReplicas;
                                newPlanParams = JSON.stringify(newPlanParamsObj, undefined, 2);
                            }

                            var numPIndexes = $scope.numPIndexes;
                            if (numPIndexes > 0) {
                                let newPlanParamsObj = JSON.parse(newPlanParams);
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
                              $location, mnPermissions, qwDocEditorService) {
  var $httpPrefixed = prefixedHttp($http, '../_p/' + ftsPrefix, true);

  var $routeParams = $stateParams;

  $scope.indexName = $stateParams.indexName;

  $httpPrefixed.get('/api/index/' + $scope.indexName).then(function(response) {
    $scope.indexDef = response.data.indexDef;
    if ($scope.indexDef &&
        ($scope.indexDef.sourceType == "couchbase" || $scope.indexDef.sourceType == "gocbcore")) {
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
             ($scope.indexDef.sourceType == "couchbase" || $scope.indexDef.sourceType == "gocbcore") &&
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

    QueryCtrl($scope, $httpPrefixed, $routeParams, $log, $sce, $location, qwDocEditorService);

    ftsServiceHostPort($scope, $http, $location);

    setTimeout(function() {
        document.getElementById("query_bar_input").focus();
    }, 100);
  })
}

// -------------------------------------------------------

function IndexDetailsCtrlFT_NS($scope, $http, $stateParams,
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

    IndexCtrl($scope, http, $routeParams,
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
        "index_dynamic": true,
        "docvalues_dynamic": false
    }
};

function blevePIndexInitController(initKind, indexParams, indexUI,
    $scope, $http, $routeParams, $location, $log, $sce, $uibModal) {

    if (initKind == "edit" || initKind == "create") {
        $scope.replicaOptions = [0];
        $scope.numReplicas = $scope.replicaOptions[0];
        $scope.vbuckets = 1024;
        $scope.numPIndexes = 0;
        $scope.collectionsSupport = false;
        $http.get('/api/conciseOptions').
        then(function(response) {
            var maxReplicasAllowed = parseInt(response.data.maxReplicasAllowed);
            $scope.replicaOptions = [];
            for (var i = 0; i <= maxReplicasAllowed; i++) {
                $scope.replicaOptions.push(i);
            }

            $scope.collectionsSupport = response.data.collectionsSupport;

            if (response.data.bucketTypesAllowed != "") {
                var bucketTypesAllowed = response.data.bucketTypesAllowed.split(":");
                var bucketNamesAllowed = [];
                let i;
                for (i = 0; i < $scope.buckets.length; i++) {
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

    try {
        var mode = $scope.ftsDocConfig.mode.split(".")
        $scope.docConfigMode = mode[mode.length-1];
        if (mode.length == 3) {
            $scope.docConfigCollections = true;
        } else {
            // mode.length == 1
            $scope.docConfigCollections = false;
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

    // set up editor state for easy mode in edit scenario
    if ($scope.easyMappings) {
        $scope.easyMappings.loadFromMapping(mapping);
        let newScopeName = getBucketScopeFromMapping(mapping);
        if (newScopeName) {
            $scope.newScopeName = newScopeName;
            $scope.listCollectionsForBucketScope($scope.newSourceName, $scope.newScopeName).then(function (collections) {
                $scope.collectionNames = collections;
                if (collections.length > 0) {
                    let aCollectionNamedInMapping = $scope.easyMappings.collectionNamedInMapping();
                    if (aCollectionNamedInMapping) {
                        $scope.expando(aCollectionNamedInMapping);
                    } else {
                        $scope.expando(collections[0]);
                    }
                }
            }, function (err) {
                $scope.errorMessage = "Error listing collections for scope.";
                console.log("error listings collections for scope", err);
            });
        }
    }

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

                rv = $scope.prepareIndex(
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
        let rv = $scope.prepareFTSIndex(
            $scope.newIndexName,
            $scope.newIndexType, $scope.newIndexParams,
            $scope.newSourceType, $scope.newSourceName, $scope.newSourceUUID, $scope.newSourceParams,
            $scope.newPlanParams, $scope.prevIndexUUID,
            true);
        if (!rv.errorFields && !rv.errorMessage) {
            var newSourceUUID = rv.newSourceUUID;
            var newPlanParams = rv.newPlanParams;
            rv = $scope.prepareIndex(
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
    $scope, $http, $routeParams, $location, $log, $sce, $uibModal) {
    if (indexParams) {
        if ($scope.easyMappings) {
            indexParams.mapping = $scope.easyMappings.getIndexMapping($scope.newScopeName);
            indexParams.easy_mode_hash = hashParams(indexParams);
        } else {
            indexParams.mapping = $scope.bleveIndexMapping();
        }
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

function IndexNewCtrlFT($scope, $http, $routeParams,
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

        IndexNewCtrl($scope, $http, $routeParams,
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
            let rv = errorMessageFull.message + ": ";
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

// -------------------------------------------------------

// getBucketScopeFromMapping determines the scope from an index mapping
// it is assumed that this mapping has types of the form scope.collection
// no attempt is made to validate that the same scope is set for all types
// if no scope can be determined, empty string is returned
function getBucketScopeFromMapping(mapping) {
    for (var typeName in mapping.types) {
        let scopeCollType = typeName.split(".", 2)
        return scopeCollType[0];
    }
    return "";
}

// -------------------------------------------------------

// IndexNewCtrlFTEasy_NS is the controller for the create/edit easy mode
function IndexNewCtrlFTEasy_NS($scope, $http, $state, $stateParams,
                               $location, $log, $sce, $uibModal,
                               $q, mnBucketsService, mnPoolDefault, mnDocumentsEditingService, mnDocumentsListService) {
    mnBucketsService.getBucketsByType(true).
    then(initWithBuckets,
        function(err) {
            // Possible RBAC issue listing buckets or other error.
            console.log("mnBucketsService.getBucketsByType failed", err);
            initWithBuckets([]);
        });

    function getRandomKeyBucketScopeCollection(bucket, scope, collection) {
        return $http({
            method: "GET",
            url: "/pools/default/buckets/" + encodeURIComponent(bucket)
                + "/scopes/" + encodeURIComponent(scope)
                + "/collections/" + encodeURIComponent(collection)
                + "/localRandomKey"
        });
    }

    function getCollections(bucket) {
        return $http({
            method: "GET",
            url: "/pools/default/buckets/" + encodeURIComponent(bucket) + "/collections"
        });
    }

    function listScopesForBucket(bucket) {
        return getCollections(bucket).then(function (resp) {
            var scopes = [];
            for (var scopeI in resp.data.scopes) {
                let scope = resp.data.scopes[scopeI];
                scopes.push(scope.name);
            }
            return scopes;
        }, function (resp) {
            $scope.errorMessage = "Error listing scopes for bucket.";
            console.log("error listing scopes for bucket", resp);
        });
    }

    $scope.listCollectionsForBucketScope = function (bucket, scopeName) {
        return getCollections(bucket).then(function (resp) {
            var collections = [];
            for (var scopeI in resp.data.scopes) {
                let scope = resp.data.scopes[scopeI];
                if (scope.name == scopeName) {
                    for ( var collI in scope.collections) {
                        let collection = scope.collections[collI];
                        collections.push(collection.name);
                    }
                }
            }
            return collections;
        }, function (resp) {
            $scope.errorMessage = "Error listing collections for bucket/scope.";
            console.log("error listing collections for bucket/scope", resp);
        });
    }

    function prepareDocForCodeMirror(doc) {
        $scope.parsedDocs.setDocForCollection($scope.collectionOpened, doc.json);
        var parsedDoc = $scope.parsedDocs.getParsedDocForCollection($scope.collectionOpened);
        return parsedDoc.getDocument();
    }

    function getSampleDocument(params) {
        return mnDocumentsEditingService.getDocument(params).then(function (sampleDocument) {
            return prepareDocForCodeMirror(sampleDocument.data);
        }, function (resp) {
            switch(resp.status) {
                case 404:
                    $scope.errorMessage = "Error retrieving document.";
                    return prepareDocForCodeMirror({
                        json: '{}'
                    });
            }
        });
    }

    function prepareRandomDocument(params) {
        return params.sampleDocumentId ? getSampleDocument({
            documentId: params.sampleDocumentId,
            bucket: params.bucket
        }) : getRandomKeyBucketScopeCollection(params.bucket, params.scope, params.collection).then(function (resp) {
            return getSampleDocument({
                documentId: resp.data.key,
                bucket: params.bucket,
                scope: params.scope,
                collection: params.collection
            });
        }, function (resp) {
            switch(resp.status) {
                case 404:
                    if (resp.data.error === "fallback_to_all_docs") {
                        return mnDocumentsListService.getDocuments({
                            bucket: params.bucket,
                            scope: params.scope,
                            collection: params.collection,
                            pageNumber: 0,
                            pageLimit: 1
                        }).then(function (resp) {
                            if (resp.data.rows[0]) {
                                return prepareDocForCodeMirror(resp.data.rows[0].doc);
                            } else {
                                $scope.errorMessage = "No documents available in this collection.";
                                return prepareDocForCodeMirror({
                                    json: '{}'
                                });
                            }
                        }, function (resp) {
                            console.log("error retrieving document list", resp);
                            $scope.errorMessage = "Error retrieving document list.";
                            return prepareDocForCodeMirror({
                                json: '{}'
                            });
                        });
                    } else {
                        $scope.errorMessage = "No documents available in this collection.";
                        return prepareDocForCodeMirror({
                            json: '{}'
                        });
                    }
            }
        });
    }

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
        $scope.scopeNames = [];
        $scope.collectionNames = [];
        $scope.collectionOpened = "";

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

        $scope.docConfigCollections = false;
        $scope.docConfigMode = "type_field";

        $scope.easyMappings = newEasyMappings();
        $scope.editFields = newEditFields();
        $scope.parsedDocs = newParsedDocs();
        $scope.easyLanguages = [
            {
                label: "Unknown/Various",
                id: "standard"
            },
            {
                label: "English",
                id: "en"
            },
            {
                label: "Arabic",
                id: "ar"
            },
            {
                label: "Chinese, Japanese, and Korean",
                id: "cjk"
            },
            {
                label: "Danish",
                id: "da"
            },
            {
                label: "Dutch",
                id: "nl"
            },
            {
                label: "Finnish",
                id: "fi"
            },
            {
                label: "French",
                id: "fr"
            },
            {
                label: "German",
                id: "de"
            },
            {
                label: "Hindi",
                id: "hi"
            },
            {
                label: "Hungarian",
                id: "hu"
            },
            {
                label: "Italian",
                id: "it"
            },
            {
                label: "Norwegian",
                id: "fo"
            },
            {
                label: "Persian",
                id: "fa"
            },
            {
                label: "Portuguese",
                id: "pt"
            },
            {
                label: "Romanian",
                id: "ro"
            },
            {
                label: "Russian",
                id: "ru"
            },
            {
                label: "Sorani Kurdish",
                id: "ckb"
            },
            {
                label: "Spanish",
                id: "es"
            },
            {
                label: "Swedish",
                id: "sv"
            },
            {
                label: "Turkish",
                id: "tr"
            },
            {
                label: "Web (text including url, email, hashtags)",
                id: "web"
            },
        ];



        $scope.userSelectedField = function(path, valType) {
            var cursor = $scope.editor.getCursor();
            var parsedDoc = $scope.parsedDocs.getParsedDocForCollection($scope.collectionOpened);
            var newRow = parsedDoc.getRow(path);
            if (newRow != cursor.line) {
                $scope.editor.focus();
                $scope.editor.setCursor({line: newRow, ch: 0})
            }


            if ($scope.easyMapping.hasFieldAtPath(path)) {
                $scope.editField = Object.assign({}, $scope.easyMapping.getFieldAtPath(path));
            } else {
                $scope.editField.path = path;
                // set defaults for new field
                $scope.editField.new = true;
                $scope.editField.name = $scope.editField.splitPathPrefixAndField()[1];
                $scope.editField.store = false;
                $scope.editField.highlight = false;
                $scope.editField.phrase = false;
                $scope.editField.sortFacet = false;
                $scope.editField.date_format = "dateTimeOptional";
                if (valType === "boolean") {
                    $scope.editField.type = "boolean";
                } else if (valType === "number") {
                    $scope.editField.type = "number";
                } else  {
                    // default to text if we aren't sure
                    $scope.editField.type = "text";
                    $scope.editField.analyzer = "en";
                }
            }
        }

        $scope.storeOptionChanged = function() {
            if (!$scope.editField.store) {
                $scope.editField.highlight = false;
            }
        }

        $scope.addField = function() {
            $scope.editField.new = false;
            $scope.easyMapping.addField($scope.editField)
        }

        $scope.deleteField = function(path) {
            var result = confirm("Are you sure you want to delete the field at path '" + path + "' ?");
            if (result) {
                $scope.easyMapping.deleteField(path);
            }
            $scope.editField.path = "";
        }

        $scope.loadAnotherDocument = function(bucket, scope, collection) {
            $scope.errorMessage = "";
            var params = {};
            params.bucket = bucket;
            params.scope = scope;
            params.collection = collection;
            prepareRandomDocument(params).then(function (randomDoc) {
                $scope.sampleDocument = randomDoc;
            });
        }

        $scope.cursorMove = function(editor) {
            var cursor = editor.getCursor();
            if (cursor.line == 0) {
                $scope.editField.path = "";
                return;
            }
            var parsedDoc = $scope.parsedDocs.getParsedDocForCollection($scope.collectionOpened);
            $scope.userSelectedField(parsedDoc.getPath(cursor.line), parsedDoc.getType(cursor.line));
        };

        $scope.bucketChanged = function(orig) {
            if (orig) {
                var result = confirm("Changing the bucket will lose all configuration made with the current bucket, are you sure?");
                if (!result) {
                    $scope.newSourceName = orig;
                    return;
                }
            }
            listScopesForBucket($scope.newSourceName).then(function (scopes) {
                $scope.scopeNames = scopes;
                if (scopes.length > 0) {
                    $scope.newScopeName = scopes[0];
                }
                $scope.scopeChanged();
            });

            $scope.easyMappings = newEasyMappings();
            $scope.editFields = newEditFields();
            $scope.parsedDocs = newParsedDocs();
        };

        $scope.scopeChanged = function(orig) {
            if (orig) {
                var result = confirm("Changing the scope will lose all configuration made with the current bucket and scope, are you sure?");
                if (!result) {
                    $scope.newScopeName = orig;
                    return;
                }
            }
            $scope.listCollectionsForBucketScope($scope.newSourceName, $scope.newScopeName).then(function (collections) {
                $scope.collectionNames = collections;
                if (collections.length > 0) {
                    $scope.expando(collections[0]);
                }
            });

            $scope.easyMappings = newEasyMappings();
            $scope.editFields = newEditFields();
            $scope.parsedDocs = newParsedDocs();
        };

        $scope.expando = function(collectionName) {
            $scope.errorMessage = "";
            $scope.collectionOpened = collectionName;
            var sampleDocument = $scope.parsedDocs.getParsedDocForCollection(collectionName).getDocument();
            if (sampleDocument == '{}') {
                $scope.loadAnotherDocument($scope.newSourceName, $scope.newScopeName, collectionName);
            } else {
                $scope.sampleDocument = sampleDocument;
            }

            $scope.editField = $scope.editFields.getFieldForCollection(collectionName);
            $scope.easyMapping = $scope.easyMappings.getMappingForCollection(collectionName);
        };

        $scope.codemirrorLoaded = function(_editor){
            $scope.editor = _editor;
            initCodeMirrorActiveLine(CodeMirror);

            // NOTE: this event must be registered before we configure
            // the styleActiveLine setting below, as it's event must
            // fire AFTER our event handler
            _editor.on("beforeSelectionChange", function(cm, obj) {
                // changes to selection must be made with the provided update
                // method, here we always update to a single element array
                // (we don't ever want multiple selections), further we
                // update the anchor and head to be the same (no multiple lines)
                // further, we use the head, not the anchor, we should get the
                // end of the selection if they attempted a multi-line selection
                if (obj.ranges && obj.ranges[0]) {
                    obj.update([
                        {
                            anchor: obj.ranges[0].head,
                            head: obj.ranges[0].head
                        }
                    ])
                }
            });

            // Options
            _editor.setOption('readOnly', true);
            _editor.setOption('lineWrapping', true);
            _editor.setOption('styleActiveLine', true);
            _editor.setOption('configureMouse', function(cm, repeat, event) {
                return {
                    addNew : false, // disables ctrl(win)/meta(mac) multiple-select
                    extend: false // disables shift extend select
                };
            });

            // Events
            _editor.on("cursorActivity", $scope.cursorMove);

        };

        IndexNewCtrlFT($scope,
            prefixedHttp($http, '../_p/' + ftsPrefix),
            $routeParams,
            $locationRewrite, $log, $sce, $uibModal,
            finishIndexNewCtrlFTInit);

        function finishIndexNewCtrlFTInit() {
            var putIndexOrig = $scope.putIndex;

            $scope.prepareFTSIndex = function(newIndexName,
                                              newIndexType, newIndexParams,
                                              newSourceType, newSourceName,
                                              newSourceUUID, newSourceParams,
                                              newPlanParams, prevIndexUUID,
                                              readOnly) {
                var errorFields = {};
                var errs = [];

                // easy mode is hard-coded to this doc config
                $scope.ftsDocConfig.mode = "scope.collection.type_field";

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
                                let newPlanParamsObj = JSON.parse(newPlanParams);
                                newPlanParamsObj["numReplicas"] = numReplicas;
                                newPlanParams = JSON.stringify(newPlanParamsObj, undefined, 2);
                            }

                            var numPIndexes = $scope.numPIndexes;
                            if (numPIndexes > 0) {
                                let newPlanParamsObj = JSON.parse(newPlanParams);
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

// Utility functions to compute attempt to compute a stable hash on a mapping

function sortObjByKey(value) {
    return (typeof value === 'object') ?
        (Array.isArray(value) ?
                value.map(sortObjByKey) :
                Object.keys(value).sort().reduce(
                    (o, key) => {
                        const v = value[key];
                        o[key] = sortObjByKey(v);
                        return o;
                    }, {})
        ) :
        value;
}


function orderedJsonStringify(obj) {
    return JSON.stringify(sortObjByKey(obj));
}

function hashCode(str) {
    var hash = 0;
    if (str.length == 0) {
        return hash;
    }
    for (var i = 0; i < str.length; i++) {
        var char = str.charCodeAt(i);
        hash = ((hash<<5)-hash)+char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
}

function hashParams(params) {
    // hash an object, with doc_config AND mapping from params
    let objToHash = {
        "doc_config": params.doc_config,
        "mapping": params.mapping
    };
    return hashCode(orderedJsonStringify(objToHash)).toString();
}

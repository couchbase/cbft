//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

var ftsAppName = 'fts';
var ftsPrefix = 'fts';

// -------------------------------------------------------
import angular from "angular";

import {downgradeInjectable} from '@angular/upgrade/static';
import { QwDialogService } from '../query/angular-directives/qw.dialog.service.js';

import uiRouter from "@uirouter/angularjs";
import uiCodemirror from "angular-ui-codemirror";
import CodeMirror from "codemirror";
import ngClipboard from "ngclipboard";
import ngSortable from "angular-legacy-sortable";
import mnPermissions from "components/mn_permissions";
import mnFooterStatsController from "mn_admin/mn_gsi_footer_controller";
import mnStatisticsNewService from "mn_admin/mn_statistics_service";
import mnDocumentsService from "mn_admin/mn_documents_service";
import mnSelect from "components/directives/mn_select/mn_select";

import BleveAnalyzerModalCtrl from "./static-bleve-mapping/js/mapping/analysis-analyzer.js";
import BleveCharFilterModalCtrl from "./static-bleve-mapping/js/mapping/analysis-charfilter.js";
import BleveDatetimeParserModalCtrl from "./static-bleve-mapping/js/mapping/analysis-datetimeparser.js";
import BleveTokenFilterModalCtrl from "./static-bleve-mapping/js/mapping/analysis-tokenfilter.js";
import BleveTokenizerModalCtrl from "./static-bleve-mapping/js/mapping/analysis-tokenizer.js";
import BleveWordListModalCtrl from "./static-bleve-mapping/js/mapping/analysis-wordlist.js";
import {bleveIndexMappingScrub} from "./static-bleve-mapping/js/mapping/index-mapping.js";

import {IndexesCtrl, IndexCtrl, IndexNewCtrl} from "./static/index.js";
import {errorMessage, confirmDialog, alertDialog, obtainBucketScopeUndecoratedIndexName} from "./static/util.js";
import QueryCtrl from "./static/query.js";
import uiTree from "angular-ui-tree";

import {initCodeMirrorActiveLine} from "./codemirror-active-line.js";
import {newParsedDocs} from "./fts_easy_parse.js";
import {newEditFields, newEditField} from "./fts_easy_field.js";
import {newEasyMappings, newEasyMapping} from "./fts_easy_mapping.js";

import ftsListTemplate from "./fts_list.html";
import ftsNewTemplate from "./fts_new.html";
import ftsNewEasyTemplate from "./fts_new_easy.html";
import ftsSearchTemplate from "./fts_search.html";
import ftsDetailsTemplate from "./fts_details.html";
import indexImportTemplate from "./import_index.html";

export default ftsAppName;

angular
    .module(ftsAppName,
            [uiRouter, ngClipboard, mnPermissions, uiTree, ngSortable, mnStatisticsNewService,
                uiCodemirror, mnDocumentsService, mnSelect])
    .config(["$stateProvider", function($stateProvider) {
      addFtsStates("app.admin.search");

      function addFtsStates(parent) {
        $stateProvider
          .state(parent, {
            url: '/fts',
            abstract: true,
            views: {
              "main@app.admin": {
                controller: "IndexesCtrlFT_NS",
                template: ftsListTemplate
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
            template: ftsListTemplate
          })
          .state(parent + '.fts_new', {
            url: '/fts_new/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                template: ftsNewTemplate
              }
            },
            data: {
              title: "Add Index",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_new_easy', {
            url: '/fts_new_easy/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFTEasy_NS',
                template: ftsNewEasyTemplate
              }
            },
            data: {
              title: "Quick Index",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_new_alias', {
            url: '/fts_new_alias/?indexType&sourceType',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                template: ftsNewTemplate
              }
            },
            data: {
              title: "Add Alias",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_edit', {
            url: '/fts_edit/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                template: ftsNewTemplate
              }
            },
            data: {
              title: "Edit Index",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_edit_easy', {
            url: '/fts_edit_easy/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFTEasy_NS',
                template: ftsNewEasyTemplate
              }
            },
            data: {
              title: "Edit Quick Index",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_edit_alias', {
            url: '/fts_edit_alias/:indexName/_edit',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                template: ftsNewTemplate
              }
            },
            data: {
              title: "Edit Alias",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_clone', {
            url: '/fts_clone/:indexName/_clone',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                template: ftsNewTemplate
              }
            },
            data: {
              title: "Clone Index",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_clone_alias', {
            url: '/fts_clone_alias/:indexName/_clone',
            views: {
              "main@app.admin": {
                controller: 'IndexNewCtrlFT_NS',
                template: ftsNewTemplate
              }
            },
            data: {
              title: "Clone Alias",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_search', {
            url: '/fts_search/:indexName/_search?q&p',
            reloadOnSearch: false,
            views: {
              "main@app.admin": {
                controller: 'IndexSearchCtrlFT_NS',
                template: ftsSearchTemplate
              }
            },
            data: {
              title: "Search Results",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          })
          .state(parent + '.fts_details', {
            url: '/fts_details/:indexName',
            views: {
              "main@app.admin": {
                controller: 'IndexDetailsCtrlFT_NS',
                template: ftsDetailsTemplate
              }
            },
            data: {
              title: "Index Details",
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
            }
          });
      }
    }]);

  angular.module(ftsAppName).
        controller('mnFooterStatsController', mnFooterStatsController).
        controller('IndexesCtrlFT_NS',
                   ["$scope", "$http", "$state", "$stateParams",
                    "$log", "$sce", "$location", "$uibModal",
                    "mnPoolDefault", "mnPermissions", IndexesCtrlFT_NS]).
        controller('indexViewController',
                   ["$scope", "$http", "$state", "$log",
                    "$sce", "$location", "$uibModal",
                    "mnPermissions", indexViewController]).
        controller('IndexCtrlFT_NS',
                   ["$scope", "$http", "$stateParams", "$state",
                    "$location", "$log", "$sce", "$uibModal", IndexCtrlFT_NS]).
        controller('IndexNewCtrlFT_NS',
                   ["$scope", "$http", "$state", "$stateParams",
                    "$location", "$log", "$sce", "$uibModal",
                    "$q", "mnBucketsService", "mnPoolDefault", IndexNewCtrlFT_NS]).
        controller('IndexNewCtrlFTEasy_NS',
                   ["$scope", "$http", "$state", "$stateParams",
                    "$location", "$log", "$sce", "$uibModal",
                    "$q", "mnBucketsService", "mnPoolDefault",
                    "mnDocumentsService", IndexNewCtrlFTEasy_NS]).
        controller('IndexSearchCtrlFT_NS',
                   ["$scope", "$http", "$stateParams", "$log", "$sce",
                    "$location", "mnPermissions",
                    "qwDialogService", IndexSearchCtrlFT_NS]).
        controller('IndexDetailsCtrlFT_NS',
                   ["$scope", "$http", "$stateParams",
                    "$location", "$log", "$sce",
                    "$uibModal", IndexDetailsCtrlFT_NS]).
        factory('qwDialogService', downgradeInjectable(QwDialogService));

// ----------------------------------------------

var updateDocCountIntervalMS = 5000;

function IndexesCtrlFT_NS($scope, $http, $state, $stateParams,
                          $log, $sce, $location, $uibModal,
                          mnPoolDefault, mnPermissions) {
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

      nextSteps();
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

      nextSteps();
    });

    function nextSteps() {
        if (!$scope.ftsChecking && !$scope.ftsAvailable && !$scope.ftsNodes.length) {
            // can't do anything if no nodes hosting search service
            return;
        }

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

        $scope.done = false;

        var rv = IndexesCtrl($scope, http, $routeParams, $log, $sce, $location, $uibModal);

        $scope.$on('$stateChangeStart', function() {
            $scope.done = true;
        });

        function isMappingIncompatibleWithQuickEditor(mapping) {
            if (!mapping.enabled) {
                // mapping is disabled
                return true;
            }

            if (!mapping.dynamic &&
                (!angular.isDefined(mapping.properties) || mapping.properties.length == 0) &&
                (!angular.isDefined(mapping.fields) || mapping.fields.length == 0)) {
                // mapping has no child mappings or fields defined within it
                return true;
            }

            for (var name in mapping.properties) {
                if (mapping.properties[name].dynamic) {
                    // dynamic child mappings not supported
                    return true;
                }
                if (isMappingIncompatibleWithQuickEditor(mapping.properties[name])) {
                    return true;
                }
            }

            if (angular.isDefined(mapping.fields)) {
                if (mapping.fields.length > 1) {
                    // a field was mapped multiple times
                    return true;
                }

                if (mapping.fields.length == 1) {
                    if (!mapping.fields[0].index) {
                        // un-indexed field
                        return true;
                    }
                }
            }

            return false;
        }

        $scope.showEasyMode = function(indexDef) {
            let params = indexDef.params;
            if (params.doc_config.mode != "scope.collection.type_field" ||
                params.doc_config.type_field != "type") {
                // quick (easy) editor works in scope.collection.type_field mode only
                return false;
            }

            if (params.mapping.default_analyzer != "standard" ||
                params.mapping.default_datetime_parser != "dateTimeOptional" ||
                params.mapping.default_field != "_all" ||
                params.mapping.default_type != "_default" ||
                params.mapping.type_field != "_type") {
                // advanced settings' violation for quick (easy) editor
                return false;
            }

            let analysis = params.mapping.analysis;
            if (angular.isDefined(analysis) &&
                Object.keys(analysis).length > 0) {
                // custom analysis elements aren't supported with quick (easy) editor
                return false;
            }

            if (params.mapping.default_mapping.enabled) {
                // default mapping not supported with quick (easy) editor
                return false;
            }

            for (var name in params.mapping.types) {
                let scopeCollection = name.split(".");
                if (scopeCollection.length != 2) {
                    // type names can only be of format "scope.collection" to
                    // work with the quick (easy) editor
                    return false
                }

                if (isMappingIncompatibleWithQuickEditor(params.mapping.types[name])) {
                    return false;
                }
            }

            return true;
        };

        $scope.expando = function(indexName) {
            $scope.detailsOpened[indexName] = !$scope.detailsOpened[indexName];

            // The timeout gives angular some time to create the input control.
            if ($scope.detailsOpened[indexName]) {
                setTimeout(function() {
                    document.getElementById('query_bar_input_' + indexName).focus()
                }, 100);
            }
        };

        return rv;
    }
}

function indexViewController($scope, $http, $state, $log, $sce, $location, $uibModal, mnPermissions) {
    $scope.indexName = $scope.indexDef.name;

    var stateParams = {indexName: $scope.indexName};

    $scope.jsonDetails = false;
    $scope.curlDetails = false;

    var rv = IndexCtrlFT_NS($scope, $http, stateParams, $state,
        $location, $log, $sce, $uibModal);

    var loadProgressStats = $scope.loadProgressStats;

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
        if (!$scope.done) {
            if (loadProgressStats(checkRetStatus)) {
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

// -------------------------------------------------------

function IndexCtrlFT_NS($scope, $http, $stateParams, $state,
                        $location, $log, $sce, $uibModal) {
    var $routeParams = $stateParams;

    var http = prefixedHttp($http, '../_p/' + ftsPrefix);

    $scope.progress = "idle";
    $scope.docCount = "";
    $scope.numMutationsToIndex = "";
    $scope.httpStatus = 200;    // OK

    $scope.loadProgressStats = function(callback) {
        if ($scope.indexDef.type != "fulltext-index") {
            return;
        }
        $scope.errorMessage = null;
        $scope.errorMessageFull = null;

        http.get('/api/stats/index/'+$scope.indexName+'/progress').
        then(function(response) {
            var data = response.data;
            if (data) {
                $scope.docCount = data["doc_count"];
                $scope.numMutationsToIndex = data["num_mutations_to_index"];
                updateProgress();
            }
        }, function(response) {
            $scope.httpStatus = response.Status;
            updateProgress();
        });

        if (callback) {
            return callback($scope.httpStatus);
        }
    }

    $scope.obtainScope = function(indexDef) {
        if (angular.isDefined(indexDef.params.doc_config.mode)) {
            if (indexDef.params.doc_config.mode.startsWith("scope.collection.")) {
                if (angular.isDefined(indexDef.params.mapping.default_mapping.enabled)) {
                    if (indexDef.params.mapping.default_mapping.enabled) {
                        return "_default";
                    }
                }
                if (angular.isDefined(indexDef.params.mapping.types)) {
                    for (let [key, value] of Object.entries(indexDef.params.mapping.types)) {
                        if (value.enabled) {
                            return key.split(".")[0];
                        }
                    }
                }
            }
        }
        return "_default"
    }

    $scope.obtainCollections = function(indexDef) {
        if (angular.isDefined(indexDef.params.doc_config.mode)) {
            if (indexDef.params.doc_config.mode.startsWith("scope.collection.")) {
                let collectionNames = [];
                if (angular.isDefined(indexDef.params.mapping.default_mapping.enabled)) {
                    if (indexDef.params.mapping.default_mapping.enabled) {
                        collectionNames.push("_default");
                    }
                }
                if (angular.isDefined(indexDef.params.mapping.types)) {
                    for (let [key, value] of Object.entries(indexDef.params.mapping.types)) {
                        if (value.enabled) {
                            try {
                                let collName = key.split(".")[1];
                                if (collName.length > 0 && collectionNames.indexOf(collName) < 0) {
                                    collectionNames.push(collName);
                                }
                            } catch (e) {}
                        }
                    }
                }
                return collectionNames;
            }
        }
        return ["_default"];
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

    function updateProgress() {
        try {
            if (angular.isDefined($scope.indexDef.planParams.nodePlanParams[""][""].canWrite)) {
                if (!$scope.indexDef.planParams.nodePlanParams[""][""].canWrite) {
                    $scope.progress = "paused";
                    return;
                }
            }
        } catch (e) {}

        var numMutationsToIndex = parseInt($scope.numMutationsToIndex);
        let prog = "idle";
        if (angular.isDefined(numMutationsToIndex) && numMutationsToIndex > 0) {
            prog = "active";
        }
        $scope.progress = prog;
    }

    IndexCtrl($scope, http, $routeParams,
              $location, $log, $sce, $uibModal);

    if ($scope.tab === "summary") {
        $scope.loadProgressStats();
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
            url: "/pools/default/buckets/" + encodeURIComponent(bucket) + "/scopes"
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
        $scope.ftsStore.indexType = "scorch";

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

        $scope.docConfigCollections = false;
        $scope.docConfigMode = "type_field";


        function initScopeName(params) {
            let paramsObj = angular.fromJson(params);
            let docConfig = angular.fromJson(paramsObj.doc_config);
            if (angular.isDefined(docConfig.mode)) {
                if (docConfig.mode.startsWith("scope.collection.")) {
                    let mapping = angular.fromJson(paramsObj.mapping);
                    if (angular.isDefined(mapping.default_mapping)) {
                        if (mapping.default_mapping.enabled) {
                            return "_default";
                        }
                        for (let [key, value] of Object.entries(mapping.types)) {
                            if (value.enabled) {
                                return key.split(".")[0];
                            }
                        }
                    }
                    return "";
                }
                return "_default";
            }
            return "";
        }

        $scope.updateBucketDetails = function(selectedBucket, keepScope) {
            listScopesForBucket(selectedBucket || $scope.newSourceName).then(function (scopes) {
                if (!keepScope) {
                    $scope.newScopeName = initScopeName($scope.newIndexParams['fulltext-index']);
                }
                $scope.scopeNames = scopes;
                if ($scope.scopeNames.length > 0) {
                    if ($scope.newScopeName == "" || $scope.scopeNames.indexOf($scope.newScopeName) < 0) {
                        // init scope to first entry in options if unavailable or not in options
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

                let newNameSuffix = newIndexName;
                let pos = newIndexName.lastIndexOf(".");
                if (pos > 0 && pos+1 < newIndexName.length) {
                    newNameSuffix = newIndexName.slice(pos+1);
                }

                if (!newNameSuffix) {
                    errorFields["indexName"] = true;
                    errs.push("index name is required");
                } else if ($scope.meta &&
                    $scope.meta.indexNameRE &&
                    !newNameSuffix.match($scope.meta.indexNameRE)) {
                    errorFields["indexName"] = true;
                    errs.push("index name '" + newNameSuffix + "'" +
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
                                       newPlanParams, prevIndexUUID, isEdit) {
                if (isEdit) {
                    newIndexName = $scope.fullIndexName;
                }
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

    function ImportIndexCtrl($uibModalInstance) {

        $scope.cancel = function() {
            $uibModalInstance.close({})
        }

        $scope.add = function(indexJSON) {
            if (isJSON(indexJSON)) {
                if ($scope.newIndexType == "fulltext-index") {
                    $scope.resetIndexDef()
                    $scope.errorMsg = ""
                    $scope.parseIndexJSON(indexJSON)
                    if ($scope.errorMsg == "") {
                        $scope.showCustomizeIndex = true
                        $scope.cancel()
                    } else {
                        $scope.resetIndexDef()
                    }
                } else if ($scope.newIndexType == "fulltext-alias") {
                    $scope.resetAliasDef()
                    $scope.errorMsg = ""
                    $scope.parseAliasJSON(indexJSON)
                    if ($scope.errorMsg == "") {
                        $scope.showCustomizeIndex = true
                        $scope.cancel()
                    } else {
                        $scope.resetAliasDef()
                    }
                } else {
                    $scope.errorMsg = "Invalid index type"
                }
            } else {
                $scope.errorMsg = "Invalid JSON input"
            }
        }

        function isJSON(string) {
            try {
                JSON.parse(string)
            } catch (e) {
                return false
            }
            return true
        }

        $scope.resetAliasDef = function() {

            $scope.newIndexName = ""

            $scope.selectedTargetIndexes = []
            $scope.indexEditorPreview["fulltext-alias"] = null
        }

        $scope.parseAliasJSON = function(aliasJSON) {

            var indexParsed = JSON.parse(aliasJSON)

            if ("name" in indexParsed) {
                $scope.newIndexName = indexParsed.name.split(".").pop()
            }

            if ("params" in indexParsed) {
                if ("targets" in indexParsed.params) {
                    for (const [k, val] of Object.entries(indexParsed.params.targets)) {
                        $scope.parseTarget(k)

                        if ($scope.errorMsg != "") {
                            return
                        }
                    }
                }
            }
        }

        $scope.parseTarget = function(key) {
            if ($scope.aliasTargets.includes(key)) {
                $scope.selectedTargetIndexes.push(key)
            } else {
                $scope.errorMsg = ""
            }
        };

        $scope.resetIndexDef = function() {

            $scope.indexEditorPreview["fulltext-index"] = null
            $scope.newIndexName = ""
            $scope.docConfigCollections = false
            $scope.newScopeName = ""
            $scope.updateScopeDetails($scope.newScopeName)

            $scope.indexMapping.analysis.char_filters = {}
            $scope.indexMapping.analysis.tokenizers = {}
            $scope.indexMapping.analysis.token_maps = {}
            $scope.indexMapping.analysis.token_filters = {}
            $scope.indexMapping.analysis.date_time_parsers = {}
            $scope.indexMapping.analysis.analyzers = {}
            $scope.indexMapping.default_analyzer = "standard"
            $scope.indexMapping.default_datetime_parser = "dateTimeOptional"
            $scope.indexMapping.default_field = "_all"
            $scope.indexMapping.default_mapping = {
                "enabled": true,
                "dynamic": true
            }
            $scope.indexMapping.default_type = "_default"
            $scope.indexMapping.docvalues_dynamic = false
            $scope.indexMapping.index_dynamic = true
            $scope.indexMapping.store_dynamic = false

            while ($scope.mappings.length > 1) {
                $scope.mappings.shift()
            }

            $scope.mappings[0].dynamic = true
            $scope.mappings[0].enabled = true
            $scope.mappings[0].fields = []

            while ($scope.mappings[0].mappings.length > 0) {
                $scope.mappings[0].mappings.pop()
            }

            delete $scope.mappings[0].date_format
            delete $scope.mappings[0].default_analyzer

            $scope.docConfigMode = "type_field"
            $scope.ftsDocConfig = {
                docid_prefix_delim: "",
                docid_regexp: "",
                mode: "type_field",
                type_field: "type"
            }

            $scope.newSourceParams = $scope.sourceParamsCopy
            $scope.numReplicas = 0
            $scope.numPIndexes = 1
        }

        $scope.parseIndexJSON = function(indexJSON) {

            var indexParsed = JSON.parse(indexJSON)
            if ("name" in indexParsed) {
                $scope.newIndexName = indexParsed.name.split(".").pop()
            }

            if ("sourceName" in indexParsed) {
                if ($scope.bucketNames.includes(indexParsed.sourceName)) {
                    $scope.newSourceName = indexParsed.sourceName
                    $scope.updateBucketDetails(indexParsed.sourceName, true)
                } else {
                    $scope.errorMsg = "Unknown source name '" + indexParsed.sourceName + "'"
                    return
                }
            }

            if ("params" in indexParsed) {
                if ("doc_config" in indexParsed.params) {
                    if ("mode" in indexParsed.params.doc_config) {
                        if (indexParsed.params.doc_config.mode.startsWith("scope.collection.")) {
                            $scope.docConfigCollections = true
                            indexParsed.params.doc_config.mode = indexParsed.params.doc_config.mode.slice(17)
                        }

                        if (indexParsed.params.doc_config.mode == "type_field" ||
                            indexParsed.params.doc_config.mode == "docid_prefix" ||
                            indexParsed.params.doc_config.mode == "docid_regexp") {
                                $scope.docConfigMode = indexParsed.params.doc_config.mode;
                                $scope.typeIdentifierChanged()
                            }
                    } else {
                        $scope.errorMsg = "mode is a required field in doc_config"
                        return
                    }

                    switch (indexParsed.params.doc_config.mode) {
                        case "type_field":
                            if ("type_field" in indexParsed.params.doc_config) {
                                $scope.ftsDocConfig.type_field = indexParsed.params.doc_config.type_field
                            } else {
                                $scope.errorMsg = "type_field is a required field in doc_config if mode is 'type_field'"
                                return
                            }
                            break
                        case "docid_prefix":
                            if ("docid_prefix_delim" in indexParsed.params.doc_config) {
                                $scope.ftsDocConfig.docid_prefix_delim = indexParsed.params.doc_config.docid_prefix_delim
                            } else {
                                $scope.errorMsg = "docid_prefix_delim is a required field in doc_config if mode is 'docid_prefix'"
                                return
                            }
                            break
                        case "docid_regexp":
                            if ("docid_regexp" in indexParsed.params.doc_config) {
                                $scope.ftsDocConfig.docid_regexp = indexParsed.params.doc_config.docid_regexp
                            } else {
                                $scope.errorMsg = "docid_regexp is a required field in doc_config if mode is 'docid_regexp'"
                                return
                            }
                            break
                    }
                }

                if ("mapping" in indexParsed.params) {

                    if ("analysis" in indexParsed.params.mapping) {

                        if ("char_filters" in indexParsed.params.mapping.analysis) {
                            for (const [k, val] of Object.entries(indexParsed.params.mapping.analysis.char_filters)) {
                                $scope.parseCharFilter(k, val)

                                if ($scope.errorMsg != "") {
                                    return
                                }
                            }
                        }

                        if ("tokenizers" in indexParsed.params.mapping.analysis) {
                            $scope.parseTokenizers(indexParsed.params.mapping.analysis.tokenizers)

                            if ($scope.errorMsg != "") {
                                return
                            }
                        }

                        if ("token_maps" in indexParsed.params.mapping.analysis) {
                            for (const [k, val] of Object.entries(indexParsed.params.mapping.analysis.token_maps)) {
                                $scope.parseTokenMap(k, val)

                                if ($scope.errorMsg != "") {
                                    return
                                }
                            }
                        }

                        if ("token_filters" in indexParsed.params.mapping.analysis) {
                            for (const [k, val] of Object.entries(indexParsed.params.mapping.analysis.token_filters)) {
                                $scope.parseTokenFilter(k, val)

                                if ($scope.errorMsg != "") {
                                    return
                                }
                            }
                        }

                        if ("date_time_parsers" in indexParsed.params.mapping.analysis) {
                            for (const [k, val] of Object.entries(indexParsed.params.mapping.analysis.date_time_parsers)) {
                                $scope.parseDateTimeParser(k, val)

                                if ($scope.errorMsg != "") {
                                    return
                                }
                            }
                        }

                        if ("analyzers" in indexParsed.params.mapping.analysis) {
                            for (const [k, val] of Object.entries(indexParsed.params.mapping.analysis.analyzers)) {
                                $scope.parseAnalyzer(k, val)
                                if ($scope.errorMsg != "") {
                                    return
                                }
                            }
                        }
                    }

                    if ("default_mapping" in indexParsed.params.mapping) {

                        var mapping = $scope.mappings[0]

                        if ("enabled" in indexParsed.params.mapping.default_mapping) {
                            if (indexParsed.params.mapping.default_mapping.enabled == true || indexParsed.params.mapping.default_mapping.enabled == false) {
                                mapping.enabled = indexParsed.params.mapping.default_mapping.enabled
                            }
                        }

                        if ("dynamic" in indexParsed.params.mapping.default_mapping) {
                            if (indexParsed.params.mapping.default_mapping.dynamic == true || indexParsed.params.mapping.default_mapping.dynamic == false) {
                                mapping.dynamic = indexParsed.params.mapping.default_mapping.dynamic
                            }
                        }

                        if ("default_analyzer" in indexParsed.params.mapping.default_mapping) {
                            if ($scope.analyzerNames.includes(indexParsed.params.mapping.default_mapping.default_analyzer)) {
                                mapping.default_analyzer = indexParsed.params.mapping.default_mapping.default_analyzer
                            } else {
                                $scope.errorMsg = "default_mapping has invalid value for field 'default_analyzer'"
                                return
                            }
                        }

                        if ("properties" in indexParsed.params.mapping.default_mapping) {
                            for (const [k, val] of Object.entries(indexParsed.params.mapping.default_mapping.properties)) {
                                $scope.parseMapping(k, val, mapping)

                                if ($scope.errorMsg != "") {
                                    return
                                }
                            }
                        }
                    }

                    if ("types" in indexParsed.params.mapping) {
                        for (const [key, value] of Object.entries(indexParsed.params.mapping.types)) {
                            $scope.parseScope(key)
                            $scope.parseMapping(key, value, null)
                        }
                    }

                    if ("default_type" in indexParsed.params.mapping) {
                        $scope.indexMapping.default_type = indexParsed.params.mapping.default_type
                    }

                    if ("default_analyzer" in indexParsed.params.mapping) {
                        if ($scope.analyzerNames.includes(indexParsed.params.mapping.default_analyzer)) {
                            $scope.indexMapping.default_analyzer = indexParsed.params.mapping.default_analyzer
                        } else {
                            $scope.errorMsg = "Unknown value for default_analyzer"
                            return
                        }
                    }

                    if ("default_datetime_parser" in indexParsed.params.mapping) {
                        if ($scope.dateTimeParserNames.includes(indexParsed.params.mapping.default_datetime_parser)) {
                            $scope.indexMapping.default_datetime_parser = indexParsed.params.mapping.default_datetime_parser
                        } else {
                            $scope.errorMsg = "Unknown value for default_datetime_parser"
                            return
                        }
                    }

                    if ("default_field" in indexParsed.params.mapping) {
                        $scope.indexMapping.default_field = indexParsed.params.mapping.default_field
                    }

                    if ("store_dynamic" in indexParsed.params.mapping) {
                        if (indexParsed.params.mapping.store_dynamic == true || indexParsed.params.mapping.store_dynamic == false) {
                            $scope.indexMapping.store_dynamic = indexParsed.params.mapping.store_dynamic
                        }
                    }

                    if ("index_dynamic" in indexParsed.params.mapping) {
                        if (indexParsed.params.mapping.index_dynamic == true || indexParsed.params.mapping.index_dynamic == false) {
                            $scope.indexMapping.index_dynamic = indexParsed.params.mapping.index_dynamic
                        }
                    }

                    if ("docvalues_dynamic" in indexParsed.params.mapping) {
                        if (indexParsed.params.mapping.docvalues_dynamic == true || indexParsed.params.mapping.docvalues_dynamic == false) {
                            $scope.indexMapping.docvalues_dynamic = indexParsed.params.mapping.docvalues_dynamic
                        }
                    }
                }
            }

            if ("sourceParams" in indexParsed) {
                $scope.newSourceParams = indexParsed.sourceParams;
            }

            if ("planParams" in indexParsed) {
                if ("numReplicas" in indexParsed.planParams) {
                    if ($scope.ftsNodes.length > indexParsed.planParams.numReplicas && indexParsed.planParams.numReplicas <= 3 && indexParsed.planParams.numReplicas >= 0) {
                        $scope.numReplicas = indexParsed.planParams.numReplicas
                    } else {
                        $scope.errorMsg = "Invalid number of replicas"
                        return
                    }
                } else {
                    $scope.numReplicas = 0
                }
                if ("indexPartitions" in indexParsed.planParams) {
                    if (indexParsed.planParams.indexPartitions >= 1) {
                        $scope.numPIndexes = indexParsed.planParams.indexPartitions
                    } else {
                        $scope.errorMsg = "indexPartitions must be a positive number"
                        return
                    }
                } else {
                    $scope.numPIndexes = 1
                }
            }
        }

        $scope.parseCharFilter = function(key, val) {

            var err = $scope.validateCharFilter(key, val, $scope.indexMapping.analysis.char_filters)
            if (err == "") {
                $scope.indexMapping.analysis.char_filters[key] = val
            } else {
                $scope.errorMsg = err
            }
        }

        $scope.validateCharFilter = function (name, newCharFilter, charFilters) {

            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            if (!name) {
                return "Name is required"
            }

            if (name != "" && charFilters[name]) {
                return "Character filter named '" + name + "' already exists"
            }

            let testFilters = {}
            testFilters[name] = newCharFilter

            let testMapping = {
                "analysis": {
                    "char_filters": testFilters
                }
            }

            http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
            then(function() {}, function(response) {
                $scope.errorMsg = response.data
            })

            return ""
        }

        $scope.parseTokenizers = function(tokenizers) {
            var err = $scope.validateAllTokenizers(tokenizers, $scope.indexMapping.analysis.tokenizers)
            if (err == "") {
                for (var t in tokenizers) {
                    $scope.indexMapping.analysis.tokenizers[t] = tokenizers[t]
                }
            } else {
                $scope.errorMsg = err
            }
        }

        $scope.validateAllTokenizers = function (newTokenizers, tokenizers) {

            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            for (const name in Object.keys(newTokenizers)) {
                if (!name) {
                    return "Tokenizer name is required"
                }
            }

            for (var t in tokenizers) {
                newTokenizers[t] = tokenizers[t]
            }

            let testMapping = {
                "analysis": {
                    "tokenizers": newTokenizers
                }
            }

            http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
            then(function() {}, function(response) {
                $scope.errorMsg = response.data
            })

            return ""
        }

        $scope.parseTokenMap = function(key, val) {

            var err = $scope.validateTokenMap(key, val, $scope.indexMapping.analysis.token_maps)
            if (err == "") {
                $scope.indexMapping.analysis.token_maps[key] = val
                $scope.tokenMapNames.push(key)
            } else {
                $scope.errorMsg = err
            }
        }

        $scope.validateTokenMap = function (name, newTokenMap, tokenMaps) {

            if (!name) {
                return "Name is required"
            }

            if (name != "" && tokenMaps[name]) {
                return "Word list named '" + name + "' already exists"
            }

            if (Object.keys(newTokenMap).length != 2) {
                return "Word list named '" + name + "' must only have type and token fields"
            }

            if (!("type" in newTokenMap) || !("tokens" in newTokenMap)) {
                return "Word list named '" + name + "' must have type and token fields"
            }

            if (newTokenMap.type != "custom") {
                return "Word list named '" + name + "' must have type 'custom'"
            }

            return ""
        }

        $scope.parseTokenFilter = function (key, val) {

            var err = $scope.validateTokenFilter(key, val, $scope.indexMapping.analysis.token_filters, $scope.indexMapping.analysis.token_maps)
            if (err == "") {
                $scope.indexMapping.analysis.token_filters[key] = val
            } else {
                $scope.errorMsg = err
            }
        }


        $scope.validateTokenFilter = function (name, newTokenFilter, tokenFilters, tokenMaps) {
            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            if (!name) {
                return "Name is required"
            }

            if (name != "" && tokenFilters[name]) {
                return "Token filter named '" + name + "' already exists"
            }

            if (!("type" in newTokenFilter)) {
                return "Token filter named '" + name + "' must have a type"
            }

            switch (newTokenFilter.type) {
                case "dict_compound":
                    if (!("dict_token_map" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'dict_token_map'"
                    }
                    if (!($scope.tokenMapNames.includes(newTokenFilter.dict_token_map))) {
                        return "Token filter named '" + name + "' has unknown token sub words map"
                    }
                    break
                case "edge_ngram":
                    if (!("back" in newTokenFilter) || !("min" in newTokenFilter) || !("max" in newTokenFilter) || Object.keys(newTokenFilter).length != 4) {
                        return "Token filter named '" + name + "' must have fields 'type', 'back', 'min' and 'max'"
                    }

                    if (newTokenFilter.min > newTokenFilter.max) {
                        return "Token filter named '" + name + "' must have max >= min"
                    }
                    break
                case "elision":
                    if (!("articles_token_map" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'articles_token_map'"
                    }

                    if (!($scope.tokenMapNames.includes(newTokenFilter.articles_token_map))) {
                        return "Token filter named '" + name + "' has unknown article map"
                    }
                    break
                case "keyword_marker":
                    if (!("keywords_token_map" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'keywords_token_map'"
                    }

                    if (!($scope.tokenMapNames.includes(newTokenFilter.keywords_token_map))) {
                        return "Token filter named '" + name + "' has unknown keyword map"
                    }
                    break
                case "length":
                    if (!("min" in newTokenFilter) || !("max" in newTokenFilter) || Object.keys(newTokenFilter).length != 3) {
                        return "Token filter named '" + name + "' must have fields 'type', 'min' and 'max'"
                    }

                    if (newTokenFilter.min > newTokenFilter.max) {
                        return "Token filter named '" + name + "' must have max >= min"
                    }
                    break
                case "ngram":
                    if (!("min" in newTokenFilter) || !("max" in newTokenFilter) || Object.keys(newTokenFilter).length != 3) {
                        return "Token filter named '" + name + "' must have fields 'type', 'min' and 'max'"
                    }

                    if (newTokenFilter.min > newTokenFilter.max) {
                        return "Token filter named '" + name + "' must have max >= min"
                    }
                    break
                case "normalize_unicode":
                    if (!("form" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'form'"
                    }

                    if (!(newTokenFilter.form == "nfc" || newTokenFilter.form == "nfd" || newTokenFilter.form == "nfkc" || newTokenFilter.form == "nfkd")) {
                        return "Token filter named '" + name + "' must have form value be nfc, nfd, nfkc or nfkd"
                    }
                    break
                case "shingle":
                    if (!("min" in newTokenFilter) || !("max" in newTokenFilter) || !("output_original" in newTokenFilter) || !("separator" in newTokenFilter) || !("filler" in newTokenFilter) || Object.keys(newTokenFilter).length != 6) {
                        return "Token filter named '" + name + "' must have fields 'min', 'max', 'output_original', 'separator', 'filter' and 'type'"
                    }

                    if (newTokenFilter.min > newTokenFilter.max) {
                        return "Token filter named '" + name + "' must have max >= min"
                    }
                    break
                case "normalize_unicode":
                    if (!("form" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'form'"
                    }

                    if (!(newTokenFilter.form == "nfc" || newTokenFilter.form == "nfd" || newTokenFilter.form == "nfkc" || newTokenFilter.form == "nfkd")) {
                        return "Token filter named '" + name + "' must have form value be nfc, nfd, nfkc or nfkd"
                    }
                    break
                case "stop_tokens":
                    if (!("stop_token_map" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'keywords_token_map'"
                    }

                    if (!($scope.tokenMapNames.includes(newTokenFilter.stop_token_map))) {
                        return "Token filter named '" + name + "' has unknown keyword map"
                    }
                    break
                case "truncate_token":
                    if (!("length" in newTokenFilter) || Object.keys(newTokenFilter).length != 2) {
                        return "Token filter named '" + name + "' must have fields 'type' and 'keywords_token_map'"
                    }

                    if (newTokenFilter.length <= 0) {
                        return "Token filter named '" + name + "' must have positive length"
                    }
                    break
                default:
                    return "Token filter named '" + name + "' has unknown type"
            }

            let tokenfilters = {}
            tokenfilters[name] = $scope.tokenfilter

            let testMapping = {
                "analysis": {
                    "token_filters": tokenfilters,
                    "token_maps": tokenMaps
                }
            }

            http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
            then(function() {}, function(response) {
                $scope.errorMsg = response.data
            })

            return ""
        }

        $scope.parseDateTimeParser = function(key, val) {
            var err = $scope.validateDateTimeParser(key, val, $scope.indexMapping.analysis.date_time_parsers)
            if (err == "") {
                $scope.indexMapping.analysis.date_time_parsers[key] = val
                $scope.dateTimeParserNames.push(key)
            } else {
                $scope.errorMsg = err
            }
        }

        $scope.validateDateTimeParser = function(name, newDateTimeParser, dateTimeParsers) {
            if (!name) {
                return "Date time parser name is required"
            }

            if (name != "" && dateTimeParsers[name]) {
                return "Date time parser named '" + name + "' already exists"
            }

            if (!("layouts" in newDateTimeParser)) {
                return "Date time parser named '" + name + "' must have atleast one layout"
            }

            if (newDateTimeParser.layouts.length <= 0) {
                return "Date time parser named '" + name + "' must have atleast one layout"
            }

            return ""
        }

        $scope.parseAnalyzer = function (key, val) {

            var err = $scope.validateAnalyzer(key, val, $scope.indexMapping.analysis)
            if (err == "") {
                $scope.indexMapping.analysis.analyzers[key] = val
                $scope.analyzerNames.push(key)
            } else {
                $scope.errorMsg = err
            }
        }

        $scope.validateAnalyzer = function (name, newAnalyzer, analysis) {

            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            if (!name) {
                return "Analyzer name is required"
            }

            if (name != "" && analysis.analyzers[name]) {
                return "Analyzer named '" + name + "' already exists"
            }

            let testAnalysis = {}
            for (var ak in analysis) {
                testAnalysis[ak] = analysis[ak]
            }

            let testAnalyzers = {}
            testAnalyzers[name] = newAnalyzer
            testAnalysis["analyzers"] = testAnalyzers

            let testMapping = {
                "analysis": testAnalysis
            }

            http.post('/api/_validateMapping', bleveIndexMappingScrub(testMapping)).
            then(function() {}, function(response) {
                $scope.errorMsg = response.data
            })

            return ""
        }

        $scope.parseScope = function(name) {
            var scopeCollection = name.split(".")
            if (scopeCollection.length == 2 && $scope.docConfigCollections) {
                $scope.newScopeName = scopeCollection[0]
                if ($scope.collectionsSelected) {
                    if (!$scope.collectionsSelected.includes(scopeCollection[1])) {
                        $scope.collectionsSelected.push(scopeCollection[1])
                    }
                } else {
                    $scope.collectionsSelected = [scopeCollection[1]]
                }
            }
        }

        $scope.parseMapping = function(name, value, parentMapping) {

            if (parentMapping == null) {
                $scope.addChildMapping(null)
                var mapping = $scope.mappings[0]
            } else {
                if ("fields" in value) {
                    $scope.addChildField(parentMapping)
                    var mapping = parentMapping.fields[0]
                } else {
                    $scope.addChildMapping(parentMapping)
                    var mapping = parentMapping.mappings[0]
                }
            }

            if ("fields" in value) {
                for (let i = 0; i < value.fields.length; i++) {

                    mapping.property = name
                    $scope.changedProperty(mapping, parentMapping)
                    if ("name" in value.fields[i]) {
                        mapping.name = value.fields[i].name
                        $scope.validateField(mapping, parentMapping)
                    }

                    if ("type" in value.fields[i]) {
                        if ($scope.fieldTypes.includes(value.fields[0].type)) {
                            mapping.type = value.fields[0].type
                        } else {
                            $scope.errorMsg = "Field named '" + name + "' has invalid value for field type"
                            return
                        }
                    }

                    if ("analyzer" in value.fields[i] && mapping.type == "text") {
                        if ($scope.analyzerNames.includes(value.default_analyzer)) {
                            mapping.analyzer = value.fields[0].analyzer
                        } else {
                            $scope.errorMsg = "Field named '" + name + "' has invalid value for field analyzer"
                            return
                        }
                    }

                    if ("date_format" in value.fields[i] && mapping.type == "datetime") {
                        if ($scope.dateTimeParserNames.includes(value.default_analyzer)) {
                            mapping.date_format = value.fields[i].date_format
                        } else {
                            $scope.errorMsg = "Field named '" + name + "' has invalid value for field date_format"
                            return
                        }
                    }

                    if ("store" in value.fields[i]) {
                        mapping.store = value.fields[i].store
                    }

                    if ("index" in value.fields[i]) {
                        mapping.index = value.fields[i].index
                    }

                    if ("include_term_vectors" in value.fields[i] && mapping.type == "text") {
                        mapping.include_term_vectors = value.fields[i].include_term_vectors
                    }

                    if ("include_in_all" in value.fields[i]) {
                        mapping.include_in_all = value.fields[i].include_in_all
                    }


                    if ("docvalues" in value.fields[i] && !(mapping.type == "geopoint" || mapping.type == "geoshape")) {
                        mapping.docvalues = value.fields[i].docvalues
                    }

                    $scope.editAttrsDone(mapping, true)

                    if (i + 1 < value.fields.length) {
                        $scope.addChildField(parentMapping)
                        mapping = parentMapping.fields[0]
                    }
                }
            } else {
                mapping.name = name

                if ("default_analyzer" in value) {
                    if ($scope.analyzerNames.includes(value.default_analyzer)) {
                        mapping.default_analyzer = value.default_analyzer
                    }
                }

                $scope.editAttrsDone(mapping, true)
            }

            if ("properties" in value && mapping._kind != "field") {
                for (const [k, val] of Object.entries(value.properties)) {
                    $scope.parseMapping(k, val, mapping)
                }
            }
        }

        $scope.loadTokenMapNames = function() {

            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            http.post('/api/_tokenMapNames', bleveIndexMappingScrub($scope.indexMapping)).
            then(function(response) {
                $scope.tokenMapNames = response.data.token_maps
            }, function(response) {
                $scope.errorMsg = response.data
            })
        }

        $scope.loadAnalyzerNames = function() {

            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            http.post('/api/_analyzerNames', bleveIndexMappingScrub($scope.indexMapping)).
            then(function(response) {
                $scope.analyzerNames = response.data.analyzers
            }, function(response) {
                $scope.errorMsg = response.data
            })
        }

        $scope.loadDatetimeParserNames = function() {

            var http = prefixedHttp($http, '/../_p/' + ftsPrefix)

            http.post('/api/_datetimeParserNames', bleveIndexMappingScrub($scope.indexMapping)).
            then(function(response) {
                $scope.dateTimeParserNames = response.data.datetime_parsers
            }, function(response) {
                $scope.errorMsg = response.data
            })
        }

        $scope.loadAnalyzerNames()
        $scope.loadDatetimeParserNames()
        $scope.loadTokenMapNames()
    }

    $scope.importIndexJSON = function() {

        $scope.errorMsg = ""
        $uibModal.open({
            template: indexImportTemplate,
            animation: $scope.animationsEnabled,
            scope: $scope,
            controller: ImportIndexCtrl,
            resolve: {
                errorMsg: function() {
                    return $scope.errorMsg
                }
            }
        }).result.then(function(){})
    }
}

// -------------------------------------------------------

function IndexSearchCtrlFT_NS($scope, $http, $stateParams, $log, $sce,
                              $location, mnPermissions, qwDialogService) {
  var $httpPrefixed = prefixedHttp($http, '../_p/' + ftsPrefix, true);

  var $routeParams = $stateParams;

  $scope.indexName = $stateParams.indexName;
  let rv = obtainBucketScopeUndecoratedIndexName($scope.indexName);
  $scope.indexBucketName = rv[0];
  $scope.indexScopeName = rv[1];
  $scope.undecoratedIndexName = rv[2];
  $scope.isScopedIndexName = ($scope.indexName != $scope.undecoratedIndexName);

  $scope.indexDefs = null;
  $httpPrefixed.get('/api/index').then(function(rsp) {
        var data = rsp.data;
        $scope.indexDefs = data && data.indexDefs && data.indexDefs.indexDefs;
  });

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

    function fetchSourceNameForHit(hit) {
        try {
            if ($scope.indexDef.type == "fulltext-index" &&
                ($scope.indexDef.sourceType == "couchbase" || $scope.indexDef.sourceType == "gocbcore")) {
                return $scope.indexDef.sourceName;
            } else if ($scope.indexDef.type == "fulltext-alias") {
                if (hit.index.length > 0) {
                    // hit.index is the pindex name of format .. <index_name>_<hash>_<hash>
                    var n = hit.index.substring(0, hit.index.lastIndexOf("_")).lastIndexOf("_");
                    let ix = hit.index.substring(0, n); // this is the index name
                    for (var indexName in $scope.indexDefs) {
                        if (indexName == ix) {
                            return $scope.indexDefs[ix].sourceName;
                        }
                    }
                }
            }
        } catch (e) {}

        return "";
    };

    $scope.decorateSearchHit = function(hit) {
        hit.docIDLink = null;
        if (!$scope.indexDef) {
            return;
        }
        let sourceName = fetchSourceNameForHit(hit)
        if (sourceName.length == 0) {
            return;
        }
        try {
            if (($scope.permsCluster.bucket[sourceName] &&
                    ($scope.permsCluster.bucket[sourceName].data.read ||
                        $scope.permsCluster.bucket[sourceName].data.docs.read)) ||
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

    QueryCtrl($scope, $httpPrefixed, $routeParams, $log, $sce, $location, qwDialogService);

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
    let rv = obtainBucketScopeUndecoratedIndexName($scope.indexName);
    $scope.indexBucketName = rv[0];
    $scope.indexScopeName = rv[1];
    $scope.undecoratedIndexName = rv[2];
    $scope.isScopedIndexName = ($scope.indexName != $scope.undecoratedIndexName);

    $scope.jsonDetails = false;
    $scope.curlDetails = false;

    $scope.indexTab = 1;

    $scope.setIndexTab = function(newTab) {
        if (newTab === 2){
            $scope.startPoller()
        }
        $scope.indexTab = newTab;
    }

    $scope.isIndexTab = function(tabNum) {
        return $scope.indexTab === tabNum
    }

    IndexCtrl($scope, http, $routeParams,
              $location, $log, $sce, $uibModal);
}

// -------------------------------------------------------

angular.module(ftsAppName).
    controller('BleveAnalyzerModalCtrl',
               ["$scope", "$uibModalInstance", "$http",
                "name", "value", "mapping", "static_prefix", BleveAnalyzerModalCtrl_NS]).
    controller('BleveCharFilterModalCtrl',
               ["$scope", "$uibModalInstance", "$http",
                "name", "value", "mapping", "static_prefix", BleveCharFilterModalCtrl_NS]).
    controller('BleveTokenizerModalCtrl',
               ["$scope", "$uibModalInstance", "$http",
                "name", "value", "mapping", "static_prefix", BleveTokenizerModalCtrl_NS]).
    controller('BleveTokenFilterModalCtrl',
               ["$scope", "$uibModalInstance", "$http",
                "name", "value", "mapping", "static_prefix", BleveTokenFilterModalCtrl_NS]).
    controller('BleveWordListModalCtrl',
               ["$scope", "$uibModalInstance",
                "name", "words", "mapping", "static_prefix", BleveWordListModalCtrl_NS]).
    controller('BleveDatetimeParserModalCtrl',
               ["$scope", "$uibModalInstance",
                "name", "layouts", "mapping", "static_prefix", BleveDatetimeParserModalCtrl_NS]);

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
                         selectedTargetIndexes, isEdit) {
                    var aliasTargets = {};
                    if (isEdit) {
                        newIndexName=$scope.fullIndexName;
                    }
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

// -------------------------------------------------------

function ftsServiceHostPort($scope, $http, $location) {
    $scope.hostPort = $location.host() + ":FTS_PORT";
    $scope.protocol = $location.protocol();

    $http.get("/pools/default/nodeServices").then(function(resp) {
        var nodes = resp.data.nodesExt;
        for (var i = 0; i < nodes.length; i++) {
            if (nodes[i].services) {
                if ($location.protocol() == "https" && nodes[i].services.ftsSSL) {
                    var host = $location.host();
                    if (angular.isDefined(nodes[i].hostname)) {
                        host = nodes[i].hostname;
                    }
                    $scope.hostPort = host + ":" + nodes[i].services.ftsSSL;
                    return;
                } else if (nodes[i].services.fts) {
                    var host = $location.host();
                    if (angular.isDefined(nodes[i].hostname)) {
                        host = nodes[i].hostname;
                    }
                    $scope.hostPort = host + ":" + nodes[i].services.fts;
                    return;
                }
            }
        }
    });
}

// -------------------------------------------------------

// IndexNewCtrlFTEasy_NS is the controller for the create/edit easy mode
function IndexNewCtrlFTEasy_NS($scope, $http, $state, $stateParams,
                               $location, $log, $sce, $uibModal,
                               $q, mnBucketsService, mnPoolDefault, mnDocumentsService) {
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
            url: "/pools/default/buckets/" + encodeURIComponent(bucket) + "/scopes"
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
        });
    }

    function prepareDocForCodeMirror(doc) {
        $scope.parsedDocs.setDocForCollection($scope.collectionOpened, doc.json);
        var parsedDoc = $scope.parsedDocs.getParsedDocForCollection($scope.collectionOpened);
        return parsedDoc.getDocument();
    }

    function getSampleDocument(params) {
        return mnDocumentsService.getDocument(params).then(function (sampleDocument) {
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
                        return mnDocumentsService.getDocuments({
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
        $scope.collectionDynamic = false;
        $scope.collectionTextFieldsAsIdentifiers = false;
        $scope.collectionAnalyzer = "standard";

        $scope.collectionDynamicToggled = function() {
            $scope.collectionDynamic = "false";
            $scope.collectionTextFieldsAsIdentifiers = false;
            $scope.collectionAnalyzer = "standard";
            $scope.resetDynamic();
        };

        $scope.setCollectionAnalyzer = function(identifier, analyzer) {
            $scope.collectionAnalyzer = "standard";
            if (identifier) {
                $scope.collectionAnalyzer = "keyword";
            } else if (analyzer.length > 0) {
                $scope.collectionAnalyzer = analyzer;
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

        $scope.docConfigCollections = false;
        $scope.docConfigMode = "type_field";

        $scope.easyMappings = newEasyMappings();
        $scope.editFields = newEditFields();
        $scope.parsedDocs = newParsedDocs();
        $scope.easyLanguages = [
            {
                label: "Standard",
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
                label: "Croatian",
                id: "hr"
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
                label: "Hebrew",
                id: "he"
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

        $scope.collectionLabel = function(collectionName) {
            if ($scope.easyMappings.hasNonEmptyMapping(collectionName)) {
                return collectionName + " *";
            }
            return collectionName;
        }

        $scope.userSelectedFieldInCollection = function(collection, path, valType) {
            if (collection != $scope.collectionOpened) {
                $scope.expando(collection);
            }
            setTimeout(function() {
                $scope.userSelectedField(path, valType)
            }, 300);
        }

        $scope.userSelectedField = function(path, valType) {
            var cursor = $scope.editor.getCursor();
            var parsedDoc = $scope.parsedDocs.getParsedDocForCollection($scope.collectionOpened);
            var newRow = parsedDoc.getRow(path);
            if (newRow != cursor.line) {
                $scope.editor.focus();
                $scope.editor.setCursor({line: newRow, ch: 0})
                return;
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
                $scope.editField.includeInAll = false;
                $scope.editField.sortFacet = false;
                $scope.editField.date_format = "dateTimeOptional";
                if (valType === "boolean") {
                    $scope.editField.type = "boolean";
                } else if (valType === "number") {
                    $scope.editField.type = "number";
                } else if (valType === "geoshape") {
                    $scope.editField.type = "geoshape";
                } else if (valType === "IP") {
                    $scope.editField.type = "IP";
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

        $scope.resetDynamic = function() {
            $scope.easyMapping.resetDynamic();
        }
        $scope.makeDynamic = function() {
            $scope.easyMapping.makeDynamic($scope.collectionAnalyzer);
        }
        $scope.hasDynamicDefChanged = function() {
            return $scope.easyMapping.hasDynamicDefChanged($scope.collectionAnalyzer);
        }

        $scope.addField = function() {
            $scope.editField.new = false;
            $scope.easyMapping.addField($scope.editField);
        }

        $scope.hasFieldDefChanged = function() {
            return $scope.easyMapping.hasFieldDefChanged($scope.editField);
        }

        $scope.deleteFieldInCollection = function(collection, path) {
            if (!angular.isDefined(path) || path.length == 0) {
                // Deleting dynamic mapping
                confirmDialog(
                    $scope, $uibModal,
                    "Confirm Drop Dynamic Mapping",
                    "Warning: This will drop the dynamic mapping for collection: `" + collection + "`.",
                    "Drop"
                ).then(function success() {
                    $scope.collectionAnalyzer = "standard";
                    $scope.collectionTextFieldsAsIdentifiers = false;
                    $scope.collectionDynamic = false;
                    $scope.resetDynamic();
                });
                return;
            }

            confirmDialog(
                $scope, $uibModal,
                "Confirm Delete Field",
                "Warning: This will drop field at path `" + path + "` within collection `" + collection + "`.",
                "Delete Field"
            ).then(function success() {
                $scope.easyMappings.deleteFieldFromCollection(collection, path);
            });
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

        $scope.bucketChanged = function(selectedBucket) {
            let orig = $scope.newSourceName;
            if (orig) {
                confirmDialog(
                    $scope, $uibModal,
                    "Confirm Bucket Change",
                    "Warning: All configurations made with the current bucket will be lost.",
                    "Update Bucket"
                ).then(function success() {
                    listScopesForBucket(selectedBucket).then(function (scopes) {
                        $scope.scopeNames = scopes;
                        if (scopes.length > 0) {
                            $scope.newScopeName = scopes[0];
                        }
                        $scope.scopeChanged();
                    });

                    $scope.easyMappings = newEasyMappings();
                    $scope.editFields = newEditFields();
                    $scope.parsedDocs = newParsedDocs();
                }, function error() {
                    $scope.newSourceName = orig;
                });
            } else {
                $scope.newSourceName = selectedBucket;
                listScopesForBucket(selectedBucket).then(function (scopes) {
                    $scope.scopeNames = scopes;
                    if (scopes.length > 0) {
                        $scope.newScopeName = scopes[0];
                    }
                    $scope.scopeChanged();
                });

                $scope.easyMappings = newEasyMappings();
                $scope.editFields = newEditFields();
                $scope.parsedDocs = newParsedDocs();
            }
        };

        $scope.scopeChanged = function(selectedScope) {
            let orig = $scope.newScopeName;
            if (selectedScope && orig) {
                confirmDialog(
                    $scope, $uibModal,
                    "Confirm Scope Change",
                    "Warning: All configurations made with the current scope will be lost.",
                    "Update Scope"
                ).then(function success() {
                    $scope.newScopeName = selectedScope;
                    $scope.listCollectionsForBucketScope($scope.newSourceName,
                      selectedScope).then(function (collections) {
                        $scope.collectionNames = collections;
                        if (collections.length > 0) {
                            $scope.expando(collections[0]);
                        }
                    });

                    $scope.easyMappings = newEasyMappings();
                    $scope.editFields = newEditFields();
                    $scope.parsedDocs = newParsedDocs();
                }, function error() {
                    $scope.newScopeName = orig;
                });
            } else {
                $scope.listCollectionsForBucketScope($scope.newSourceName,
                    $scope.newScopeName).then(function (collections) {
                        $scope.collectionNames = collections;
                        if (collections.length > 0) {
                            $scope.expando(collections[0]);
                        }
                    });

                $scope.easyMappings = newEasyMappings();
                $scope.editFields = newEditFields();
                $scope.parsedDocs = newParsedDocs();
            }
        };

        $scope.expando = function(collectionName) {
            $scope.errorMessage = "";
            $scope.collectionOpened = collectionName;
            $scope.collectionDynamic = false;
            $scope.collectionAnalyzer = "";
            $scope.collectionTextFieldsAsIdentifiers = false;

            var sampleDocument = $scope.parsedDocs.getParsedDocForCollection(collectionName).getDocument();
            if (sampleDocument == '{}') {
                $scope.loadAnotherDocument($scope.newSourceName, $scope.newScopeName, collectionName);
            } else {
                $scope.sampleDocument = sampleDocument;
            }

            $scope.editField = $scope.editFields.getFieldForCollection(collectionName);
            $scope.easyMapping = $scope.easyMappings.getMappingForCollection(collectionName);

            if (angular.isDefined($scope.easyMapping)) {
                let dynamicAnalyzer = $scope.easyMapping.dynamicAnalyzer();
                if (dynamicAnalyzer.length > 0) {
                    $scope.collectionDynamic = true;
                    $scope.collectionAnalyzer = dynamicAnalyzer;
                    if (dynamicAnalyzer == "keyword") {
                        $scope.collectionTextFieldsAsIdentifiers = true;
                    }
                }
            }
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

                let newNameSuffix = newIndexName;
                let pos = newIndexName.lastIndexOf(".");
                if (pos > 0 && pos+1 < newIndexName.length) {
                    newNameSuffix = newIndexName.slice(pos+1);
                }

                if (!newNameSuffix) {
                    errorFields["indexName"] = true;
                    errs.push("index name is required");
                } else if ($scope.meta &&
                    $scope.meta.indexNameRE &&
                    !newNameSuffix.match($scope.meta.indexNameRE)) {
                    errorFields["indexName"] = true;
                    errs.push("index name '" + newNameSuffix + "'" +
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
                                       newPlanParams, prevIndexUUID, isEdit) {
                if (isEdit) {
                    newIndexName=$scope.fullIndexName;
                }
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

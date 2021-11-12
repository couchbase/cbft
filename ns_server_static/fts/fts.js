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

import {IndexesCtrl, IndexCtrl, IndexNewCtrl} from "./static/index.js";
import {errorMessage, confirmDialog, alertDialog} from "./static/util.js";
import QueryCtrl from "./static/query.js";
import uiTree from "angular-ui-tree";

import {initCodeMirrorActiveLine} from "./codemirror-active-line.js";
import {newParsedDocs} from "./fts_easy_parse.js";
import {newEditFields, newEditField} from "./fts_easy_field.js";
import {newEasyMappings, newEasyMapping} from "./fts_easy_mapping.js";

export default ftsAppName;

angular
    .module(ftsAppName,
            [uiRouter, ngClipboard, mnPermissions, uiTree, ngSortable, mnStatisticsNewService, 
                uiCodemirror, mnDocumentsService, mnSelect])
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
              parent: {name: 'Full Text Search', link: parent + '.fts_list'}
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
        controller('IndexDetailsCtrlFT_NS', IndexDetailsCtrlFT_NS).
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
            if (!done) {
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

    var rv = IndexesCtrl($scope, http, $routeParams, $log, $sce, $location, $uibModal);

    $scope.$on('$stateChangeStart', function() {
        done = true;
    });

    function isMappingIncompatibleWithQuickEditor(mapping) {
        if (!mapping.enabled ||
            mapping.dynamic ||
            (angular.isDefined(mapping.default_analyzer) && mapping.default_analyzer != "")) {
            // mapping is either disabled and/or dynamic and/or has default_analyzer
            // explicitly defined
            return true;
        }

        if ((!angular.isDefined(mapping.properties) || mapping.properties.length == 0) &&
            (!angular.isDefined(mapping.fields) || mapping.fields.length == 0)) {
            // mapping has no child mappings or fields defined within it
            return true;
        }

        for (var name in mapping.properties) {
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

                if (mapping.fields[0].type == "text") {
                    if (!mapping.fields[0].include_in_all ||
                        !angular.isDefined(mapping.fields[0].analyzer) ||
                        mapping.fields[0].analyzer == "") {
                        // text field has include_in_all disabled and/or no analyzer specified
                        return true;
                    }
                } else {
                    if (mapping.fields[0].include_in_all ||
                        (angular.isDefined(mapping.fields[0].analyzer) &&
                        mapping.fields[0].analyzer != "")) {
                        // fields of other types have include_in_all enabled or
                        // an analyzer set
                        return true;
                    }
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

        if (params.mapping.index_dynamic ||
            params.mapping.store_dynamic ||
            params.mapping.docvalues_dynamic ||
            params.mapping.default_analyzer != "standard" ||
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

// -------------------------------------------------------

function IndexCtrlFT_NS($scope, $http, $stateParams, $state,
                        $location, $log, $sce, $uibModal) {
    var $routeParams = $stateParams;

    var http = prefixedHttp($http, '../_p/' + ftsPrefix);

    $scope.progressPct = "--";
    $scope.docCount = "";
    $scope.totSeqReceived = "";
    $scope.prevTotSeqReceived = "";
    $scope.numMutationsToIndex = "";
    $scope.httpStatus = 200;    // OK
    $scope.showHiddenUI = false;

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
                $scope.totSeqReceived = data["tot_seq_received"];
                $scope.numMutationsToIndex = data["num_mutations_to_index"];
                updateProgressPct();
            }
        }, function(response) {
            $scope.httpStatus = response.Status;
            updateProgressPct();
        });

        if (callback) {
            return callback($scope.httpStatus);
        }
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

    function updateProgressPct() {
        var docCount = parseInt($scope.docCount);
        var numMutationsToIndex = parseInt($scope.numMutationsToIndex);
        var totSeqReceived = parseInt($scope.totSeqReceived);
        var prevTotSeqReceived = parseInt($scope.prevTotSeqReceived);
        let prog = 0.0;

        if (totSeqReceived == 0 && prevTotSeqReceived == 0) {
            prog = 100.0;
        } else if ((totSeqReceived == prevTotSeqReceived) &&
            numMutationsToIndex > 0 && docCount > 0) {
            // if the totSeqReceived is equal to the one recorded in the
            // previous iteration, check if we can determine the progress
            // based on the docCount and numMutationsToIndex.
            prog = ((1.0 * docCount) / (docCount + numMutationsToIndex)) * 100.0;
        } else if (totSeqReceived > 0 && prevTotSeqReceived >= 0) {
            // determine progressPct relative to the current
            // totSeqReceived and the prevTotSeqReceived.
            prog = ((1.0 * prevTotSeqReceived) / totSeqReceived) * 100.0;
        }

        $scope.progressPct = prog.toPrecision(4);
        $scope.prevTotSeqReceived = $scope.totSeqReceived;
    }

    IndexCtrl($scope, http, $routeParams,
              $location, $log, $sce, $uibModal);

    if ($scope.tab === "summary") {
        $scope.loadProgressStats();
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

        $scope.indexStoreOptions = ["Version 5.0 (Moss) (deprecated)", "Version 6.0 (Scorch)"];
        $scope.indexStoreOption = $scope.indexStoreOptions[1];

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

        $scope.updateBucketDetails = function(selectedBucket) {
            listScopesForBucket(selectedBucket || $scope.newSourceName).then(function (scopes) {
                $scope.newScopeName = initScopeName($scope.newIndexParams['fulltext-index']);
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
                              $location, mnPermissions, qwDialogService) {
  var $httpPrefixed = prefixedHttp($http, '../_p/' + ftsPrefix, true);

  var $routeParams = $stateParams;

  $scope.indexName = $stateParams.indexName;

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

        $scope.deleteFieldInCollection = function(collection, path) {
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

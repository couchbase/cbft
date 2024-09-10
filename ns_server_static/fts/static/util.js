//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

import initBleveIndexMappingController from "../static-bleve-mapping/js/mapping/index-mapping.js";

import confirmDialogTemplate from "../confirm_dialog.html";
import alertDialogTemplate from "../alert_dialog.html";

export {errorMessage, confirmDialog, alertDialog, obtainBucketScopeUndecoratedIndexName, 
        loadStateFromStorage, saveStateToStorage};
export {blevePIndexInitController, blevePIndexDoneController};

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

confirmDialog.$inject = ["$scope", "$uibModal", "title", "desc", "confirmMessage"];
function confirmDialog($scope, $uibModal, title, desc, confirmMessage) {
    var innerScope = $scope.$new(true);
    innerScope.title = title;
    innerScope.desc = desc;
    innerScope.confirmMessage = confirmMessage;

    return $uibModal.open({
        template: confirmDialogTemplate,
        scope: innerScope
    }).result;
}

confirmDialog.$inject = ["$scope", "$uibModal", "title", "desc"];
function alertDialog($scope, $uibModal, title, desc) {
    var innerScope = $scope.$new(true);
    innerScope.title = title;
    innerScope.desc = desc;
    return $uibModal.open({
        template: alertDialogTemplate,
        scope: innerScope
    }).result;
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

blevePIndexInitController.$inject = ["initKind", "indexParams", "indexUI",
    "$scope", "$http", "$routeParams", "$location", "$log", "$sce", "$uibModal"];
function blevePIndexInitController(initKind, indexParams, indexUI,
    $scope, $http, $routeParams, $location, $log, $sce, $uibModal) {

    if (initKind == "edit" || initKind == "create") {
        $scope.replicaOptions = [0];
        $scope.numReplicas = $scope.replicaOptions[0];
        $scope.numPIndexes = 0;
        $scope.collectionsSupport = false;
        $scope.scopedIndexesSupport = false;
        $http.get('/api/conciseOptions').
        then(function(response) {
            var maxReplicasAllowed = parseInt(response.data.maxReplicasAllowed);
            $scope.replicaOptions = [];
            for (var i = 0; i <= maxReplicasAllowed; i++) {
                $scope.replicaOptions.push(i);
            }

            $scope.collectionsSupport = response.data.collectionsSupport;
            $scope.scopedIndexesSupport = response.data.scopedIndexesSupport;

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

            if ($scope.newIndexType != "fulltext-alias") {
                if ($scope.newPlanParams) {
                    try {
                        var newPlanParamsObj = JSON.parse($scope.newPlanParams);

                        $scope.numReplicas = $scope.replicaOptions[newPlanParamsObj["numReplicas"] || 0];
                        delete newPlanParamsObj["numReplicas"];
                        $scope.newPlanParams = JSON.stringify(newPlanParamsObj, undefined, 2);

                        if (angular.isDefined(newPlanParamsObj["indexPartitions"])) {
                            $scope.numPIndexes = newPlanParamsObj["indexPartitions"];
                        } else {
                            $scope.numPIndexes = 1;
                        }
                    } catch (e) {
                        console.log("blevePIndexInitController numPlanParams", initKind, e)
                    }
                }

                var deploymentModel = response.data.deploymentModel;
                if (deploymentModel == "serverless") {
                    $scope.numPIndexes = 1;
                    $scope.numReplicas = 1;
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

    var previewPrev = "";

    $scope.getScopeForIndex = function(docConfigMode, mapping) {
        if (docConfigMode.startsWith("scope.collection.")) {
            if (mapping.default_mapping.enabled) {
                return "_default";
            }
            if (angular.isDefined(mapping.types)) {
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

    $scope.getBucketScopeForAlias = function(targets) {
        var rv = "";
        for (let k in targets) {
            let pos = k.lastIndexOf(".");
            if (pos > 0) {
                var bucketDotScope = k.substring(0, pos);
                if (rv == "") {
                    rv = bucketDotScope;
                } else if (rv != bucketDotScope) {
                    return "";
                }
            } else {
                return "";
            }
        }
        return rv;
    }

    function updatePreview() {
        var done = $location.path().match(/_list$/);
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

            if ($scope.newIndexType == "fulltext-index") {
                if (angular.isDefined(rv.indexDef) &&
                    angular.isDefined(rv.indexDef.params) &&
                    angular.isDefined(rv.indexDef.params.doc_config) &&
                    angular.isDefined(rv.indexDef.params.mapping)) {
                    if (rv.indexDef.params.doc_config.mode.startsWith("scope.collection.")) {
                        let scopeName = $scope.getScopeForIndex(rv.indexDef.params.doc_config.mode, rv.indexDef.params.mapping);
                        if (scopeName.length > 0 && scopeName != $scope.newScopeName) {
                            $scope.errorMessage =
                                "scope selected `" + $scope.newScopeName + "`, mappings use `" + scopeName + "`";
                            $scope.scopeMismatch = true;
                        } else {
                            $scope.scopeMismatch = false;
                            if ($scope.errorMessage != null && $scope.errorMessage.startsWith("scope selected")) {
                                // reset a previous scope mismatch error
                                $scope.errorMessage = "";
                            }
                        }

                        let mapping = rv.indexDef.params.mapping;
                        if (angular.isDefined(mapping.default_mapping) && mapping.default_mapping.enabled) {
                            $scope.collectionsSelected = ["_default"];
                            $scope.scopeSelected = "_default";
                        } else if (angular.isDefined(mapping.types)) {
                            let collectionNames = [];
                            for (let [key, value] of Object.entries(mapping.types)) {
                                if (value.enabled) {
                                    try {
                                        let scopeName = key.split(".")[0];
                                        if (scopeName.length > 0) {
                                            $scope.scopeSelected = scopeName;
                                        }
                                        let collName = key.split(".")[1];
                                        if (collName.length > 0 && collectionNames.indexOf(collName) < 0) {
                                            collectionNames.push(collName);
                                        }
                                    } catch (e) {}
                                }
                            }
                            $scope.collectionsSelected = collectionNames;
                        } else {
                            $scope.collectionsSelected = [];
                        }
                    } else {
                        $scope.scopeMismatch = false;
                        if ($scope.errorMessage != null && $scope.errorMessage.startsWith("scope selected")) {
                            // reset a previous scope mismatch error
                            $scope.errorMessage = "";
                        }
                        $scope.scopeSelected = "_default";
                        $scope.collectionsSelected = ["_default"];
                    }
                }
            }

            setTimeout(updatePreview, bleveUpdatePreviewTimeoutMS);
        }
    }

    setTimeout(updatePreview, bleveUpdatePreviewTimeoutMS);

    $scope.indexDefChanged = function(origIndexDef, isDraft) {
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

            // since draft indexes are saved as is and not sent to the server,
            // we do not need to perform these special steps for them to check
            // if the index definition has changed. 
            if (!isDraft) {
                try {
                    // Add an empty "analysis" section if no analysis elements defined.
                    if (!angular.isDefined(rv.indexDef["params"]["mapping"]["analysis"])) {
                        rv.indexDef["params"]["mapping"]["analysis"] = {};
                    }
                    // Delete "numReplicas" if set to 0.
                    if (angular.isDefined(rv.indexDef["planParams"]["numReplicas"]) &&
                        rv.indexDef["planParams"]["numReplicas"] == 0) {
                        delete rv.indexDef["planParams"]["numReplicas"];
                    }
                    // Delete "empty" fields array if present in type mappings objects.
                    for (var name in rv.indexDef["params"]["mapping"]["types"]) {
                        if (angular.isDefined(rv.indexDef["params"]["mapping"]["types"][name]["fields"]) &&
                            rv.indexDef["params"]["mapping"]["types"][name]["fields"].length == 0) {
                            delete rv.indexDef["params"]["mapping"]["types"][name]["fields"];
                        }
                    }
                } catch (e) {
                }
                if (rv.indexDef["type"] == "fulltext-alias") {
                    delete rv.indexDef["sourceUUID"];
                }
                // Drop "name" from the original and built index definition,
                // to account global vs scoped naming. Index name changes
                // are not allowed once created anyway.
                delete origIndexDef["name"];
                delete rv.indexDef["name"];
            }
            if (angular.equals(origIndexDef, rv.indexDef)) {
                return false;
            }
        } // Else could not retrieve the index definition, permit the update.

        return true;
    };
}

var bleveUpdatePreviewTimeoutMS = 1000;
blevePIndexDoneController.$inject = ["doneKind", "indexParams", "indexUI",
                                     "$scope", "$http", "$routeParams", "$location",
                                     "$log", "$sce", "$uibModal"];
function blevePIndexDoneController(doneKind, indexParams, indexUI,
    $scope, $http, $routeParams, $location, $log, $sce, $uibModal) {
    if (indexParams) {
        if ($scope.easyMappings) {
            indexParams.mapping = $scope.easyMappings.getIndexMapping($scope.newScopeName);
        } else {
            indexParams.mapping = $scope.bleveIndexMapping();
        }
    }
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

// obtainBucketScopeUndecoratedIndexName retrieves the bucket, scope
// and the user chosen name for the index from a scoped index name.
// If the index were a global one, just the name is returned.
function obtainBucketScopeUndecoratedIndexName(indexName) {
    if (!angular.isDefined(indexName)) {
        return ["", "", indexName];
    }

    let lastDotIndex = indexName.lastIndexOf(".");
    if (lastDotIndex < 0) {
        return ["", "", indexName];
    }

    let secondDotIndex = indexName.lastIndexOf(".", lastDotIndex - 1);
    if (secondDotIndex < 0) {
        return ["", "", indexName];
    }

    return [
        indexName.slice(0, secondDotIndex),
        indexName.slice(secondDotIndex + 1, lastDotIndex),
        indexName.slice(lastDotIndex + 1)
    ];
}

var hasLocalStorage = supportsHtml5Storage();
var localStorageKey = 'CouchbaseFTS_' + window.location.host

function supportsHtml5Storage() {
    try {
        return 'localStorage' in window && window['localStorage'] !== null;
    } catch (e) {
        return false;
    }
}

function loadStateFromStorage() {
    if (hasLocalStorage && typeof localStorage[localStorageKey] === 'string') {
        try {
            return JSON.parse(localStorage[localStorageKey]);
        } catch (err) {
            console.log("Error loading state from local storage", err);
        }
    }
}

function saveStateToStorage(state) {
    // nop if we don't have local storage
    if (!hasLocalStorage)
        return;
    try {
        localStorage[localStorageKey] = JSON.stringify(state);
    } catch (err) {
        console.log("Error saving state to local storage", err);
    }
}

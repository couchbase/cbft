//  Copyright 2023-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
export default BleveDocumentFilterModalCtrl;
BleveDocumentFilterModalCtrl.$inject = ["$scope", "$modalInstance",
                                        "type", "value", "docConfig", "mapping"];
function BleveDocumentFilterModalCtrl($scope, $modalInstance,
                                        type, value, docConfig, mapping) {
    $scope.type = type;
    $scope.origType = type;
    $scope.errorMessage = "";
    $scope.docConfig = docConfig;
    $scope.mapping = mapping;
    $scope.docFilterEditor = null;
    $scope.docFilterEditorError = null;
    $scope.docFilterEditorInitValue = "";
    try {
        if (!$scope.isObjectEmpty(value)) {
            $scope.docFilterEditorInitValue = JSON.stringify(value, null, 2);
        }
    } catch (e) {
        $scope.errorMessage = "Error parsing JSON: " + e;
    }

    $scope.updateType = function(i) {
        $scope.type = i;
    }

    $scope.docFilterAceLoaded = function(editor) {
        editor.renderer.setPrintMarginColumn(false);
        editor.setSelectionStyle("json");
        editor.getSession().on("changeAnnotation", function() {
            var annot_list = editor.getSession().getAnnotations();
            if (annot_list && annot_list.length) for (var i=0; i < annot_list.length; i++)
              if (annot_list[i].type == "error") {
                $scope.docFilterEditorError = "Error on row: " + annot_list[i].row + ": " + annot_list[i].text;
                return;
              }
            if (editor) {
                $scope.docFilterEditorError = null;
            }
          });
        if (/^((?!chrome).)*safari/i.test(navigator.userAgent))
          editor.renderer.scrollBarV.width = 20; // fix for missing scrollbars in Safari
        editor.setValue($scope.docFilterEditorInitValue, -1);
        editor.setReadOnly($scope.viewOnlyModal===true);
        $scope.docFilterEditor = editor;
    }

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };

    $scope.build = function(type) {
        if (!type) {
            $scope.errorMessage = "Name is required";
            return;
        }
        // document filter for type must not already be defined
        if (!$scope.isObjectEmpty($scope.ftsDocConfig.doc_filter) && 
             $scope.ftsDocConfig.doc_filter[type] && 
             type != $scope.origType) {
                $scope.errorMessage = "Document Filter '" + type + "' already exists";
                return;
        }
        try {
            $scope.returnValue = JSON.parse($scope.docFilterEditor.getValue());
        } catch (e) {
            $scope.errorMessage = "Error parsing JSON: " + e;
            return;
        }
        let result = {};
        result[type] = $scope.returnValue 
        $modalInstance.close(result);
    };
}
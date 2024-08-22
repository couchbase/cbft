//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
export default TestAnalyzerModalCtrl;
TestAnalyzerModalCtrl.$inject =
  ["$uibModalInstance", "$scope", "analyzerName", "analyzerList", "textValue", "indexMapping", "httpClient"];
function TestAnalyzerModalCtrl($uibModalInstance, $scope, analyzerName, analyzerList, textValue, indexMapping, httpClient) {
    $scope.analyzerName = analyzerName;
    $scope.analyzerList = analyzerList;
    $scope.indexMapping = indexMapping;
    if (textValue == "" || textValue == null) {
        textValue = "The quick brown fox jumps over the lazy dog.";
    }
    $scope.origTextValue = textValue;
    $scope.currTextValue = $scope.origTextValue;
    $scope.lastAnalyzedText = "";
    $scope.lastUsedAnalyzer = "";
    $scope.resetEnabled = false;
    $scope.analyzeEnabled = false;
    $scope.isResetting = false;

    $scope.cancel = function() {
        $uibModalInstance.close({})
    }

    $scope.analyze = function() {
        $scope.currTextValue = $scope.inputEditor.getValue();
        $scope.analyzeInputText().then(function(result) {
            $scope.outputEditor.setValue(result, -1);
        })
        $scope.lastAnalyzedText = $scope.currTextValue;
        $scope.lastUsedAnalyzer = $scope.analyzerName;
        $scope.checkChanges();
    }

    $scope.reset = function() {
        $scope.isResetting = true;
        $scope.currTextValue = $scope.origTextValue;
        $scope.analyzeInputText().then(function(result) {
            $scope.outputEditor.setValue(result, -1);
        })
        $scope.lastAnalyzedText = $scope.currTextValue;
        $scope.lastUsedAnalyzer = $scope.analyzerName;
        $scope.inputEditor.setValue($scope.origTextValue);
        $scope.isResetting = false;
        $scope.inputEditor.focus();
        $scope.checkChanges();
    }

    $scope.onAnalyzerChange = function(analyzer) {
        $scope.analyzerName = analyzer;
        $scope.checkChanges();
    }

    $scope.decodeBase64 = function(base64) {
        const text = atob(base64);
        const length = text.length;
        const bytes = new Uint8Array(length);
        for (let i = 0; i < length; i++) {
            bytes[i] = text.charCodeAt(i);
        }
        const decoder = new TextDecoder(); // default is utf-8
        return decoder.decode(bytes);
    }

    $scope.analyzeInputText = async function() {
        try {
            let response = await httpClient.post('/api/_analyze', {
                analyzer: $scope.analyzerName,
                text: $scope.currTextValue,
                mapping: $scope.indexMapping
            });
            response.data.token_stream.forEach(obj => {
                if (obj.term) {
                  try {
                    obj.term = $scope.decodeBase64(obj.term);
                  } catch (e) {
                    return 'Error analyzing text: ' + e;
                  }
                }
              });
            return JSON.stringify(response.data.token_stream, null, 2);
        } catch (error) {
           return 'Error analyzing text: ' + JSON.stringify(error);
        }
    };

    $scope.checkChanges = function() {
        var currentText = ace.edit('inputText').getValue();
        $scope.resetEnabled = currentText !== $scope.origTextValue;
        $scope.analyzeEnabled = (currentText !== $scope.lastAnalyzedText || $scope.analyzerName !== $scope.lastUsedAnalyzer) &&
            currentText !== "" &&
            currentText !== null &&
            currentText !== undefined;
    };

    $scope.aceInputOnLoad = function(editor) {
        editor.renderer.setPrintMarginColumn(false);
        editor.setSelectionStyle("text");
        if (/^((?!chrome).)*safari/i.test(navigator.userAgent))
            editor.renderer.scrollBarV.width = 20; // fix for missing scrollbars in Safari
        editor.setValue($scope.currTextValue); // moves cursor to the start
        $scope.inputEditor = editor;
        var outputEditor = ace.edit("analyzedTokens");
        $scope.aceOutputOnLoad(outputEditor);
        $scope.analyzeInputText().then(function(result) {
            $scope.outputEditor.setValue(result, -1);
        })
        $scope.lastAnalyzedText = $scope.currTextValue;
        $scope.lastUsedAnalyzer = $scope.analyzerName;
        editor.getSession().on('change', function() {
            if (!$scope.isResetting) {
                $scope.$apply(function() {
                    $scope.checkChanges();
                });
            }
        });
        editor.focus();
    };

    $scope.aceOutputOnLoad = function(editor) {
        editor.renderer.setPrintMarginColumn(false);
        editor.setSelectionStyle("json");
        if (/^((?!chrome).)*safari/i.test(navigator.userAgent))
            editor.renderer.scrollBarV.width = 20; // fix for missing scrollbars in Safari
        editor.setValue("", -1); // moves cursor to the start
        editor.setReadOnly(true);
        $scope.outputEditor = editor;
    };

    $scope.getAnalyzerLabel = function(selectedId) {
        if (!$scope.easyLanguages || !$scope.easyLanguages.length || !selectedId) {
            return selectedId;
        }
        var selectedItem = $scope.easyLanguages.find(function(item) {
            return item.id === selectedId;
        });
        return selectedItem ? selectedItem.label : selectedId;
    };
}

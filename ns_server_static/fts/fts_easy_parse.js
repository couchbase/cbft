//  Copyright (c) 2020 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

function newParsedDocs() {
    var parsedDocs = {};

    return {
        getParsedDocForCollection: function(collectionName) {
            if (!(collectionName in parsedDocs)) {
                parsedDocs[collectionName] = parseDocument('{}');
            }
            return parsedDocs[collectionName];
        },
        setDocForCollection: function(collectionName, src) {
            parsedDocs[collectionName] = parseDocument(src);
        }
    }
}

function parseDocument(doc) {

    var parseStack = [];
    var keysStack = [];
    var rowPaths = [];
    var rowTypes = [];
    var count = 0;
    var valsSincePop = 0;

    var replacer = function(key, value) {
        // on closing brace, pop stack, duplicate last entry for rowpaths
        while ((parseStack.length > 0) && (this !== parseStack[parseStack.length-1])) {
            parseStack.pop();
            keysStack.pop();

            if (valsSincePop > 1) {
                rowPaths.push(rowPaths[rowPaths.length - 1]);
                rowTypes.push(rowTypes[rowTypes.length - 1]);
                valsSincePop--;
            }
        }
        var ks = keysStack;
        var keysCopy = [];

        // copy non-numeric elements from keys stack
        // this builds correct path traversing arrays
        for (var i = 0; i < ks.length; i++) {
            if (isNaN(ks[i])) {
                keysCopy.push(ks[i]);
            }
        }
        if (isNaN(key)) {
            keysCopy.push(key);
        }
        var fullPath = keysCopy.join(".");

        // if object, push onto stack
        var valType = typeof value;
        if (valType === "object") {
            parseStack.push(value);
            keysStack.push(key);
            valsSincePop = 0;
        }

        // for any row other than the first, push the path onto rowPaths
        if (count != 0) {
            rowPaths.push(fullPath);
            rowTypes.push (valType);
        }
        // this is the second row, push an extra to fix up opening brace case
        if (count == 1) {
            rowPaths.push(fullPath);
            rowTypes.push (valType);
        }

        count++;
        valsSincePop++;
        return value;
    };

    var parsedObj;
    try {
        parsedObj = JSON.parse(doc)
    } catch (e) {
        console.log("error parsing json", e)
    }

    var docString = JSON.stringify(parsedObj || "", replacer, 2);

    while (parseStack.length > 0) {
        parseStack.pop();
        keysStack.pop();
        rowPaths.push(rowPaths[rowPaths.length-1]);
        rowTypes.push (rowTypes[rowTypes.length-1]);
    }

    return {
        getPath: function(col) {
            return rowPaths[col];
        },
        getType: function(col) {
            return rowTypes[col];
        },
        getDocument: function() {
            return docString;
        },
        getRow: function(path) {
            for (var i = 1; i < rowPaths.length; i++) {
                if (rowPaths[i] == path) {
                    return i;
                }
            }
            return 1;
        }
    };
}

export {newParsedDocs};

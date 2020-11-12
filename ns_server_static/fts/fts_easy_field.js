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

function newEditFields() {
    var fields = {};

    return {
        getFieldForCollection: function(collectionName) {
            if (!(collectionName in fields)) {
                fields[collectionName] = newEditField();
            }
            return fields[collectionName];
        }
    }
}

function newEditField() {
    return {
        splitPathPrefixAndField: function() {
            let n = this.path.lastIndexOf(".");
            if (n < 0) {
                return ["", this.path];
            }
            return [this.path.substring(0, n+1), this.path.substring(n+1)];
        },
        pathPrefix: function(path) {
            return this.splitPathPrefixAndField(path)[0];
        },
        description: function() {
            var rv = "";
            if (this.type == "text") {
                if (this.analyzer == "keyword") {
                    rv = "keyword ";
                } else {
                    rv = this.analyzer + " text ";
                }
            } else if (this.type == "number") {
                rv = "number ";
            } else if (this.type == "datetime") {
                rv = "datetime ";
            } else if (this.type == "geopoint") {
                rv = "geopoint ";
            } else if (this.type == "boolean") {
                rv = "boolean ";
            }
            var supporting = [];
            if (this.store) {
                supporting.push("search results");
            }
            if (this.highlight) {
                supporting.push("highlighting");
            }
            if (this.phrase) {
                supporting.push("phrase matching");
            }
            if (this.sortFacet) {
                supporting.push("sorting and faceting");
            }
            if (supporting.length > 0) {
                rv += "supporting " + supporting.join(", ");
            }

            return rv;
        },
    }
}

export {newEditFields, newEditField};

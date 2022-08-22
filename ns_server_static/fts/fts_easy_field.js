//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
            if (!angular.isDefined(this.path)) {
                return ["", this.path];
            }
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
                if (!angular.isDefined(this.analyzer) || this.analyzer == "") {
                    rv = "text ";
                } else if (this.analyzer == "keyword") {
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
            } else if (this.type == "geoshape") {
                    rv = "geoshape ";
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
            if (this.includeInAll) {
                supporting.push("field agnostic search");
            }
            if (this.sortFacet) {
                supporting.push("sorting and faceting");
            }

            if (supporting.length > 0) {
                rv += "supporting: [" + supporting.join(", ") + "]";
            }

            return rv;
        },
    }
}

export {newEditFields, newEditField};

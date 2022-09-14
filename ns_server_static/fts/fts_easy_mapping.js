//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

import {newEditFields, newEditField} from "./fts_easy_field.js";

function newEasyMappings() {
    var mappings = {};

    return {
        collectionNamedInMapping: function() {
            let collectionNames = Object.keys(mappings);
            for (var collectionNameI in collectionNames) {
                let collectionName = collectionNames[collectionNameI];
                if (mappings[collectionName].numFields() > 0) {
                    return collectionName;
                }
            }
            return "";
        },
        hasNonEmptyMapping: function(collectionName) {
            if (collectionName in mappings) {
                if (mappings[collectionName].numFields() > 0) {
                    return true;
                }
            }
            return false;
        },
        deleteFieldFromCollection: function(collectionName, path) {
            if (collectionName in mappings) {
                mappings[collectionName].deleteField(path);
            }
        },
        getMappingForCollection: function(collectionName) {
            if (!(collectionName in mappings)) {
                mappings[collectionName] = newEasyMapping();
            }
            return mappings[collectionName];
        },
        loadFromMapping: function(indexMapping) {
            for (let typeName in indexMapping.types) {
                let scopeCollType = typeName.split(".", 2)
                let collectionName = scopeCollType[1];
                mappings[collectionName] = newEasyMapping();
                mappings[collectionName].loadFromMapping(indexMapping.types[typeName]);
            }
        },
        numCollections: function() {
            return Object.keys(mappings).length;
        },
        collectionNames: function() {
            return Object.keys(mappings);
        },
        collectionNamesNotEmpty: function() {
            let rv = new Array();
            let collectionNames = Object.keys(mappings);
            for (var collectionNameI in collectionNames) {
                let collectionName = collectionNames[collectionNameI];
                if (mappings[collectionName].numFields() > 0) {
                    rv.push(collectionName);
                }
            }
            return rv;
        },
        numFieldsInAllCollections: function() {
            let result = 0;
            let collectionNames = Object.keys(mappings);
            for (var collectionNameI in collectionNames) {
                let collectionName = collectionNames[collectionNameI];
                result += mappings[collectionName].numFields();
            }
            return result;
        },
        getIndexMapping: function(scopeName) {
            var indexMapping = {};
            indexMapping.type_field = "_type";
            indexMapping.default_type = "_default";
            indexMapping.default_analyzer = "standard";
            indexMapping.default_datetime_parser = "dateTimeOptional";
            indexMapping.default_field = "_all";
            indexMapping.store_dynamic = false;
            indexMapping.index_dynamic = false;
            indexMapping.docvalues_dynamic = false;
            indexMapping.analysis = {};
            indexMapping.types = {};
            indexMapping.default_mapping = {
                "dynamic": false,
                "enabled": false
            };

            let dynamicCollectionMappings = false;
            let collectionNames = Object.keys(mappings);
            for (var collectionNameI in collectionNames) {
                let collectionName = collectionNames[collectionNameI];
                if (mappings[collectionName].numFields() > 0) {
                    indexMapping.types[scopeName + '.' + collectionName] = mappings[collectionName].getMapping();
                }
                if (mappings[collectionName].dynamicAnalyzer().length > 0) {
                    dynamicCollectionMappings = true;
                }
            }

            if (dynamicCollectionMappings) {
                indexMapping.store_dynamic = true;
                indexMapping.index_dynamic = true;
                indexMapping.docvalues_dynamic = true;
            }

            return indexMapping;
        }
    };
}

function newEasyMapping() {
    var mapping = {};

    var newDocMapping = function() {
        var docMapping = {};
        docMapping.enabled = true;
        docMapping.dynamic = false;
        return docMapping;
    };

    var newTextField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "text";
        if (field.identifier) {
            fieldMapping.analyzer = "keyword";
        } else {
            fieldMapping.analyzer = field.analyzer;
        }
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.highlight || field.phrase) {
            fieldMapping.include_term_vectors = true;
        }
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docvalues = true;
        }
        return fieldMapping;
    };

    var newNumericField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "number";
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.highlight || field.phrase) {
            fieldMapping.include_term_vectors = true;
        }
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docvalues = true;
        }
        return fieldMapping;
    };

    var newDateTimeField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "datetime";
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.highlight || field.phrase) {
            fieldMapping.include_term_vectors = true;
        }
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docvalues = true;
        }
        fieldMapping.date_format = field.date_format;
        return fieldMapping;
    };

    var newGeoPointField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "geopoint";
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.highlight || field.phrase) {
            fieldMapping.include_term_vectors = true;
        }
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docvalues = true;
        }
        return fieldMapping;
    };

    var newGeoShapeField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "geoshape";
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.highlight || field.phrase) {
            fieldMapping.include_term_vectors = true;
        }
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docvalues = true;
        }
        return fieldMapping;
    };

    var newBooleanField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "boolean";
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docValues = true;
        }
        return fieldMapping;
    };

    var newIPField = function(field) {
        var fieldMapping = {};
        fieldMapping.name = field.name;
        fieldMapping.type = "IP";
        if (field.store) {
            fieldMapping.store = true;
        }
        fieldMapping.index = true;
        if (field.highlight || field.phrase) {
            fieldMapping.include_term_vectors = true;
        }
        if (field.includeInAll) {
            fieldMapping.include_in_all = true;
        }
        if (field.sortFacet) {
            fieldMapping.docvalues = true;
        }
        return fieldMapping;
    };

    var addDocumentMappingFromPathField = function(mapping, path, field) {
        // split dotted-path into path elements
        var pathElements = path.split('.');
        // traverse the path, adding any missing document mappings along the way
        for (var pathElementI in pathElements) {
            let pathElement = pathElements[pathElementI];
            if (pathElement === "*dynamic*") {
                mapping.dynamic = true;
                mapping.default_analyzer = field.analyzer;
                continue;
            }
            if (mapping.properties && (pathElement in mapping.properties)) {
                mapping = mapping.properties[pathElement];
                continue;
            }
            if (!mapping.properties) {
                mapping.properties = {};
            }
            let newMapping = newDocMapping();
            mapping.properties[pathElement] = newMapping;
            mapping = mapping.properties[pathElement];
        }

        if (!mapping.fields) {
            mapping.fields = [];
        }

        // define a field in the terminal mapping
        if (field.type == "text") {
            mapping.fields.push(newTextField(field));
        } else if (field.type == "number") {
            mapping.fields.push(newNumericField(field));
        } else if (field.type == "datetime") {
            mapping.fields.push(newDateTimeField(field));
        } else if (field.type == "geopoint") {
            mapping.fields.push(newGeoPointField(field));
        } else if (field.type == "geoshape") {
            mapping.fields.push(newGeoShapeField(field));
        } else if (field.type == "boolean") {
            mapping.fields.push(newBooleanField(field));
        } else if (field.type == "IP") {
            mapping.fields.push(newIPField(field));
        }
    };

    var buildDocumentMapping = function() {
        var rootDocMapping = newDocMapping();
        for (var path in mapping) {
            addDocumentMappingFromPathField(rootDocMapping, path, mapping[path]);
        }
        return rootDocMapping;
    }

    var rebuildDynamicness = function(indexMapping) {
        if (indexMapping.dynamic) {
            let analyzer = indexMapping.default_analyzer;
            let obj = {
                analyzer: analyzer,
                name: "(dynamic - " + analyzer + ")",
            };
            mapping["*dynamic*"] = obj;
        }
    }

    var rebuildFieldFromProperty = function(parentPath, path, property) {
        let fullPath = "";
        if (parentPath) {
            fullPath = parentPath + "." + path;
        } else {
            fullPath = path;
        }
        for (var prop in property.properties) {
            rebuildFieldFromProperty(fullPath, prop, property.properties[prop])
        }
        for (var fieldI in property.fields) {
            let field = property.fields[fieldI];
            let editField = newEditField()
            editField.path = fullPath;
            editField.name = field.name;

            // set up defaults
            editField.new = false;
            editField.identifier = false;
            editField.store = false;
            editField.highlight = false;
            editField.phrase = false;
            editField.includeInAll = false;
            editField.sortFacet = false;

            if (field.type == "text") {
                editField.type = "text";
                editField.analyzer = field.analyzer;
                if (field.analyzer == "keyword") {
                    editField.identifier = true;
                }
            } else if (field.type == "number") {
                editField.type = "number";
            } else if (field.type == "datetime") {
                editField.type = "datetime";
                editField.date_format = field.date_format;
            } else if (field.type == "geopoint") {
                editField.type = "geopoint";
            } else if (field.type == "geoshape") {
                editField.type = "geoshape";
            } else if (field.type == "boolean") {
                editField.type = "boolean";
            } else if (field.type == "IP") {
                editField.type = "IP";
            }

            // finish some common settings
            if (field.store) {
                editField.store = true;
            }
            if (field.include_term_vectors) {
                editField.highlight = true;
                editField.phrase = true;
            }
            if (field.include_in_all) {
                editField.includeInAll = true;
            }
            if (field.docvalues) {
                editField.sortFacet = true;
            }

            mapping[editField.path] = editField;
        }
    }

    return {
        loadFromMapping: function(indexMapping) {
            rebuildDynamicness(indexMapping);
            for (var propertyName in indexMapping.properties) {
                rebuildFieldFromProperty("", propertyName, indexMapping.properties[propertyName]);
            }
        },
        resetDynamic: function() {
            delete mapping["*dynamic*"];
        },
        makeDynamic: function(collectionAnalyzer) {
            if (collectionAnalyzer.length == 0) {
                collectionAnalyzer = "standard";
            }
            let obj = {
                analyzer: collectionAnalyzer,
                name: "(dynamic - " + collectionAnalyzer + ")",
            };
            mapping["*dynamic*"] = Object.assign({}, obj);
        },
        hasDynamicDefChanged: function(collectionAnalyzer) {
            if (collectionAnalyzer.length == 0) {
                collectionAnalyzer = "standard";
            }
            let obj = {
                analyzer: collectionAnalyzer,
                name: "(dynamic - " + collectionAnalyzer + ")",
            };
            if (JSON.stringify(mapping["*dynamic*"]) === JSON.stringify(obj)) {
                return false;
            }
            return true;
        },
        dynamicAnalyzer: function() {
            if (angular.isDefined(mapping["*dynamic*"])) {
                let rv = mapping["*dynamic*"].analyzer;
                if (rv.length == 0) {
                    rv = "standard"
                }
                return rv;
            }
            return "";
        },
        addField: function(editField) {
            if (editField.identifier) {
                editField.analyzer = "keyword";
            }
            mapping[editField.path] = Object.assign({}, editField);
        },
        deleteField: function(path) {
            delete mapping[path];
        },
        hasFieldDefChanged: function(editField) {
            if (JSON.stringify(mapping[editField.path]) === JSON.stringify(editField)) {
                return false;
            }
            return true;
        },
        hasFieldAtPath: function(path) {
            if (path in mapping) {
                return true;
            }
            return false;
        },
        getFieldAtPath: function(path) {
            return mapping[path];
        },
        fields: function() {
            return Object.values(mapping);
        },
        numFields: function() {
            return Object.keys(mapping).length;
        },
        getMapping: function() {
            return buildDocumentMapping()
        },
        getIndexMapping: function() {
            var indexMapping = {};
            indexMapping.type_field = "_type";
            indexMapping.default_type = "_default";
            indexMapping.default_analyzer = "standard";
            indexMapping.default_datetime_parser = "dateTimeOptional";
            indexMapping.default_field = "_all";
            indexMapping.store_dynamic = false;
            indexMapping.index_dynamic = false;
            indexMapping.docvalues_dynamic = false;
            indexMapping.analysis = {};
            indexMapping.default_mapping = buildDocumentMapping();
            return indexMapping;
        },
    };
}

export {newEasyMappings, newEasyMapping};

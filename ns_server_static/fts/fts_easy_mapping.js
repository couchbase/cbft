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

import {newEditFields, newEditField} from "/_p/ui/fts/fts_easy_field.js";

function newEasyMappings() {
    var mappings = {};

    return {
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

            let collectionNames = Object.keys(mappings);
            for (var collectionNameI in collectionNames) {
                let collectionName = collectionNames[collectionNameI];
                if (mappings[collectionName].numFields() > 0) {
                    indexMapping.types[scopeName + '.' + collectionName] = mappings[collectionName].getMapping();
                }
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
        fieldMapping.include_in_all = true;
        if (field.sortFacet) {
            fieldMapping.docvalues = true
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
        if (field.sortFacet) {
            fieldMapping.docvalues = true
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
        if (field.sortFacet) {
            fieldMapping.docvalues = true
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
        if (field.sortFacet) {
            fieldMapping.docvalues = true
        }
        return fieldMapping;
    };

    var addDocumentMappingFromPathField = function(mapping, path, field) {
        // split dotted-path into path elements
        var pathElements = path.split('.');
        // traverse the path, adding any missing document mappings along the way
        for (var pathElementI in pathElements) {
            let pathElement = pathElements[pathElementI];
            if (mapping.propertyies && (pathElement in mapping.properties)) {
                mapping = mapping.properties[pathElement];
                continue;
            }
            let newMapping = newDocMapping();
            if (!mapping.properties) {
                mapping.properties = {};
            }
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
        }
    };

    var buildDocumentMapping = function() {
        var rootDocMapping = newDocMapping();
        for (var path in mapping) {
            addDocumentMappingFromPathField(rootDocMapping, path, mapping[path]);
        }
        return rootDocMapping;
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
            editField.name = fullPath;
            editField.path = editField.name;

            // set up defaults
            editField.new = false;
            editField.identifier = false;
            editField.store = false;
            editField.highlight = false;
            editField.phrase = false;
            editField.sortFacet = false;

            if (field.type == "text") {
                editField.type = "text";
                editField.analyzer = "standard";
                if (field.analyzer == "keyword") {
                    editField.identifier = true;
                } else {
                    editField.analyzer = field.analyzer;
                }
            } else if (field.type == "number") {
                editField.type = "number";
            } else if (field.type == "datetime") {
                editField.type = "datetime";
                editField.date_format = field.date_format;
            } else if (field.type == "geopoint") {
                editField.type = "geopoint";
            }

            // finish some common settings
            if (field.store) {
                editField.store = true;
            }
            if (field.include_term_vectors) {
                editField.highlight = true;
                editField.phrase = true;
            }
            if (field.docvalues) {
                editField.sortFacet = true;
            }

            mapping[editField.name] = editField;
        }
    }

    return {
        loadFromMapping: function(indexMapping) {
            for (var propertyName in indexMapping.properties) {
                rebuildFieldFromProperty("", propertyName, indexMapping.properties[propertyName])
            }
        },
        addField: function(editField) {
            mapping[editField.path] = Object.assign({}, editField);
        },
        deleteField: function(path) {
            delete mapping[path];
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
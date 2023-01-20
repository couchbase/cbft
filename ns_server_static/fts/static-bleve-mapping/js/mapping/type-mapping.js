//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.
export default initBleveTypeMappingController;
initBleveTypeMappingController.$inject = ["$scope", "typeMappingIn", "options"];
function initBleveTypeMappingController($scope, typeMappingIn, options) {
    options = options || {};

    var defaultFieldType = options["defaultFieldType"] || "text";
    var defaultFieldAnalyzer = null;
    if (options["defaultFieldAnalyzer"] != undefined) {
        defaultFieldAnalyzer = options["defaultFieldAnalyzer"];
    }
    var defaultFieldStore = false;
    if (options["defaultFieldStore"] != undefined) {
        defaultFieldStore = options["defaultFieldStore"];
    }
    var defaultFieldIndex = true;
    if (options["defaultFieldIndex"] != undefined) {
        defaultFieldIndex = options["defaultFieldIndex"];
    }
    var defaultFieldIncludeTermVectors = false;
    if (options["defaultFieldIncludeTermVectors"] != undefined) {
        defaultFieldIncludeTermVectors = options["defaultFieldIncludeTermVectors"];
    }
    var defaultFieldIncludeInAll = false;
    if (options["defaultFieldIncludeInAll"] != undefined) {
        defaultFieldIncludeInAll = options["defaultFieldIncludeInAll"];
    }
    var defaultFieldDocValues = false;
    if (options["defaultFieldDocValues"] != undefined) {
        defaultFieldDocValues = options["defaultFieldDocValues"];
    }
    var defaultFieldDateFormat = null;
    if (options["defaultFieldDateFormat"] != undefined) {
        defaultFieldDateFormat = options["defaultFieldDateFormat"];
    }
    var defaultMappingEnabled = true;
    if (options["defaultMappingEnabled"] != undefined) {
        defaultMappingEnabled = options["defaultMappingEnabled"];
    }
    var defaultMappingDynamic = true;
    if (options["defaultMappingDynamic"] != undefined) {
        defaultMappingDynamic = options["defaultMappingDynamic"];
    }

    var mappings = bleveConvertFromTypeMapping(typeMappingIn);


    $scope.fieldTypes = ['text', 'number', 'datetime', 'boolean', 'disabled', 'geopoint', 'geoshape', 'IP'];

    var kindAttrs = {
        "field": {
            'property': null,
            'name': null,
            'type': null,
            'analyzer': null,
            'store': null,
            'index': null,
            'include_term_vectors': null,
            'include_in_all': null,
            'docvalues': null,
            'date_format': null,
        },
        "mapping": {
            'name': null,
            'enabled': null,
            'dynamic': null,
            'default_analyzer': null
        }
    };

    kindAttrs['mappingType'] = kindAttrs['mapping'];

    // -------------------------------------------------------

    $scope.buildMappingName = function(scopeName, collectionName) {
        if (scopeName == "") {
            return "";
        }

        if (collectionName == "") {
            return "";
        }

        return scopeName + "." + collectionName;
    };

    // -------------------------------------------------------

    $scope.mappings = mappings;

    $scope.editing = null;

    $scope.popup = null;

    $scope.popupToggle = function(obj) {
        $scope.popup = ($scope.popup == obj) ? null : obj;
    }

    $scope.removeFromParent = function(obj, scope) {
        $scope.editAttrsDone(obj, false); // Cancel any edits.

        scope.remove();
    }

    $scope.addChildField = function(mapping) {
        if ($scope.editing) {
            return;
        }

        var f = {
            _kind: 'field',
            property: "",
            name: "",
            type: defaultFieldType,
            analyzer: defaultFieldAnalyzer,
            store: defaultFieldStore,
            index: defaultFieldIndex,
            include_term_vectors: defaultFieldIncludeTermVectors,
            include_in_all: defaultFieldIncludeInAll,
            docvalues: defaultFieldDocValues,
            date_format: defaultFieldDateFormat,
        };
        f._editing = function() { removeEntry(mapping.fields, f); };
        mapping.fields.unshift(f);

        $scope.validateField(f, mapping);

        $scope.editing = f;
        $scope.popup = null;
    }

    $scope.addChildMapping = function(mapping) {
        if ($scope.editing) {
            return;
        }

        if (mapping == null) {
            mapping = $scope;
        }

        var m = {
            _kind: mapping == $scope ? 'mappingType' : 'mapping',
            name: "",
            enabled: defaultMappingEnabled,
            dynamic: defaultMappingDynamic,
            fields: [],
            mappings: []
        };
        m._editing = function() { removeEntry(mapping.mappings, m); };
        mapping.mappings.unshift(m);

        $scope.validateMapping(m, mapping.mappings)

        $scope.editing = m;
        $scope.popup = null;
    }

    $scope.editAttrs = function(obj) {
        var attrs = kindAttrs[obj._kind];
        let attr;
        for (attr in attrs) {
            obj["_" + attr + "_ERR"] = null;
            obj["_" + attr + "_PREV"] = obj[attr];
        }

        obj._editing = true;
        $scope.editing = obj;
    }

    $scope.editAttrsDone = function(obj, ok) {
        var valid = true;

        var attrs = kindAttrs[obj._kind];
        let attr;
        for (attr in attrs) {
            var attrValidator = attrs[attr];

            if (ok) {
                if (attrValidator) {
                    valid = attrValidator(obj, attr) && valid;
                }
            } else { // Cancelled.
                obj[attr] = obj["_" + attr + "_PREV"];
            }
        }

        if (!valid) {
            return;
        }
        for (attr in attrs) {
            delete obj["_" + attr + "_ERR"];
            delete obj["_" + attr + "_PREV"];
        }

        if (!ok && typeof(obj._editing) == 'function') {
            obj._editing(); // Invoke editing cancellation callback.
        }

        delete obj._editing;
        $scope.editing = null;
    }

    $scope.changedProperty = function(field, mapping) {
        if (field._name_PREV == field._property_PREV) {
            field.name = field.property;
        }

        $scope.validateField(field, mapping)
    }

    $scope.validateField = function(field, mapping) {
        if (mapping) {
            var taken = false;
            let i;
            for (i in mapping.fields) {
                if (mapping.fields[i] != field &&
                    mapping.fields[i].name == field.name &&
                    mapping.fields[i].property == field.property) {
                    taken = true;
                }
            }
            if (taken) {
                field._invalid = true;
            } else {
                delete field._invalid;
            }
        }
    }

    $scope.validateMapping = function(mapping, mappings) {
        if (mappings) {
            var taken = false;
            let i;
            for (i in mappings) {
                if (mappings[i] != mapping &&
                    mappings[i].name == mapping.name) {
                    taken = true;
                }
            }
            if (taken) {
                mapping._invalid = true;
            } else {
                delete mapping._invalid;
            }
        }
    }

    $scope.options = {
        accept: function(sourceAccept, destAccept) {
            if ($scope.editing) {
                return false;
            }

            var sourceData = sourceAccept.$modelValue;
            var destType = destAccept.$element.attr('data-type');

            return (sourceData._kind+"Container") == destType;
        }
    }

    function removeEntry(arr, entry) {
        let i;
        for (i = 0; i < arr.length; i++) {
            if (arr[i] === entry) {
                arr.splice(i, 1);
            }
        }
    }

    return {
        isValid: function() {
            return true;
        },
        typeMapping: function() {
            return bleveConvertToTypeMapping($scope.mappings);
        }
    }
}

// Convert from a near-bleve-friendly TypeMapping data structure to a
// UI-friendly data structure.
function bleveConvertFromTypeMapping(typeMapping) {
    var mappings = [];

    typeMapping = JSON.parse(JSON.stringify(typeMapping));
    for (var type in typeMapping) {
        var mapping = typeMapping[type];

        mappings.push(mapping);
        mapping._kind = 'mappingType';

        delete mapping["name"];
        if (type) {
            mapping.name = type;
        }

        convert(mapping);
    }

    mappings.sort(displayOrderComparator);

    return mappings;

    function displayOrderComparator(a, b) {
        return (a.display_order || 0) - (b.display_order || 0);
    }

    function isEmpty(obj) {
        for (var k in obj) {
            return false;
        }
        return true;
    }

    function convert(mapping) {
        var mappings = [];
        var fields = mapping.fields || [];

        for (var property in mapping.properties) {
            var m = mapping.properties[property];
            if (isEmpty(m.properties) && !isEmpty(m.fields)) {
                // Promote m's fields into to propertied fields.
                let i;
                for (i in m.fields) {
                    var field = m.fields[i];
                    field.property = property;
                    fields.push(field);
                }
            } else {
                m._kind = 'mapping';
                m.name = property;
                mappings.push(m);

                convert(m);
            }
        }
        let i;
        for (i in fields) {
            fields[i]._kind = 'field';
        }

        delete mapping["properties"];

        mappings.sort(displayOrderComparator);
        fields.sort(displayOrderComparator);

        mapping.mappings = mappings;
        mapping.fields = fields;
    }
}

// Convert from a UI-friendly data structure to a near-bleve-friendly
// TypeMapping data structure.
function bleveConvertToTypeMapping(mappings) {
    var typeMapping = {};

    mappings = scrub(JSON.parse(JSON.stringify(mappings)));
    let i;
    for (i in mappings) {
        var mapping = mappings[i];

        typeMapping[mapping.name || ""] = mapping;

        delete mapping["name"];

        convertPropertiedFields(mapping);
    }

    return typeMapping;

    // Recursively remove every entry with '_' prefix.
    function scrub(m) {
        if (typeof(m) == "object") {
            for (var k in m) {
                if (typeof(k) == "string" && k.charAt(0) == "_") {
                    delete m[k];
                    continue;
                }

                m[k] = scrub(m[k]);
            }
        }

        return m;
    }

    // Recursively convert fields with "property" attribute into a
    // document mapping in the properties map.
    function convertPropertiedFields(m) {
        var properties = {};
        var fields = [];
        let i;
        for (i in m.mappings) {
            var mapping = m.mappings[i];

            properties[mapping.name] = mapping;

            delete mapping["name"];
            mapping.display_order = i;

            convertPropertiedFields(mapping);
        }

        for (i in m.fields) {
            var field = m.fields[i];

            if (field.date_format == null ||
                field.date_format == "") {
                delete field["date_format"];
            }

            if (field.property != null) { // "" is allowed.
                var property = properties[field.property];
                if (property == null) {
                    property = properties[field.property] = {
                        enabled: true,
                        dynamic: false,
                        properties: {}
                    };
                }

                if (!property.fields) {
                    property.fields = [];
                }

                property.fields.push(field);
            } else {
                fields.push(field);
            }

            delete field["property"];
            field.display_order = i;
        }

        delete m["mappings"];

        m.properties = properties;

        m.fields = fields;
        if (m.fields == null || m.fields.length <= 0) {
            delete m["fields"];
        }
    }
}

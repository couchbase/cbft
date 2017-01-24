function initBleveTypeMappingController($scope, typeMappingIn, options) {
    options = options || {};

    var defaultFieldType = options["defaultFieldType"] || "text";
    var defaultFieldAnalyzer = options["defaultFieldAnalyzer"] || "";
    var defaultFieldStore = true;
    if (options["defaultFieldStore"] != undefined) {
        defaultFieldStore = options["defaultFieldStore"];
    }
    var defaultFieldIndex = true;
    if (options["defaultFieldIndex"] != undefined) {
        defaultFieldIndex = options["defaultFieldIndex"];
    }
    var defaultFieldIncludeTermVectors = true;
    if (options["defaultFieldIncludeTermVectors"] != undefined) {
        defaultFieldIncludeTermVectors = options["defaultFieldIncludeTermVectors"];
    }
    var defaultFieldIncludeInAll = true;
    if (options["defaultFieldIncludeInAll"] != undefined) {
        defaultFieldIncludeInAll = options["defaultFieldIncludeInAll"];
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

    mappings = bleveConvertFromTypeMapping(typeMappingIn)

    $scope.fieldTypes = ['text', 'number', 'datetime', 'boolean', 'disabled'];

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
            date_format: defaultFieldDateFormat,
        };
        f._editing = function() { removeEntry(mapping.fields, f); };
        mapping.fields.unshift(f);

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

        $scope.editing = m;
        $scope.popup = null;
    }

    $scope.editAttrs = function(obj) {
        var attrs = kindAttrs[obj._kind];
        for (var attr in attrs) {
            obj["_" + attr + "_ERR"] = null;
            obj["_" + attr + "_PREV"] = obj[attr];
        }

        obj._editing = true;
        $scope.editing = obj;
    }

    $scope.editAttrsDone = function(obj, ok) {
        var valid = true;

        var attrs = kindAttrs[obj._kind];
        for (var attr in attrs) {
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

        for (var attr in attrs) {
            delete obj["_" + attr + "_ERR"];
            delete obj["_" + attr + "_PREV"];
        }

        if (!ok && typeof(obj._editing) == 'function') {
            obj._editing(); // Invoke editing cancellation callback.
        }

        delete obj._editing;
        $scope.editing = null;
    }

    $scope.changedProperty = function(field) {
        if (field._name_PREV == field._property_PREV) {
            field.name = field.property;
        }
    }

    $scope.options = {
        accept: function(sourceAccept, destAccept, destIndex) {
            if ($scope.editing) {
                return false;
            }

            var sourceData = sourceAccept.$modelValue;
            var destType = destAccept.$element.attr('data-type');

            return (sourceData._kind+"Container") == destType;
        }
    }

    function removeEntry(arr, entry) {
        for (var i = 0; i < arr.length; i++) {
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
// UI-friendly data structure.  By "near", an entry with "" key
// represents the default type mapping.
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
            if (isEmpty(m.properties)) {
                // Promote m's fields into to propertied fields.
                for (var i in m.fields) {
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

        for (var i in fields) {
            fields[i]._kind = 'field';
        }

        delete mapping["properties"];

        mappings.sort(displayOrderComparator);
        fields.sort(displayOrderComparator);

        mapping.mappings = mappings;
        mapping.fields = fields;
    }
}

// Convert froma UI-friendly data structure to a near-bleve-friendly
// TypeMapping data structure.  By "near", an entry with "" key
// represents the default type mapping.
function bleveConvertToTypeMapping(mappings) {
    var typeMapping = {};

    mappings = scrub(JSON.parse(JSON.stringify(mappings)));
    for (var i in mappings) {
        var mapping = mappings[i];

        typeMapping[mapping.name || ""] = mapping;

        delete mapping["name"];

        convertPropertiedFields(mapping);

        mapping.display_order = i;
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

        for (var i in m.fields) {
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
                        properties: {},
                        fields: []
                    };
                }

                property.fields.push(field);
            } else {
                fields.push(field);
            }

            delete field["property"];
            field.display_order = i;
        }

        for (var i in m.mappings) {
            var mapping = m.mappings[i];

            properties[mapping.name] = mapping;

            delete mapping["name"];
            mapping.display_order = i;

            convertPropertiedFields(mapping);
        }

        delete m["mappings"];

        m.properties = properties;

        m.fields = fields;
        if (m.fields == null || m.fields.length <= 0) {
            delete m["fields"];
        }
    }
}

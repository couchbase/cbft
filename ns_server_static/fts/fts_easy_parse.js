//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

function newParsedDocs() {
    var parsedDocs = {};
    var parsedXattrsDocs = {};

    return {
        getParsedDocForCollection: function (collectionName, xattrs) {
            if (!(collectionName in parsedDocs)) {
                parsedDocs[collectionName] = parseDocument('{}', null);
            }
            if (xattrs) {
                return parsedXattrsDocs[collectionName];
            }
            return parsedDocs[collectionName];
        },
        setDocForCollection: function (collectionName, src, xattrs) {
            parsedDocs[collectionName] = parseDocument(src, null);
            parsedXattrsDocs[collectionName] = parseDocument(src, xattrs)
        }
    }
}

// Regex matches the IPv4 and IPv6 address formats
// IPv4:
// - Four octets separated by periods
// - Each octet is 0-255
// IPv6:
// - Full format, :: shorthand, and IPv4-mapped IPv6
// - 8 groups of 4 hexadecimal digits separated by colons
// - IPv4-mapped IPv6: 6 groups of 4 hexadecimal digits separated by colons followed by an IPv4 address
// - :: shorthand: 0-8 groups of 4 hexadecimal digits separated by colons
const ipRegex = new RegExp(
    "^(?:" +
      // IPv4: Ensures each octet is 0-255
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})" +
      "|" +
      // IPv6: Standard full format and shortened forms
      "([a-fA-F0-9]{1,4}:){7}[a-fA-F0-9]{1,4}" +
      "|([a-fA-F0-9]{1,4}:){1,7}:" +
      "|([a-fA-F0-9]{1,4}:){1,6}:[a-fA-F0-9]{1,4}" +
      "|([a-fA-F0-9]{1,4}:){1,5}(:[a-fA-F0-9]{1,4}){1,2}" +
      "|([a-fA-F0-9]{1,4}:){1,4}(:[a-fA-F0-9]{1,4}){1,3}" +
      "|([a-fA-F0-9]{1,4}:){1,3}(:[a-fA-F0-9]{1,4}){1,4}" +
      "|([a-fA-F0-9]{1,4}:){1,2}(:[a-fA-F0-9]{1,4}){1,5}" +
      "|[a-fA-F0-9]{1,4}:((:[a-fA-F0-9]{1,4}){1,6})" +
      "|:((:[a-fA-F0-9]{1,4}){1,7}|:)" +
      "|([a-fA-F0-9]{1,4}:){1,6}:" +
      // IPv6 with embedded IPv4
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})" +
      "|" +
      // IPv4-Mapped IPv6 (e.g., ::ffff:192.168.1.1)
      "::ffff:(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})\\." +
      "(?:25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})" +
    ")$", "i"
);

function isValidIP(ip) {
    return ipRegex.test(ip);
}

// Regex matches the RFC3339 date-time format with optional
// - fractional part
// - timezone
// - space separator between date and time
// - `T` separator between date and time
// - `Z` UTC designator
// - timezone offset with or without `:` separator or Z
const dateTimeRegex = new RegExp(
    "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])" +                                        // YYYY-MM-DD (Required)
    "(?:[T ]" +                                                                                 // Optional separator `T` or space
    "(?:([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])" +                                         // HH:MM:SS (Optional)
    "(\\.\\d+)?" +                                                                              // Optional fractional seconds
    "(?:\\s?(Z|[+-](?:0[0-9]|1[0-3]):?[0-5][0-9]|[+-]14:00|[+-]1400))?" +                       // Optional timezone
    ")?)?$"                                                                                     // Time is optional, but if present, must follow a date
);

function isValidDateTime(dateTime) {
    return dateTimeRegex.test(dateTime);
}

// Regex matches the geopoint in the form of <latitude,longitude>
// Latitude: -90 to 90
// Longitude: -180 to 180
const geoPointRegex = new RegExp(
    "^\\s*" +                                                               // Allow optional leading whitespace                                                                // Start of either latitude/longitude or geohash match
    "(-?(?:90(?:\\.0+)?|[1-8]?\\d(?:\\.\\d+)?))\\s*,\\s*" +                 // Latitude: -90 to 90, optional decimals
    "(-?(?:180(?:\\.0+)?|1[0-7]\\d(?:\\.\\d+)?|\\d{1,2}(?:\\.\\d+)?))" +    // Longitude: -180 to 180, optional decimals
    "\\s*$"
);

function isValidGeoPoint(geoPoint) {
    return geoPointRegex.test(geoPoint);
}

function parseDocument(doc, xattrs) {

    var parseStack = [];
    var keysStack = [];
    var rowPaths = [];
    var rowTypes = [];
    var rowValues = [];
    var count = 0;
    var valsSincePop = 0;
    var dims = {};

    var replacer = function (key, value) {
        // on closing brace, pop stack, duplicate last entry for rowpaths
        while ((parseStack.length > 0) && (this !== parseStack[parseStack.length - 1])) {
            parseStack.pop();
            keysStack.pop();

            if (valsSincePop > 1) {
                rowPaths.push(rowPaths[rowPaths.length - 1]);
                rowTypes.push(rowTypes[rowTypes.length - 1]);
                rowValues.push(rowValues[rowValues.length - 1]);
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
            if (Array.isArray(value) && value.every(item => typeof item == 'number')) {
                dims[fullPath] = value.length
            }
        }

        // for any row other than the first, push the path onto rowPaths
        if (count != 0) {
            rowPaths.push(fullPath);
            rowTypes.push(valType);
            rowValues.push(value);
        }
        // this is the second row, push an extra to fix up opening brace case
        if (count == 1) {
            rowPaths.push(fullPath);
            rowTypes.push(valType);
            rowValues.push(value);
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

    if (xattrs != null && Object.keys(xattrs) != 0) {
        var parsedXattrs = {}
        for (const [key, val] of Object.entries(xattrs)) {
            if (!key.startsWith('_') && key != "txn") {
                parsedXattrs[key] = val
            }
        }
        if (Object.keys(parsedXattrs) != 0) {
            parsedObj["_$xattrs"] = parsedXattrs
        }
    }

    var docString = JSON.stringify(parsedObj || "", replacer, 2);

    while (parseStack.length > 0) {
        parseStack.pop();
        keysStack.pop();
        rowPaths.push(rowPaths[rowPaths.length - 1]);
        rowTypes.push(rowTypes[rowTypes.length - 1]);
        rowValues.push(rowValues[rowValues.length - 1]);
    }

    return {
        getPath: function (col) {
            return rowPaths[col];
        },
        getType: function (col) {
            // check whether the object is a nested geojson shape or a geopoint struct.
            if (rowTypes[col] === "object") {
                var obj = rowValues[col] || {};
                var keys = Object.keys(obj);
                // geoshape object must have a type and
                // - coordinates (for basic geoshape)
                // - geometries (for geometry collection)
                if (keys.includes("type") && (keys.includes("coordinates") || keys.includes("geometries"))) {
                    return "geoshape";
                }
                // geopoint object must have lat and lon / lng
                if (keys.includes("lat") && (keys.includes("lon") || keys.includes("lng"))) {
                    return "geopoint"
                }
            }

            // check whether the object is a vector
            if (rowTypes[col] === "object") {
                if (rowPaths[col] in dims) {
                    if (dims[rowPaths[col]] > 1) {
                        // if the slice is of length 2, consider it is a
                        // geopoint of the form [lon, lat]
                        // otherwise, consider it as a vector
                        return dims[rowPaths[col]] === 2 ? "geopoint" : "vector";
                    }
                }
            }

            // check whether the object is a vector_base64, geopoint, IP or datetime
            if (rowTypes[col] === "string") {
                var strToTest = rowValues[col]
                if (strToTest == null) {
                    return rowTypes[col];
                }
                if (isValidDateTime(strToTest)) {
                    return "datetime"
                }
                if (isValidGeoPoint(strToTest)) {
                    return "geopoint"
                }
                var vecLen = parseBase64Length(strToTest)
                if (vecLen > 2) {
                    dims[rowPaths[col]] = vecLen
                    return "vector_base64"
                }
                if (isValidIP(strToTest)) {
                    return "IP"
                }
            }

            return rowTypes[col];
        },
        getDocument: function () {
            return docString;
        },
        getDims: function (col) {
            return dims[rowPaths[col]]
        },
        getTextValue: function (col) {
            return rowValues[col]
        }
    };
}

function parseBase64Length(str) {
    try {
        var vecStr = atob(str)
        if (vecStr.length % 4 == 0 && vecStr.length > 0) {
            return vecStr.length / 4
        }
        return -1
    } catch {
        return -1
    }
}

export { newParsedDocs };

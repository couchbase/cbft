var module = angular.module('expvar', []);

module.factory('expvar', function($http, $log) {

    var result = {
        numSamplesToKeep: 60*10,

        // An example of an individual metric would be
        // like "stats/indexes/SOME_PINDEX/index/index_time".
        metricPaths: {}, // name => jsonPointerPath.
        metricValues: {}, // name => [val0, val1, ..., val59].

        // Used for server-driven dynamic data.  For example, when
        // jsonPointerPath is "stats/indexes", then the
        // dynamicDataKeys will be all the PINDEX_NAME's.
        dynamicDataPaths: {}, // dynamicName => jsonPointerPath.
        dynamicDataKeys: {}, // dynamicName => [key0, key1, ...].

        addMetric: function(name, path) {
            this.metricPaths[name] = path;
        },

        removeMetric: function(name) {
            delete this.metricPaths[name];
        },

        addDynamicDataPath: function(name, path) {
            this.dynamicDataPaths[name] = path;
        },

        getDynamicDataKeys: function(name) {
            return this.dynamicDataKeys[name];
        },

        getMetricValues: function(name) {
            return this.metricValues[name];
        },

        getMetricCurrentValue: function(name) {
            if (this.metricValues[name] !== undefined) {
                len = this.metricValues[name].length;
                if (len > 0) {
                    values = this.metricValues[name];
                    return values[len-1];
                }
            }
            return 0;
        },

        getMetricCurrentRate: function(name) {
            if (this.metricValues[name] !== undefined) {
                len = this.metricValues[name].length;
                if (len > 1) {
                    values = this.metricValues[name];
                    curr = values[len-1];
                    prev = values[len-2];
                    return curr - prev;
                }
            }
            return 0;
        },

        pollExpvar : function() {
            numSamplesToKeep = this.numSamplesToKeep;
            metricPaths = this.metricPaths;
            metricValues = this.metricValues;
            dynamicDataPaths = this.dynamicDataPaths;
            dynamicDataKeys = this.dynamicDataKeys;

            $http.get("/debug/vars").then(function(response) {
                var data = response.data;

                // lookup dynamic keys
                for(var keyLookupName in dynamicDataPaths) {
                    keyPath = this.dynamicDataPaths[keyLookupName];
                    keysContainer = jsonpointer.get(data, keyPath);
                    keys = [];
                    for(var key in keysContainer) {
                        keys.push(key);
                    }
                    dynamicDataKeys[keyLookupName] = keys;
                }

                // lookup metrics
                for(var metricName in metricPaths) {
                    metricPath = this.metricPaths[metricName];
                    metricValue = jsonpointer.get(data, metricPath);
                    thisMetricValues = metricValues[metricName];
                    if (thisMetricValues === undefined) {
                        thisMetricValues = [];
                    }
                    thisMetricValues.push(metricValue);
                    while (thisMetricValues.length > numSamplesToKeep) {
                        thisMetricValues.shift();
                    }
                    metricValues[metricName] = thisMetricValues;
                }
            }, function(response) {
                $log.info("error polling expvar");
            });
        },
    };

    return result;
});

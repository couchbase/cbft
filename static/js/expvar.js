var module = angular.module('expvar', []);

module.factory('expvar', function($http, $log) {

	var result = {
		pollInterval: null,
		numSamplesToKeep: 60*10,
		metricPaths: {},
		metricValues: {},
		keyLookupPaths: {},
		keyLookupValues: {},

		addMetric: function(name, path) {
			this.metricPaths[name] = path;
		},

		removeMetric: function(name) {
			delete this.metricPaths[name];
		},

		addKeysLookup: function(name, path) {
			this.keyLookupPaths[name] = path;
		},

		getKeys: function(name) {
			return this.keyLookupValues[name];
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
			keyLookupPaths = this.keyLookupPaths;
			keyLookupValues = this.keyLookupValues;
			$http.get("/debug/vars").success(function(data) {
				// lookup key names
				for(var keyLookupName in keyLookupPaths) {
					keyPath = this.keyLookupPaths[keyLookupName];
					keysContainer = jsonpointer.get(data, keyPath);
					keys = [];
					for(var key in keysContainer) {
						keys.push(key);
					}
					keyLookupValues[keyLookupName] = keys;
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
			}).error(function(data, status, headers, config) {
				$log.info("error polling expvar");
			});
		},

	};

	return result;

});
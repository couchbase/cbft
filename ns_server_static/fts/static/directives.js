//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

'use strict';

/* Directives */

angular.module('cbgtApp.directives', []).
  directive('appVersion', ['version', function(version) {
    return function(scope, elm, attrs) {
      elm.text(version);
    };
  }]).
  directive('nagPrism', ['$compile', function($compile) {
    return {
        restrict: 'A',
        transclude: true,
        scope: {
          source: '@'
        },
        link: function(scope, element, attrs, controller, transclude) {
            scope.$watch('source', function(v) {
              element.find("code").html(v);

              Prism.highlightElement(element.find("code")[0]);
            });

            transclude(function(clone) {
              if (clone.html() !== undefined) {
                element.find("code").html(clone.html());
                $compile(element.contents())(scope.$parent);
              }
            });
        },
        template: "<code></code>"
    };
}]);

'use strict';

/* Directives */

angular.module('myApp.directives', []).
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

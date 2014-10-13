'use strict';

// Declare app level module which depends on filters, and services
angular.module('myApp', [
  'ngRoute',
  'myApp.filters',
  'myApp.services',
  'myApp.directives',
  'myApp.controllers',
  'expvar'
]).
config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider) {
  $routeProvider.when('/indexes/', {templateUrl: '/static/partials/index/list.html', controller: 'IndexesCtrl'});
  $routeProvider.when('/indexes/_new', {templateUrl: '/static/partials/index/new.html', controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName', {templateUrl: '/static/partials/index/index.html', controller: 'IndexCtrl'});
  $routeProvider.when('/indexes/:indexName/:tabName', {templateUrl: '/static/partials/index/index.html', controller: 'IndexCtrl'});
  $routeProvider.when('/monitor/', {templateUrl: '/static/partials/monitor.html', controller: 'MonitorCtrl'});
  $routeProvider.when('/analysis/:typ', {templateUrl: '/static/partials/analysis/analysis.html', controller: 'AnalysisCtrl'});
  $routeProvider.when('/search/debug/', {templateUrl: '/static/partials/debug.html', controller: 'DebugCtrl'});
  $routeProvider.otherwise({redirectTo: '/indexes'});
  $locationProvider.html5Mode(true);
}]);

'use strict';

// Declare app level module which depends on filters, and services
angular.module('myApp', [
  'ngRoute',
  'myApp.filters',
  'myApp.services',
  'myApp.directives',
  'myApp.controllers',
  'expvar',
  'angularTreeview',
  'ui.sortable',
  'ui.bootstrap.transition',
  'ui.bootstrap.modal',
  'ui.bootstrap.tabs'
]).
config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider) {
  $routeProvider.when('/indexes/',
                      {templateUrl: '/static/partials/index/list.html',
                       controller: 'IndexesCtrl'});
  $routeProvider.when('/indexes/_new',
                      {templateUrl: '/static/partials/index/new.html',
                       controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName',
                      {templateUrl: '/static/partials/index/index.html',
                       controller: 'IndexCtrl'});
  $routeProvider.when('/indexes/:indexName/_edit',
                      {templateUrl: '/static/partials/index/new.html',
                       controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName/_clone',
                      {templateUrl: '/static/partials/index/new.html',
                       controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName/:tabName',
                      {templateUrl: '/static/partials/index/index.html',
                       controller: 'IndexCtrl'});

  $routeProvider.when('/nodes/',
                      {templateUrl: '/static/partials/node/list.html',
                       controller: 'NodeCtrl'});
  $routeProvider.when('/nodes/:nodeAddr',
                      {templateUrl: '/static/partials/node/node.html',
                       controller: 'NodeCtrl'});
  $routeProvider.when('/nodes/:nodeAddr/:tabName',
                      {templateUrl: '/static/partials/node/node.html',
                       controller: 'NodeCtrl'});

  $routeProvider.when('/monitor/',
                      {templateUrl: '/static/partials/monitor.html',
                       controller: 'MonitorCtrl'});

  $routeProvider.when('/manage/',
                      {templateUrl: '/static/partials/manage.html',
                       controller: 'ManageCtrl'});

  $routeProvider.when('/logs/',
                      {templateUrl: '/static/partials/logs.html',
                       controller: 'LogsCtrl'});

  $routeProvider.when('/debug/',
                      {templateUrl: '/static/partials/debug.html',
                       controller: 'DebugCtrl'});

  $routeProvider.otherwise({redirectTo: '/indexes'});
  $locationProvider.html5Mode(true);
}]);

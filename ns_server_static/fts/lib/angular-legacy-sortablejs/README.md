angular-legacy-sortable
-----------------------

Angular 1 module that integrates with Sortable.js

# Installation

## Install with NPM

	npm install angular-legacy-sortablejs-maintained

Don't install the old angular-legacy-sortablejs package as thats not maintained


# Examples

## Simple Drag and Drop

```js
angular.module('exampleApp', ['ng-sortable'])
	.component('dragAndDropExample', {
		template: `<ul ng-sortable>
				<li ng-repeat="item in ['burgers', 'chips', 'hotdog']">
					{$ item $}
				</li>
			</ul>`,
	})
```

## Specifying a Config
You can pass a Config obj to `ng-sortable` and it will pass this onto the created sortable object. The available options can be found [here](https://github.com/RubaXa/Sortable#options)

```js
angular.module('exampleApp', ['ng-sortable'])
	.component('dragAndDropExample', {
		template: `<ul ng-sortable=$ctrl.sortableConf>
				<li ng-repeat="item in ['burgers', 'chips', 'hotdog']">
					{$ item $}
				</li>
			</ul>`,
		controller: function AppSidebarController() {
			var ctrl = this;
			ctrl.sortableConf = {
				animation: 350,
				chosenClass: 'sortable-chosen',
				forceFallback: true,
			};
		},
	});
```

# Drag handle
Example showing how use the handle option

```js
angular.module('exampleApp', ['ng-sortable'])
	.component('dragAndDropExample', {
		template: `<ul ng-sortable=$ctrl.sortableConf>
				<li ng-repeat="item in ['burgers', 'chips', 'hotdog']" draggable="false">
					<span class="grab-handle">Drag Header</span>
					<div>{$ item $}</div>
				</li>
			</ul>`,
		controller: function AppSidebarController() {
  			var ctrl = this;
			ctrl.sortableConf = {
				animation: 350,
				chosenClass: 'sortable-chosen',
				handle: '.grab-handle',
				forceFallback: true,
			};
		},
	});
```

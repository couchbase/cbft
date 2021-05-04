import angular from "/ui/web_modules/angular.js";
import app from "/ui/app/app.js";
import mnElementCrane from "/ui/app/components/directives/mn_element_crane/mn_element_crane.js";
import { mnLazyload } from "/ui/app/mn.app.imports.js";

import { NgModule } from '/ui/web_modules/@angular/core.js';
import { UIRouterUpgradeModule } from '/ui/web_modules/@uirouter/angular-hybrid.js';

angular
  .module(app)
  .config(function (mnPluggableUiRegistryProvider, mnPermissionsProvider) {
    mnPluggableUiRegistryProvider.registerConfig({
      name: 'Search',
      state: 'app.admin.search.fts_list',
      plugIn: 'workbenchTab',
      index: 2,
      responsiveHide: true,
      includedByState: 'app.admin.search',
      ngShow: 'rbac.cluster.settings.fts.read'
    });

    (["cluster.settings.fts!read", "cluster.settings.fts!write"])
      .forEach(mnPermissionsProvider.set);

    mnPermissionsProvider.setBucketSpecific(function(name) {
      return [
        "cluster.bucket[" + name + "].fts!write",
        "cluster.bucket[" + name + "].data!read"
      ];
    });
  });

class FtsUI {
  static get annotations() { return [
    new NgModule({
      imports: [
        UIRouterUpgradeModule.forRoot({
          states: [{
            name: "app.admin.search.**",
            url: "/fts",
            lazyLoad: mnLazyload('/_p/ui/fts/fts.js', 'fts')
          }]
        })
      ]
    })
  ]}
}

export default FtsUI;

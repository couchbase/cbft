/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import app from "app";
import mnElementCrane from "components/directives/mn_element_crane/mn_element_crane";
import { mnLazyload } from "mn.app.imports";

import { NgModule } from '@angular/core';
import { UIRouterUpgradeModule } from '@uirouter/angular-hybrid';

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
            lazyLoad: mnLazyload(() => import('./fts.js'), 'fts')
          }]
        })
      ]
    })
  ]}
}

export default FtsUI;

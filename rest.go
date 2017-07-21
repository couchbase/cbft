//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"

	"github.com/couchbase/goutils/go-cbaudit"
)

const RESTIndexQueryPath = "/api/index/{indexName}/query"

// MapRESTPathStats is keyed by path spec strings.
var MapRESTPathStats = map[string]*rest.RESTPathStats{
	RESTIndexQueryPath: {},
}

func InitStaticRouter(staticDir, staticETag string,
	mgr *cbgt.Manager) *mux.Router {
	router := mux.NewRouter()
	router.StrictSlash(true)

	showUI := true
	if mgr != nil && mgr.Options()["hideUI"] != "" {
		hideUI, err := strconv.ParseBool(mgr.Options()["hideUI"])
		if err == nil && hideUI {
			showUI = false
		}
	}

	if showUI {
		prefix := ""
		if mgr != nil {
			prefix = mgr.Options()["urlPrefix"]
		}

		hfsStaticX := http.FileServer(assetFS())

		router.Handle(prefix+"/",
			http.RedirectHandler(prefix+"/index.html", 302))
		router.Handle(prefix+"/index.html",
			http.RedirectHandler(prefix+"/staticx/index.html", 302))
		router.Handle(prefix+"/static/partials/index/start.html",
			http.RedirectHandler(prefix+"/staticx/partials/index/start.html", 302))
		router.Handle(prefix+"/static/partials/index/new.html",
			http.RedirectHandler(prefix+"/staticx/partials/index/ft/new.html", 302))
		router.Handle(prefix+"/static/partials/index/list.html",
			http.RedirectHandler(prefix+"/staticx/partials/index/ft/list.html", 302))
		router.Handle(prefix+"/static/partials/index/index.html",
			http.RedirectHandler(prefix+"/staticx/partials/debug.html", 302))

		router = rest.InitStaticRouterEx(router,
			staticDir, staticETag, []string{
				prefix + "/indexes",
				prefix + "/nodes",
				prefix + "/monitor",
				prefix + "/manage",
				prefix + "/logs",
				prefix + "/debug",
			}, http.RedirectHandler(prefix+"/staticx/index.html", 302), mgr)

		staticxRoutes := []string{
			"/staticx/",
			"/staticx/index.html",
			"/staticx/index-ft.html",
			"/staticx/js/debug.js",
			"/staticx/partials/debug-rows.html",
			"/staticx/partials/debug.html",
			"/staticx/partials/index/ft/list.html",
			"/staticx/partials/index/ft/new.html",
			"/staticx/partials/index/start.html",
		}

		for _, route := range staticxRoutes {
			router.Handle(prefix+route, http.StripPrefix(prefix+"/staticx/", hfsStaticX))
		}
	}

	return router
}

func myAssetDir(name string) ([]string, error) {
	a, err := AssetDir(name)
	if err == nil {
		return a, err
	}

	return rest.AssetDir(name)
}

func myAsset(name string) ([]byte, error) {
	b, err := Asset(name)
	if err == nil {
		return b, err
	}

	return rest.Asset(name)
}

// NewRESTRouter creates a mux.Router initialized with the REST
// API and web UI routes.  See also InitStaticRouter if you need finer
// control of the router initialization.
func NewRESTRouter(versionMain string, mgr *cbgt.Manager,
	staticDir, staticETag string, mr *cbgt.MsgRing,
	adtSvc *audit.AuditSvc) (
	*mux.Router, map[string]rest.RESTMeta, error) {
	wrapAuthVersionHandler := func(h http.Handler) http.Handler {
		return &AuthVersionHandler{mgr: mgr, H: h, adtSvc: adtSvc}
	}

	var options = map[string]interface{}{
		"auth":             wrapAuthVersionHandler,
		"mapRESTPathStats": MapRESTPathStats,
	}

	return rest.InitRESTRouterEx(
		InitStaticRouter(staticDir, staticETag, mgr),
		versionMain, mgr, staticDir, staticETag, mr,
		myAssetDir, myAsset, options)
}

// --------------------------------------------------

type AuthVersionHandler struct {
	mgr    *cbgt.Manager
	H      http.Handler
	adtSvc *audit.AuditSvc
}

func (c *AuthVersionHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	if err := CheckAPIVersion(w, req); err != nil {
		return
	}

	path := ""
	if c.H != nil {
		hwrm, ok := c.H.(*rest.HandlerWithRESTMeta)
		if ok && hwrm.RESTMeta != nil {
			path = hwrm.RESTMeta.Opts["_path"]
		}
	}

	c.doAudit(req, path)

	if !CheckAPIAuth(c.mgr, w, req, path) {
		return
	}

	if c.H != nil {
		c.H.ServeHTTP(w, req)
	}
}

func (c *AuthVersionHandler) doAudit(req *http.Request, path string) {
	if c.adtSvc == nil {
		return
	}
	eventId, ok := restAuditMap[req.Method+":"+path]
	if ok {
		d := GetAuditEventData(eventId, req)
		go c.adtSvc.Write(eventId, d)
	}
}

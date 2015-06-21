//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package cbft

import (
	"net/http"
	"os"

	"github.com/elazarl/go-bindata-assetfs"

	"github.com/gorilla/mux"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt/rest"
)

// AssetFS returns the assetfs.AssetFS "filesystem" that holds static
// HTTP resources (css/html/js/images, etc) for the web UI.
//
// Users might introduce their own static HTTP resources and override
// resources from AssetFS() with their own resource lookup chaining.
func AssetFS() *assetfs.AssetFS {
	return assetFS()
}

// InitStaticFileRouter adds static HTTP resource routes to a router.
func InitStaticFileRouter(r *mux.Router, staticDir, staticETag string,
	pages []string) *mux.Router {
	rest.PIndexTypesInitRouter(r, "static.before")

	var s http.FileSystem
	if staticDir != "" {
		if _, err := os.Stat(staticDir); err == nil {
			log.Printf("http: serving assets from staticDir: %s", staticDir)
			s = http.Dir(staticDir)
		}
	}
	if s == nil {
		log.Printf("http: serving assets from embedded data")
		s = AssetFS()
	}

	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/",
		myFileHandler{http.FileServer(s), staticETag}))
	// Bootstrap UI insists on loading templates from this path.
	r.PathPrefix("/template/").Handler(http.StripPrefix("/template/",
		myFileHandler{http.FileServer(s), staticETag}))

	for _, p := range pages {
		// If client ask for any of the pages, redirect.
		r.PathPrefix(p).Handler(RewriteURL("/", http.FileServer(s)))
	}

	r.Handle("/index.html", http.RedirectHandler("/static/index.html", 302))
	r.Handle("/", http.RedirectHandler("/static/index.html", 302))

	rest.PIndexTypesInitRouter(r, "static.after")

	return r
}

type myFileHandler struct {
	h    http.Handler
	etag string
}

func (mfh myFileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if mfh.etag != "" {
		w.Header().Set("Etag", mfh.etag)
	}
	mfh.h.ServeHTTP(w, r)
}

// RewriteURL is a helper function that returns a URL path rewriter
// HandlerFunc, rewriting the URL path to a provided "to" string.
func RewriteURL(to string, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.URL.Path = to
		h.ServeHTTP(w, r)
	})
}

func showError(w http.ResponseWriter, r *http.Request,
	msg string, code int) {
	log.Printf("http: error code: %d, msg: %s", code, msg)
	http.Error(w, msg, code)
}

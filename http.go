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

	"github.com/gorilla/mux"

	log "github.com/couchbaselabs/clog"
)

func staticFileRouter(staticDir, staticETag string, pages []string) *mux.Router {
	var s http.FileSystem
	if _, err := os.Stat(staticDir); err == nil {
		log.Printf("http: serving assets from staticDir: %s", staticDir)
		s = http.Dir(staticDir)
	} else {
		log.Printf("http: serving assets from embedded data")
		s = assetFS()
	}

	r := mux.NewRouter()
	r.StrictSlash(true)
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/",
		myFileHandler{http.FileServer(s), staticETag}))

	for _, p := range pages {
		// If client ask for any of the pages, redirect.
		r.PathPrefix(p).Handler(RewriteURL("/", http.FileServer(s)))
	}

	r.Handle("/", http.RedirectHandler("/static/index.html", 302))

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

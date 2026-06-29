//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"net/http"
	"runtime"

	"github.com/gorilla/mux"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	audit "github.com/couchbase/goutils/go-cbaudit"
)

// MapRESTPathStats is keyed by path spec strings.
var MapRESTPathStats map[string]*rest.RESTPathStats

var indexPaths = map[string]bool{
	"/api/index/{indexName}": true,
	"/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}": true,
}

var queryPaths = map[string]bool{
	"/api/index/{indexName}/query":                                       true,
	"/api/bucket/{bucketName}/scope/{scopeName}/index/{indexName}/query": true,
	"/Search": true, // grpc requests from n1fty
}

func isIndexPath(path string) bool {
	return indexPaths[path]
}

func isQueryPath(path string) bool {
	return queryPaths[path]
}

// -----------------------------------------------------------------------------

func InitRESTPathStats() {
	MapRESTPathStats = make(map[string]*rest.RESTPathStats)
	for k := range queryPaths {
		MapRESTPathStats[k] = &rest.RESTPathStats{}
	}
}

// NewRESTRouter creates a mux.Router initialized with the REST
// API routes.
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

	r := mux.NewRouter()
	r.StrictSlash(true)

	return rest.InitRESTRouterEx(
		r, versionMain, mgr, staticDir, staticETag, mr,
		nil, nil, options)
}

// --------------------------------------------------

type AuthVersionHandler struct {
	mgr    *cbgt.Manager
	H      http.Handler
	adtSvc *audit.AuditSvc
}

func NewAuthVersionHandler(mgr *cbgt.Manager, adtSvc *audit.AuditSvc,
	h http.Handler) *AuthVersionHandler {
	return &AuthVersionHandler{
		mgr:    mgr,
		H:      h,
		adtSvc: adtSvc,
	}
}

func (c *AuthVersionHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			buf := make([]byte, 2048)
			n := runtime.Stack(buf, false)
			cbgt.PublishCrashEvent(map[string]interface{}{
				"stackTrace": string(buf[:n]),
				"crashError": fmt.Sprintf("%v", err),
			})
		}
	}()
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

	wEx := wrapResponseWriter(w)

	ok, username := checkAPIAuth(c, wEx, req, path)
	if !ok {
		return
	}

	if c.H != nil {
		c.H.ServeHTTP(wEx, req)
	}

	completeRequest(username, path, req, wEx.Length())
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

// -----------------------------------------------------------------------------

func wrapResponseWriter(w http.ResponseWriter) *ResponseWriterEx {
	return &ResponseWriterEx{w, 0}
}

// Wrapper for ResponseWriter to capture the number of bytes written to it
type ResponseWriterEx struct {
	http.ResponseWriter
	length int64
}

func (w *ResponseWriterEx) Write(b []byte) (n int, err error) {
	n, err = w.ResponseWriter.Write(b)
	w.length += int64(n)
	return
}

func (w *ResponseWriterEx) Length() int64 {
	return w.length
}

func (w *ResponseWriterEx) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

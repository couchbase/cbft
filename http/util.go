//  Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package http

import (
	"encoding/json"
	"io"
	"net/http"

	log "github.com/couchbase/clog"
)

func showError(w http.ResponseWriter, r *http.Request,
	msg string, code int) {
	log.Warnf("ftsHttp reporting error %v/%v", code, msg)
	http.Error(w, msg, code)
}

func mustEncode(w io.Writer, i interface{}) {
	if headered, ok := w.(http.ResponseWriter); ok {
		headered.Header().Set("Cache-Control", "no-cache")
		headered.Header().Set("Content-type", "application/json")
	}

	e := json.NewEncoder(w)
	if err := e.Encode(i); err != nil {
		log.Warnf("ftsHttp mustEncode error: %v", err)
	}
}

type varLookupFunc func(req *http.Request) string

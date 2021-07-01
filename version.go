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
	"strings"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// cbft product version
const VERSION = "v0.5.5"

// cbft api versions, helps in controlling the
// service expectations during upgrades.
// See: MB-17990 on cbft API versioning.
const API_MAX_VERSION = "2.0.0"
const API_MIN_VERSION = "0.0.0"

var API_MAX_VERSION_JSON = WithJSONVersion(API_MAX_VERSION)

const VersionTag = "version="

func HandleAPIVersion(h string) (string, error) {
	if len(h) <= 0 || h == "*/*" {
		return API_MAX_VERSION, nil
	}

	foundRequestVersion := false

	for _, val := range strings.Split(h, ",") {
		versionIndex := strings.Index(val, VersionTag)
		if versionIndex < 0 {
			continue
		}

		foundRequestVersion = true

		requestVersion := val[versionIndex+len(VersionTag):]
		if cbgt.VersionGTE(API_MAX_VERSION, requestVersion) &&
			cbgt.VersionGTE(requestVersion, API_MIN_VERSION) {
			return requestVersion, nil
		}
	}

	if !foundRequestVersion {
		return API_MAX_VERSION, nil
	}

	return "", fmt.Errorf("version: unsupported version")
}

func WithJSONVersion(v string) string {
	return "application/json;" + VersionTag + v
}

func CheckAPIVersion(w http.ResponseWriter, req *http.Request) (err error) {
	if req.Header != nil {
		accept := req.Header["Accept"]
		if len(accept) > 0 {
			version, err := HandleAPIVersion(accept[0])
			if err != nil {
				w.WriteHeader(406)

				versionList := []string{
					WithJSONVersion(API_MAX_VERSION),
					WithJSONVersion(API_MIN_VERSION),
				}

				rest.MustEncode(w, versionList)
				return err
			}

			w.Header().Set("Content-type", WithJSONVersion(version))
			return nil
		}
	}

	w.Header().Set("Content-type", API_MAX_VERSION_JSON)
	return nil
}

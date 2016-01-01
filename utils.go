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
	"encoding/json"
	"fmt"
	"github.com/couchbase/cbgt"
	"net/http"
	"strings"
)

const API_MAX_VERSION = "1.0.0"
const API_MIN_VERSION = "0.0.0"
const VersionTag = "version="

func SetHandler(h http.Handler) http.Handler {
	return &CbftHandler{H: h}
}

type CbftHandler struct {
	H http.Handler
}

func (c *CbftHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	if err := checkAPIVersion(w, req); err != nil {
		return
	}
	if c.H != nil {
		c.H.ServeHTTP(w, req)
	}
}

func WithContentType(v string) string {
	return "application/json;version=" + v
}

func HandleVersion(h string) (string, error) {
	if h == "*/*" {
		return API_MAX_VERSION, nil
	}
	found := false
	for _, val := range strings.Split(h, ",") {
		fmt.Println("version is", val)
		versionIndex := strings.Index(val, VersionTag)
		if versionIndex == -1 {
			continue
		}
		found = true
		requestVersion := val[versionIndex+len(VersionTag):]
		if cbgt.VersionGTE(API_MAX_VERSION, requestVersion) &&
			cbgt.VersionGTE(requestVersion, API_MIN_VERSION) {
			return requestVersion, nil
		}
	}
	// no version string found
	if !found {
		return API_MAX_VERSION, nil
	}
	// unsupported version
	return "", fmt.Errorf("Version number is not supported")
}

func checkAPIVersion(w http.ResponseWriter, req *http.Request) (err error) {
	var version = API_MAX_VERSION
	if req.Header != nil && req.Header["Accept"] != nil {
		version, err = HandleVersion(req.Header["Accept"][0])
		if err != nil {
			w.WriteHeader(406)
			versionList := []string{WithContentType(API_MAX_VERSION),
				WithContentType(API_MIN_VERSION)}
			if val, err := json.Marshal(versionList); err == nil {
				w.Write(val)
			}
			return
		}
	}
	w.Header().Set("Content-type", "application/json;version="+version)
	return
}

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
	"fmt"
	"net/http"
	"net/url"

	"github.com/couchbase/cbauth"
)

var authType = ""

func SetAuthType(authType string) {
	authType = authType
}

func CheckAPIAuth(w http.ResponseWriter, req *http.Request) (admin bool) {
	if authType == "" {
		return true
	}

	if authType != "cbauth" {
		return false
	}

	creds, err := cbauth.AuthWebCreds(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("rest_auth: err: %v ", err), 403)
		return false
	}

	admin, err = creds.IsAdmin()
	if err != nil {
		http.Error(w, fmt.Sprintf("rest_auth: err: %v ", err), 403)
		return false
	}

	if !admin {
		cbauth.SendUnauthorized(w)
		return false
	}

	return true
}

func UrlWithAuth(urlStr string) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", err
	}

	if authType == "cbauth" {
		adminUser, adminPasswd, err := cbauth.GetHTTPServiceAuth(u.Host)
		if err != nil {
			return "", err
		}

		u.User = url.UserPassword(adminUser, adminPasswd)
	}

	return u.String(), nil
}

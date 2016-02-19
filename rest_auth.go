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
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// Map of "method:path" => "perm".  For example, "GET:/api/index" =>
// "cluster.bucket.fts!read".
var restPermsMap = map[string]string{}

func init() {
	// Initialze restPermsMap from restPerms.
	rps := strings.Split(strings.TrimSpace(restPerms), "\n\n")
	for _, rp := range rps {
		// Example rp: "GET /api/index\ncluster.bucket...!read".
		rpa := strings.Split(rp, "\n")
		ra := strings.Split(rpa[0], " ")
		method := ra[0]
		path := ra[1]
		perm := rpa[1]
		restPermsMap[method+":"+path] = perm
	}
}

// --------------------------------------------------------

func CheckAPIAuth(mgr *cbgt.Manager,
	w http.ResponseWriter, req *http.Request, path string) (allowed bool) {
	authType := ""
	if mgr != nil && mgr.Options() != nil {
		authType = mgr.Options()["authType"]
	}

	if authType == "" {
		return true
	}

	if authType != "cbauth" {
		return false
	}

	creds, err := cbauth.AuthWebCreds(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("rest_auth: cbauth.AuthWebCreds,"+
			" err: %v ", err), 403)
		return false
	}

	perm, err := preparePerm(mgr, req, req.Method, path)
	if err != nil {
		http.Error(w, fmt.Sprintf("rest_auth: preparePerm,"+
			" err: %v ", err), 403)
		return false
	}

	allowed, err = creds.IsAllowed(perm)
	if err != nil {
		http.Error(w, fmt.Sprintf("rest_auth: cbauth.IsAllowed,"+
			" err: %v ", err), 403)
		return false
	}

	if !allowed {
		cbauth.SendUnauthorized(w)
		return false
	}

	return true
}

func UrlWithAuth(authType, urlStr string) (string, error) {
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

// --------------------------------------------------------

func preparePerm(mgr *cbgt.Manager, req *http.Request,
	method, path string) (string, error) {
	perm := restPermsMap[method+":"+path]
	if perm == "" {
		perm = restPermDefault
	}

	// TODO: Handle full-text-alias auth check better by calling
	// IsAllowed on all of the target buckets of the alias.

	if strings.Index(perm, "<sourceName>") >= 0 {
		indexName := rest.IndexNameLookup(req)
		if indexName != "" {
			_, indexDefsByName, err := mgr.GetIndexDefs(false)
			if err != nil {
				return "", err
			}

			indexDef, exists := indexDefsByName[indexName]
			if !exists || indexDef == nil {
				if method == "PUT" {
					// Special case where PUT can mean CREATE, which
					// we assume when there's no indexDef.
					return preparePerm(mgr, req, "CREATE", path)
				}

				return "", fmt.Errorf("index not found")
			}

			perm = strings.Replace(perm, "<sourceName>",
				indexDef.SourceName, -1)
		} else {
			pindexName := rest.PIndexNameLookup(req)
			if pindexName != "" {
				pindex := mgr.GetPIndex(pindexName)
				if pindex == nil {
					return "", fmt.Errorf("no pindex,"+
						" pindexName: %s", pindexName)
				}

				perm = strings.Replace(perm, "<sourceName>",
					pindex.SourceName, -1)
			} else {
				return "", fmt.Errorf("missing indexName/pindexName")
			}
		}
	}

	perm = strings.Replace(perm, "[]", "", -1)

	return perm, nil
}

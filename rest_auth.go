//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/buger/jsonparser"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	audit "github.com/couchbase/goutils/go-cbaudit"
)

// CBAuthWebCreds extra level-of-indirection allows for overrides and
// for more testability.
var CBAuthWebCreds = cbauth.AuthWebCreds

// CBAuthIsAllowed extra level-of-indirection allows for overrides and
// for more testability.
var CBAuthIsAllowed = func(creds cbauth.Creds, permission string) (
	bool, error) {
	return creds.IsAllowed(permission)
}

// CBAuthSendForbidden extra level-of-indirection allows for overrides
// and for more testability.
var CBAuthSendForbidden = func(w http.ResponseWriter, permission string) {
	cbauth.SendForbidden(w, permission)
}

// CBAuthSendUnauthorized extra level-of-indirection allows for
// overrides and for more testability.
var CBAuthSendUnauthorized = func(w http.ResponseWriter) {
	cbauth.SendUnauthorized(w)
}

// UrlWithAuth extra level-of-indirection allows for
// overrides and for more testability.
var UrlWithAuth = func(authType, urlStr string) (string, error) {
	if authType == "cbauth" {
		return cbgt.CBAuthURL(urlStr)
	}

	return urlStr, nil
}

// --------------------------------------------------

// Map of "method:path" => "perm".  For example, "GET:/api/index" =>
// "cluster.bucket.fts!read".
var restPermsMap = map[string]string{}
var restAuditMap = map[string]uint32{}

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
		if len(rpa) > 2 {
			eventId, _ := strconv.ParseUint(rpa[2], 0, 32)
			restAuditMap[method+":"+path] = uint32(eventId)
		}
	}
}

// --------------------------------------------------------

func addUserAgentHeaderToRequest(req *http.Request) {
	req.Header.Set("User-Agent", cbgt.UserAgentStr)
}

func checkAPIAuth(avh *AuthVersionHandler,
	w http.ResponseWriter, req *http.Request, path string) (
	allowed bool, username string) {
	authType := ""

	var mgr *cbgt.Manager
	var adtSvc *audit.AuditSvc

	if avh != nil {
		mgr = avh.mgr
		adtSvc = avh.adtSvc
	}

	if mgr != nil && mgr.Options() != nil {
		authType = mgr.Options()["authType"]
	}

	if authType == "" {
		return true, ""
	}

	if authType != "cbauth" {
		return false, ""
	}

	r := &restRequestParser{req: req}

	perms, err := preparePerms(mgr, r, req.Method, path)
	if err != nil {
		requestBody, _ := ioutil.ReadAll(req.Body)
		rest.PropagateError(w, requestBody, fmt.Sprintf("rest_auth: preparePerms,"+
			" err: %v", err), http.StatusBadRequest)
		return false, ""
	}

	addUserAgentHeaderToRequest(req)
	creds, err := CBAuthWebCreds(req)
	if err != nil {
		requestBody, _ := ioutil.ReadAll(req.Body)
		rest.PropagateError(w, requestBody, fmt.Sprintf("rest_auth: cbauth.AuthWebCreds,"+
			" err: %v", err), http.StatusForbidden)

		if adtSvc != nil {
			d := GetAuditEventData(AuditAccessDeniedEvent, req)
			go adtSvc.Write(AuditAccessDeniedEvent, d)
		}

		return false, ""
	}

	for _, perm := range perms {
		allowed, err = CBAuthIsAllowed(creds, perm)
		if err != nil {
			requestBody, _ := ioutil.ReadAll(req.Body)
			rest.PropagateError(w, requestBody, fmt.Sprintf("rest_auth: cbauth.IsAllowed,"+
				" err: %v", err), http.StatusForbidden)
			return false, ""
		}

		if !allowed {
			CBAuthSendForbidden(w, perm)

			if adtSvc != nil {
				d := GetAuditEventData(AuditAccessDeniedEvent, req)
				go adtSvc.Write(AuditAccessDeniedEvent, d)
			}

			return false, ""
		}
	}

	username = creds.Name()

	if ok, msg := processRequest(username, path, req); !ok {
		requestBody, _ := ioutil.ReadAll(req.Body)
		rest.PropagateError(w, requestBody, msg, http.StatusTooManyRequests)
		return false, ""
	}

	return true, username
}

// --------------------------------------------------------

func sourceNamesForAlias(name string, indexDefsByName map[string]*cbgt.IndexDef,
	depth int) ([]string, error) {
	if depth > 50 {
		return nil, errAliasExpansionTooDeep
	}

	var rv []string

	indexDef, exists := indexDefsByName[name]
	if exists && indexDef != nil && indexDef.Type == "fulltext-alias" {
		aliasParams, err := parseAliasParams(indexDef.Params)
		if err != nil {
			return nil, fmt.Errorf("error expanding fulltext-alias: %v", err)
		}
		for aliasIndexName := range aliasParams.Targets {
			aliasIndexDef, exists := indexDefsByName[aliasIndexName]
			// if alias target doesn't exist, do nothing
			if exists {
				if aliasIndexDef.Type == "fulltext-alias" {
					// handle nested aliases with recursive call
					nestedSources, err := sourceNamesForAlias(aliasIndexName,
						indexDefsByName, depth+1)
					if err != nil {
						return nil, err
					}
					rv = append(rv, nestedSources...)
				} else {
					sourceNames, err := getSourceNamesFromIndexDef(aliasIndexDef)
					if err != nil {
						return nil, err
					}
					rv = append(rv, sourceNames...)
				}
			}
		}
	}

	return rv, nil
}

func getSourceNamesFromIndexDef(indexDef *cbgt.IndexDef) ([]string, error) {
	if len(indexDef.Params) > 0 {
		bleveParamBytes := []byte(indexDef.Params)
		docConfig, _, _, err := jsonparser.Get(bleveParamBytes, "doc_config")
		if err != nil {
			// couldn't find doc_config to detect the mode
			return []string{indexDef.SourceName}, nil
		}

		docConfigMode, _, _, _ := jsonparser.Get(docConfig, "mode")
		if strings.HasPrefix(string(docConfigMode), ConfigModeCollPrefix) {
			// look up the scope/collection details from the cache.
			scopeName, collectionNames := metaFieldValCache.getScopeCollectionNames(indexDef.Name)
			if len(scopeName) > 0 && len(collectionNames) > 0 {
				sourceNames := make([]string, len(collectionNames))
				for i := range collectionNames {
					sourceNames[i] = indexDef.SourceName + ":" + scopeName + ":" + collectionNames[i]
				}
				return sourceNames, nil
			}

			// parse it again if the cache is empty like that in a unit test.
			bmapping, _, _, err := jsonparser.Get(bleveParamBytes, "mapping")
			if err != nil {
				return nil, err
			}

			mapping := bleve.NewIndexMapping()
			err = UnmarshalJSON(bmapping, mapping)
			if err != nil {
				return nil, err
			}

			sName, colNames, _, err := getScopeCollTypeMappings(mapping, true)
			if err != nil {
				return nil, err
			}

			sourceNames := make([]string, len(colNames))
			for i, colName := range colNames {
				sourceNames[i] = indexDef.SourceName + ":" + sName + ":" + colName
			}
			return sourceNames, nil
		}
	}

	return []string{indexDef.SourceName}, nil
}

// an interface to abstract the bare minimum aspect of a cbgt.Manager
// that we need, so that we can stub the interface for testing
type definitionLookuper interface {
	GetPIndex(pindexName string) *cbgt.PIndex
	GetIndexDefs(refresh bool) (*cbgt.IndexDefs, map[string]*cbgt.IndexDef, error)
}

// requestParser is an interface which both the rest and rpc based
// services to implement for eliciting the bare minimum parameters
// needed for performing the authentication
type requestParser interface {
	GetIndexName() (string, error)
	GetPIndexName() (string, error)
	GetIndexDef() (*cbgt.IndexDef, error)
	GetRequest() (interface{}, string)
	GetCollectionNames() ([]string, error)
}

var errInvalidHttpRequest = fmt.Errorf("rest_auth: invalid http request")

type restRequestParser struct {
	req *http.Request
}

func (p *restRequestParser) GetRequest() (interface{}, string) {
	return p.req, "REST"
}

func (p *restRequestParser) GetIndexName() (string, error) {
	return rest.IndexNameLookup(p.req), nil
}

func (p *restRequestParser) GetPIndexName() (string, error) {
	pindexName := rest.PIndexNameLookup(p.req)
	if pindexName != "" {
		return pindexName, nil
	}
	return "", fmt.Errorf("rest_auth: restRequestParser, missing pindexName")
}

func (p *restRequestParser) GetIndexDef() (*cbgt.IndexDef, error) {
	var requestBody []byte
	var err error
	if p.req.Body != nil {
		requestBody, err = ioutil.ReadAll(p.req.Body)
		if err != nil {
			return nil, fmt.Errorf("rest_auth: restRequestParser, err: %v", err)
		}
	}

	// reset req.Body so it can be read later by the handler
	p.req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))

	var indexDef cbgt.IndexDef
	if len(requestBody) > 0 {
		err := json.Unmarshal(requestBody, &indexDef)
		if err != nil {
			return nil, fmt.Errorf("rest_auth: restRequestParser, unmarshal err: %v", err)
		}
	}

	if indexDef.Type == "" {
		// if indexType wasn't found in the request body, attempt reading it
		// from the form entries.
		indexDef.Type = p.req.FormValue("indexType")
	}

	return &indexDef, nil
}

func (p *restRequestParser) GetCollectionNames() ([]string, error) {
	var requestBody []byte
	var err error
	if p.req.Body != nil {
		requestBody, err = ioutil.ReadAll(p.req.Body)
		if err != nil {
			return nil, fmt.Errorf("rest_auth: restRequestParser, err: %v", err)
		}
	}

	// reset req.Body so it can be read later by the handler
	p.req.Body = ioutil.NopCloser(bytes.NewReader(requestBody))

	var rv []string
	jsonparser.ArrayEach(requestBody, func(value []byte,
		dataType jsonparser.ValueType, offset int, err error) {
		rv = append(rv, string(value))
	}, "collections")

	return rv, nil
}

func (p *restRequestParser) GetSourceName() (string, error) {
	return p.req.FormValue("sourceName"), nil
}

var errIndexNotFound = fmt.Errorf("index not found")
var errPIndexNotFound = fmt.Errorf("pindex not found")
var errAliasExpansionTooDeep = fmt.Errorf("alias expansion too deep")

func sourceNamesFromReq(mgr definitionLookuper, rp requestParser,
	method, path string) ([]string, error) {
	indexName, _ := rp.GetIndexName()
	_, indexDefsByName, err := mgr.GetIndexDefs(false)
	if err != nil {
		return nil, err
	}
	if indexName != "" {
		indexDef, exists := indexDefsByName[indexName]
		if !exists {
			// Force refresh of indexDefs and try again.
			_, indexDefsByName, err = mgr.GetIndexDefs(true)
			if err != nil {
				return nil, err
			}
			indexDef, exists = indexDefsByName[indexName]
			if !exists || indexDef == nil {
				if method == "PUT" {
					// Special case where PUT represents an index creation
					// when there's no indexDef.
					return findCouchbaseSourceNames(rp, indexName, indexDefsByName)
				}
				return nil, errIndexNotFound
			}
		}

		var sourceNames []string
		var currSourceNames []string
		if indexDef.Type == "fulltext-alias" {
			// this finds the sources in current definition
			currSourceNames, err = sourceNamesForAlias(indexName, indexDefsByName, 0)
			if err != nil {
				return nil, err
			}
			sourceNames = append(sourceNames, currSourceNames...)
		} else {
			// first use the source in current definition
			currSourceNames, err = getSourceNamesFromIndexDef(indexDef)
			if err != nil {
				return nil, err
			}

			// get the target collections from the request
			targetColls, _ := rp.GetCollectionNames()

			// authenticate against all the sourcenames if its a blanket query
			if len(targetColls) == 0 {
				return append(sourceNames, currSourceNames...), nil
			}

			// authenticate only against the given target collections
			collMap := cbgt.StringsToMap(targetColls)
			for _, sn := range currSourceNames {
				pos := strings.LastIndex(sn, ":") + 1
				if _, found := collMap[sn[pos:]]; found {
					sourceNames = append(sourceNames, sn)
				}
			}
		}

		return sourceNames, nil
	}

	pindexName, err := rp.GetPIndexName()
	if pindexName == "" || err != nil {
		return nil, fmt.Errorf("missing indexName/pindexName, err: %v", err)
	}

	pindex := mgr.GetPIndex(pindexName)
	if pindex == nil {
		return nil, errPIndexNotFound
	}

	indexDef, exists := indexDefsByName[pindex.IndexName]
	if !exists {
		// Force refresh of indexDefs and try again.
		_, indexDefsByName, err = mgr.GetIndexDefs(true)
		if err != nil {
			return nil, err
		}
		indexDef, _ = indexDefsByName[pindex.IndexName]
	}
	if indexDef != nil {
		return getSourceNamesFromIndexDef(indexDef)
	}

	return nil, fmt.Errorf("invalid pindexName: %s", pindexName)
}

func preparePerms(mgr definitionLookuper, r requestParser,
	method, path string) ([]string, error) {
	perm := restPermsMap[method+":"+path]
	if perm == "" {
		perm = restPermDefault
	} else if perm == "none" {
		return nil, nil
	} else if strings.Index(perm, "{}") >= 0 {
		return nil, nil // Need dynamic post-filtering of REST response.
	}
	if strings.Index(perm, "<sourceName>") >= 0 {
		sourceNames, err := sourceNamesFromReq(mgr, r, method, path)
		if err != nil {
			return nil, err
		}

		perms := make([]string, 0, len(sourceNames))
		for _, sourceName := range sourceNames {
			perms = append(perms,
				decoratePermStrings(perm, sourceName))
		}
		return perms, nil
	}

	return []string{perm}, nil
}

func decoratePermStrings(perm, sourceName string) string {
	// If the RBAC settings are done at the scope or collection
	// level, then update the perm placeholder strings (refer rest_perm.go)
	// accordingly before decorating it with the source details.
	// This source enriched perm strings are further sent for
	// authentications.
	/*
		Perm string for RBAC at bucket level “test”:
		cluster.bucket[test].data.docs!read

		Perm string for RBAC at bucket “test”, scope “s”:
		cluster.scope[test:s].data.docs!read

		Perm string for RBAC at bucket “test”, scope “s”, collection “c”:
		cluster.collection[test:s:c].data.docs!read
	*/
	rbacLevel := strings.Count(sourceName, ":")
	if rbacLevel == 0 {
		perm = strings.ReplaceAll(perm, "collection", "bucket")
	} else if rbacLevel == 1 {
		perm = strings.ReplaceAll(perm, "collection", "scope")
	}

	return strings.ReplaceAll(perm, "<sourceName>", sourceName)
}

func findCouchbaseSourceNames(r requestParser, indexName string,
	indexDefsByName map[string]*cbgt.IndexDef) (rv []string, err error) {
	indexDef, err := r.GetIndexDef()
	if err != nil || indexDef == nil {
		return nil, err
	}

	if indexDef.Type == "fulltext-index" {
		t, reqType := r.GetRequest()
		req := t.(*http.Request)
		// TODO handle this for RPCs.
		if reqType == "REST" {
			sourceType, _ := rest.ExtractSourceTypeName(req, indexDef, indexName)
			if sourceType == cbgt.SOURCE_GOCOUCHBASE || sourceType == cbgt.SOURCE_GOCBCORE {
				return getSourceNamesFromIndexDef(indexDef)
			}
		}
	} else if indexDef.Type == "fulltext-alias" {
		// create a copy of indexDefNames with the new one added
		futureIndexDefsByName := make(map[string]*cbgt.IndexDef,
			len(indexDefsByName)+1)
		for k, v := range indexDefsByName {
			futureIndexDefsByName[k] = v
		}
		futureIndexDefsByName[indexName] = indexDef

		return sourceNamesForAlias(indexName, futureIndexDefsByName, 0)
	}

	return nil, nil
}

type CBAuthBasicLogin struct {
	mgr *cbgt.Manager
}

func CBAuthBasicLoginHandler(mgr *cbgt.Manager) (*CBAuthBasicLogin, error) {
	return &CBAuthBasicLogin{
		mgr: mgr,
	}, nil
}

func (h *CBAuthBasicLogin) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	authType := ""
	if h.mgr != nil && h.mgr.Options() != nil {
		authType = h.mgr.Options()["authType"]
	}

	if authType == "cbauth" {
		addUserAgentHeaderToRequest(req)
		creds, err := CBAuthWebCreds(req)
		if err != nil {
			requestBody, _ := ioutil.ReadAll(req.Body)
			rest.PropagateError(w, requestBody, fmt.Sprintf("rest_auth: cbauth.AuthWebCreds,"+
				" err: %v", err), http.StatusForbidden)
			return
		}

		if creds.Domain() == "anonymous" {
			// force basic auth login by sending 401
			CBAuthSendUnauthorized(w)
			return
		}
	}

	// redirect to /
	http.Redirect(w, req, "/", http.StatusMovedPermanently)
}

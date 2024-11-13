//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	bleveMappingUI "github.com/blevesearch/bleve-mapping-ui"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/buger/jsonparser"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// RestoreIndexHandler is a REST handler that processes
// a restore request at the service level across buckets.
type RestoreIndexHandler struct {
	mgr *cbgt.Manager
}

func NewRestoreIndexHandler(mgr *cbgt.Manager) *RestoreIndexHandler {
	return &RestoreIndexHandler{mgr: mgr}
}

func (h *RestoreIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	// parse and process the request body.
	indexDefs, err := processRemapRequest(req, "")
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_backup_restore: processRemapRequest "+
			"failed, err: %v", err), http.StatusBadRequest)
		return
	}

	// restore the remapped index definitions to the Cfg.
	err = restoreIndexDefs(indexDefs, h.mgr.Cfg())
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_backup_restore: "+
			"restoreIndexDefs failed, err: %v", err), http.StatusBadRequest)
		return
	}

	// send the remapped index definitions alone to the caller for
	// any verification purposes.
	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	rest.MustEncode(w, rv)
}

// BucketRestoreIndexHandler is a REST handler that processes
// a restore request at the bucket level.
type BucketRestoreIndexHandler struct {
	mgr *cbgt.Manager
}

func NewBucketRestoreIndexHandler(mgr *cbgt.Manager) *BucketRestoreIndexHandler {
	return &BucketRestoreIndexHandler{mgr: mgr}
}

func (h *BucketRestoreIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	bucketName := rest.BucketNameLookup(req)
	if bucketName == "" {
		rest.ShowError(w, req, "rest_backup_restore: bucket name is required",
			http.StatusBadRequest)
		return
	}
	// parse and process the request body.
	indexDefs, err := processRemapRequest(req, bucketName)
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_backup_restore: processRemapRequest failed,"+
			" for bucket: %s, err: %v", bucketName, err), http.StatusBadRequest)
		return
	}

	// restore the remapped index definitions to the Cfg.
	err = restoreIndexDefs(indexDefs, h.mgr.Cfg())
	if err != nil {
		rest.ShowError(w, req, fmt.Sprintf("rest_backup_restore: "+
			"restoreIndexDefs failed for bucket: %s, err: %v", bucketName,
			err), http.StatusBadRequest)
		return
	}

	// send the remapped index definitions alone to the caller for
	// any verification purposes.
	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	rest.MustEncode(w, rv)
}

func restoreIndexDefs(indexDefs *cbgt.IndexDefs, cfg cbgt.Cfg) error {
	curIndexDefs, cas, err := cbgt.CfgGetIndexDefs(cfg)
	if err != nil {
		return fmt.Errorf("CfgGetIndexDefs error: %v", err)
	}

	// indexDefs could be nil on a new cluster.
	if curIndexDefs == nil {
		curIndexDefs = cbgt.NewIndexDefs(cbgt.CfgGetVersion(cfg))
	}

	// update the remapped index definitions.
	for indexName, remappedIndexDef := range indexDefs.IndexDefs {
		delete(curIndexDefs.IndexDefs, indexName)
		curIndexDefs.IndexDefs[indexName] = remappedIndexDef
	}

	// fail upon any cas conflicts.
	_, err = cbgt.CfgSetIndexDefs(cfg, curIndexDefs, cas)
	if err != nil {
		return fmt.Errorf("CfgSetIndexDefs error: %v", err)
	}

	return nil
}

func processRemapRequest(req *http.Request, bucketName string) (
	*cbgt.IndexDefs, error) {
	requestBody, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body, err: %v", err)
	}

	indexDefs := &cbgt.IndexDefs{}
	if len(requestBody) > 0 {
		err := json.Unmarshal(requestBody, indexDefs)
		if err != nil {
			return nil, fmt.Errorf("requestBody: %s, json unmarshal err: %v", requestBody, err)
		}
	}

	if len(indexDefs.IndexDefs) == 0 {
		return nil, fmt.Errorf("requestBody: no index definitions parsed")
	}

	queryParams := req.URL.Query()
	params := queryParams.Get("remap")
	mapingRules, err := parseMappingParams(params)
	if err != nil {
		return nil, err
	}
	indexDefs, err = remapIndexDefinitions(indexDefs, mapingRules,
		bucketName, false)
	if err != nil {
		return nil, fmt.Errorf("index remapping error: %v", err)
	}

	return indexDefs, nil
}

func parseMappingParams(params string) (map[string]string, error) {
	rv := make(map[string]string)
	mappings := strings.Split(params, ",")
	for _, mapping := range mappings {
		if len(mapping) >= 3 {
			parts := strings.SplitN(mapping, ":", 2)
			// remap rule should be of a format r1:r2 at a minimum.
			// for example: b1:b2 or b1.s1:b2.s2 or b1.s1.c1:b2.s2.c2
			if len(parts) != 2 || len(parts[0]) < 1 || len(parts[1]) < 1 {
				return nil, fmt.Errorf("rest_backup_restore: "+
					"invalid mapping params found: %v", mapping)
			}
			rv[parts[0]] = parts[1]
		}
	}
	return rv, nil
}

func remapIndexDefinitions(indexDefs *cbgt.IndexDefs,
	mappingRules map[string]string, bucketName string,
	skipValidation bool) (*cbgt.IndexDefs, error) {
	for _, indexDef := range indexDefs.IndexDefs {
		// override the UUID during restore.
		indexDef.UUID = cbgt.NewUUID()

		// skip the index aliases from remapping.
		if indexDef.Type == "fulltext-alias" {
			continue
		}
		// there are no explicit mapping rules other than the bucketName from the URL.
		if len(mappingRules) == 0 && bucketName != "" {
			if indexDef.SourceName != bucketName {
				indexDef.SourceName = bucketName
			}
			continue
		}

		if len(indexDef.Params) > 0 {
			bleveParams := NewBleveParams()
			buf, err := bleveMappingUI.CleanseJSON([]byte(indexDef.Params))
			if err != nil {
				return nil, fmt.Errorf("rest_backup_restore: indexName: %s, "+
					"remap errs: %v", indexDef.Name, err)
			}

			err = json.Unmarshal(buf, bleveParams)
			if err != nil {
				return nil, fmt.Errorf("rest_backup_restore: indexName: %s, "+
					"json unmarshal errs: %v", indexDef.Name, err)
			}

			if strings.HasPrefix(bleveParams.DocConfig.Mode, ConfigModeCollPrefix) {
				if im, ok := bleveParams.Mapping.(*mapping.IndexMappingImpl); ok {
					remappedTypeMapping, newBucketName, err := remapTypeMappings(
						im.TypeMapping, mappingRules, indexDef.Name,
						indexDef.SourceName, bucketName != "")
					if err != nil {
						return nil, err
					}

					if newBucketName != "" &&
						indexDef.SourceName != newBucketName {
						indexDef.SourceName = newBucketName
					}

					im.TypeMapping = remappedTypeMapping
					ipBytes, err := json.Marshal(bleveParams)
					if err != nil {
						return nil, fmt.Errorf("rest_backup_restore: indexName: %s, "+
							"json marshal errs: %v", indexDef.Name, err)
					}
					indexDef.Params = string(ipBytes)

					var scopeName string
					if skipValidation {
						sName, _, _, err := getScopeCollTypeMappings(im, false)
						if err != nil {
							return nil, err
						}
						scopeName = sName
					} else {
						scope, err := validateScopeCollFromMappings(
							indexDef.SourceName, im, false, true)
						if err != nil {
							return nil, fmt.Errorf("rest_backup_restore: indexName: %s, "+
								"validation errs: %v", indexDef.Name, err)
						}
						scopeName = scope.Name
					}

					indexDef.Name, err = decorateIndexNameWithKeySpace(
						indexDef.SourceName, scopeName, indexDef.Name)
					if err != nil {
						return nil, fmt.Errorf("rest_backup_restore: err: %v", err)
					}
				}
			} else {
				if bname, ok := mappingRules[indexDef.SourceName]; ok {
					indexDef.SourceName = bname
				} else if bucketName != "" && bucketName != indexDef.SourceName {
					indexDef.SourceName = bucketName
				}
			}
		}
	}

	rv := cbgt.NewIndexDefs(indexDefs.ImplVersion)
	for _, indexDef := range indexDefs.IndexDefs {
		rv.IndexDefs[indexDef.Name] = indexDef
	}

	return rv, nil
}

// decorateIndexNameWithKeySpace updates the indexname
// by prefixing the $bucketName.$scopeName keyspace.
func decorateIndexNameWithKeySpace(sourceName, scopeName,
	indexName string) (string, error) {
	sourceName = sourceName + "."
	scopeName = scopeName + "."
	// note: '.' is a valid character only for bucket name;
	// if the index name was already decorated - then the "undecorated"
	// index name starts after the last '.';
	// if strings.LastIndex returns -1 then the name was undecorated already
	undecoratedIndexName := indexName[strings.LastIndex(indexName, ".")+1:]
	if len(undecoratedIndexName) == 0 {
		// bad index name, trailing "."
		return "", fmt.Errorf("invalid index name: %s", indexName)
	}

	if !isClusterCompatibleFor(FeatureScopedIndexNamesVersion) ||
		len(undecoratedIndexName) == len(indexName) {
		// no name decoration for a partially upgraded cluster, or if the index
		// name was earlier undecorated.
		return undecoratedIndexName, nil
	}

	// re-decorate index name
	decoratedIndexName := sourceName + scopeName + undecoratedIndexName
	if len(decoratedIndexName) > cbgt.MaxIndexNameLength {
		return "", fmt.Errorf("decorated index name %s has a length of %d, which "+
			"is longer than the maximum permissible length %d", decoratedIndexName,
			len(decoratedIndexName), cbgt.MaxIndexNameLength)
	}

	return decoratedIndexName, nil
}

func remapTypeMappings(typeMappings map[string]*mapping.DocumentMapping,
	mappingRules map[string]string, indexName, bucketName string,
	bucketLevel bool) (map[string]*mapping.DocumentMapping, string, error) {
	remappedTypeMappings := make(map[string]*mapping.DocumentMapping, 1)
	var newBucketName string
	for tp, dm := range typeMappings {
		if !dm.Enabled {
			remappedTypeMappings[tp] = dm
			continue
		}
		var remapped bool
		curScope, curCol, curTyp := scopeCollTypeMapping(tp)
		curmp := curScope + "." + curCol
		if !bucketLevel {
			curmp = bucketName + "." + curmp
		}

		for curMapping, newMapping := range mappingRules {
			bucket, scope, col, err := parseBucketScopeColNames(curMapping, bucketLevel)
			if err != nil {
				return nil, "", err
			}
			if (bucket == "" || bucket == bucketName) &&
				(scope == "" || scope == curScope) &&
				(col == "" || col == curCol) {
				newBucket, newScope, newCol, err := parseBucketScopeColNames(newMapping, bucketLevel)
				if err != nil {
					return nil, "", err
				}
				if newBucket == "" {
					newBucket = bucketName
				}
				if newScope == "" {
					newScope = curScope
				}
				if newCol == "" {
					newCol = curCol
				}
				newTyp := newScope + "." + newCol
				if curTyp != "" {
					newTyp += "." + curTyp
				}
				if newBucket != newBucketName {
					newBucketName = newBucket
				}
				remappedTypeMappings[newTyp] = dm
				remapped = true
				break
			}
		}
		// if remap isn't applicable then fallback to the current mappings.
		if !remapped {
			remappedTypeMappings[tp] = dm
		}
	}
	return remappedTypeMappings, newBucketName, nil
}

// parseBucketScopeColNames parses the remapping rules which would be
// of formats like bucket1:bucket2 or bucket1.scope1:bucket1.scope2 or
// bucket2.scope3.collection1:bucket2.scope3.collection2
func parseBucketScopeColNames(remapping string, bucketLevel bool) (bucket,
	scope, col string, err error) {
	args := strings.Split(remapping, ".")
	if len(args) >= 3 && bucketLevel {
		err = fmt.Errorf("Invalid remap rules: %s found for bucket", remapping)
		return
	}
	if len(args) == 1 {
		if bucketLevel {
			scope = args[0]
		} else {
			bucket = args[0]
		}
	}

	if len(args) == 2 {
		if bucketLevel {
			scope, col = args[0], args[1]
		} else {
			bucket, scope = args[0], args[1]
		}
	}

	if len(args) >= 3 {
		col = args[len(args)-1]
		scope = args[len(args)-2]
		for i := 0; i < len(args)-2; i++ {
			bucket = bucket + args[i]
		}
	}

	return bucket, scope, col, nil
}

// BackupIndexHandler is a REST handler that processes
// a backup request at the service level across buckets.
type BackupIndexHandler struct {
	mgr *cbgt.Manager
}

func NewBackupIndexHandler(mgr *cbgt.Manager) *BackupIndexHandler {
	return &BackupIndexHandler{mgr: mgr}
}

func (h *BackupIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	queryParams := req.URL.Query()
	var include bool
	params := queryParams.Get("exclude")
	if params == "" {
		params = queryParams.Get("include")
		include = true
	}

	bucketFilters, scopeFilters, colFilters := parseBackupFilters(params, "")

	indexDefs, _, err := cbgt.CfgGetIndexDefs(h.mgr.Cfg())
	if err != nil {
		rest.ShowError(w, req, "rest_backup_restore: "+
			"could not retrieve index defs", http.StatusInternalServerError)
		return
	}

	// filter index definitions for the given buckets/scopes/collections.
	indexDefs = filterIndexDefinitions(indexDefs, bucketFilters,
		scopeFilters, colFilters, include, false)

	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	rest.MustEncode(w, rv)
}

// BucketBackupIndexHandler is a REST handler that processes
// a backup request at a bucket level.
type BucketBackupIndexHandler struct {
	mgr *cbgt.Manager
}

func NewBucketBackupIndexHandler(mgr *cbgt.Manager) *BucketBackupIndexHandler {
	return &BucketBackupIndexHandler{mgr: mgr}
}

func (h *BucketBackupIndexHandler) ServeHTTP(
	w http.ResponseWriter, req *http.Request) {
	bucketName := rest.BucketNameLookup(req)
	if bucketName == "" {
		rest.ShowError(w, req, "rest_backup_restore: bucket name is required",
			http.StatusBadRequest)
		return
	}

	queryParams := req.URL.Query()
	var include bool
	params := queryParams.Get("exclude")
	if params == "" {
		params = queryParams.Get("include")
		include = true
	}

	_, scopeFilters, colFilters := parseBackupFilters(params, bucketName)

	indexDefs, _, err := cbgt.CfgGetIndexDefs(h.mgr.Cfg())
	if err != nil {
		rest.ShowError(w, req, "could not retrieve index defs", http.StatusInternalServerError)
		return
	}

	// filter index definitions for the given buckets/scopes/collections.
	indexDefs = filterIndexDefinitions(indexDefs, nil,
		scopeFilters, colFilters, include, true)

	rv := struct {
		Status    string          `json:"status"`
		IndexDefs *cbgt.IndexDefs `json:"indexDefs"`
	}{
		Status:    "ok",
		IndexDefs: indexDefs,
	}
	rest.MustEncode(w, rv)
}

// parseBackupFilters parses the backup filters which would be
// a comma seperated list of filters like,
// bucketName or bucketName.ScopeName or
// bucketName.ScopeName.CollectionName.
func parseBackupFilters(input string, bucketName string) (
	bucketFilters, scopeFilters, colFilters []string) {
	if len(input) == 0 {
		return
	}
	filters := strings.Split(input, ",")
	for _, filter := range filters {
		var levels []string
		if bucketName != "" {
			levels = strings.SplitN(filter, ".", 2)
		} else {
			levels = strings.SplitN(filter, ".", 3)
		}

		if len(levels) == 1 {
			if bucketName != "" {
				scopeFilters = append(scopeFilters, bucketName+"."+levels[0])
			} else {
				bucketFilters = append(bucketFilters, levels[0])
			}
			continue
		}

		if len(levels) == 2 {
			if bucketName != "" {
				colFilters = append(colFilters, bucketName+"."+levels[0]+"."+levels[1])
			} else {
				scopeFilters = append(scopeFilters, levels[0]+"."+levels[1])
			}
			continue
		}

		if len(levels) == 3 {
			colFilters = append(colFilters, levels[0]+"."+levels[1]+"."+levels[2])
		}
	}
	return cbgt.StringsRemoveDuplicates(bucketFilters),
		cbgt.StringsRemoveDuplicates(scopeFilters),
		cbgt.StringsRemoveDuplicates(colFilters)
}

func checkSourceNameMatchesFilters(sourceNames,
	bucketFilters, scopeFilters, colFilters []string) bool {
	// match upon empty filters
	if len(bucketFilters) == 0 && len(scopeFilters) == 0 &&
		len(colFilters) == 0 {
		return true
	}

	isMatch := func(sources, filters []string) bool {
		matched := make(map[string]struct{})
		for _, f := range filters {
			if len(f) <= 1 {
				continue
			}
			for _, sn := range sources {
				if strings.HasPrefix(sn, f) {
					matched[sn] = struct{}{}
				}
			}
			// all sources has to come under the filters.
			if len(matched) == len(sources) {
				return true
			}
		}

		return len(matched) == len(sources)
	}

	// check for the filters in the order of buckets, scopes and collections.
	ruless := [][]string{bucketFilters, scopeFilters, colFilters}
	for _, rules := range ruless {
		if isMatch(sourceNames, rules) {
			return true
		}
	}

	return false
}

func filterIndexDefinitions(indexDefs *cbgt.IndexDefs,
	bucketRules, scopeRules, colRules []string,
	include, bucketLevel bool) *cbgt.IndexDefs {
	if indexDefs == nil || len(indexDefs.IndexDefs) == 0 {
		return indexDefs
	}

	rv := make(map[string]*cbgt.IndexDef, 1)
	for _, indexDef := range indexDefs.IndexDefs {
		// include index-aliases only for cluster level invocations.
		if indexDef.Type == "fulltext-alias" {
			if !bucketLevel {
				rv[indexDef.Name] = indexDef
			}
			continue
		}
		// fetch all the sourceNames for the index definition.
		sourceNames := parseSourceNamesFromIndexDefs(indexDef)
		matches := checkSourceNameMatchesFilters(sourceNames,
			bucketRules, scopeRules, colRules)
		if matches && include {
			rv[indexDef.Name] = indexDef
			continue
		}
		if !matches && !include {
			rv[indexDef.Name] = indexDef
			continue
		}
	}
	indexDefs.IndexDefs = rv
	// reset the indexUUIDs and sourceUUIDs to make it
	// restore friendly.
	for _, indexDef := range indexDefs.IndexDefs {
		indexDef.UUID = ""
		indexDef.SourceUUID = ""
	}
	return indexDefs
}

func parseSourceNamesFromIndexDefs(indexDef *cbgt.IndexDef) []string {
	if len(indexDef.Params) > 0 {
		bleveParamBytes := []byte(indexDef.Params)
		docConfig, _, _, err := jsonparser.Get(bleveParamBytes, "doc_config")
		if err != nil {
			return []string{indexDef.SourceName + "." + defaultScopeName +
				"." + defaultCollName}
		}

		docConfigMode, _, _, _ := jsonparser.Get(docConfig, "mode")
		if strings.HasPrefix(string(docConfigMode), "scope.collection") {
			bmapping, _, _, err := jsonparser.Get(bleveParamBytes, "mapping")
			if err != nil {
				return nil
			}

			mapping := bleve.NewIndexMapping()
			err = json.Unmarshal(bmapping, mapping)
			if err != nil {
				return nil
			}

			sname, colNames, _, err := getScopeCollTypeMappings(mapping, true)
			if err != nil {
				return nil
			}

			var rv []string
			for _, cname := range colNames {
				rv = append(rv, indexDef.SourceName+"."+sname+"."+cname)
			}
			sort.Strings(rv)
			return rv
		}
	}

	if indexDef.SourceName != "" {
		return []string{indexDef.SourceName + "." + defaultScopeName +
			"." + defaultCollName}
	}

	return nil
}

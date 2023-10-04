//  Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbft

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
)

const defaultScopeName = "_default"

const defaultCollName = "_default"

type sourceDetails struct {
	scopeName      string
	collUIDNameMap map[uint32]string // coll uid => coll name
}

type collMetaFieldCache struct {
	m                sync.RWMutex
	cache            map[string]string         // indexName$collName => _$suid_$cuid
	sourceDetailsMap map[string]*sourceDetails // indexName => sourceDetails
}

// metaFieldValCache holds a runtime volatile cache
// for collection meta field look ups during the collection
// specific query in a multi collection index.
var metaFieldValCache *collMetaFieldCache

func init() {
	metaFieldValCache = &collMetaFieldCache{
		cache:            make(map[string]string),
		sourceDetailsMap: make(map[string]*sourceDetails),
	}
}

// refreshMetaFieldValCache updates the metaFieldValCache with
// the latest index definitions.
func refreshMetaFieldValCache(indexDefs *cbgt.IndexDefs) {
	if indexDefs == nil || metaFieldValCache == nil {
		return
	}

	bleveParams := NewBleveParams()
	for _, indexDef := range indexDefs.IndexDefs {
		err := json.Unmarshal([]byte(indexDef.Params), bleveParams)
		if err != nil {
			log.Printf("collection_utils: refreshMetaFieldValCache,"+
				" indexName: %v, err: %v", indexDef.Name, err)
			continue
		}
		if strings.HasPrefix(bleveParams.DocConfig.Mode, ConfigModeCollPrefix) {
			if im, ok := bleveParams.Mapping.(*mapping.IndexMappingImpl); ok {
				scope, err := validateScopeCollFromMappings(indexDef.SourceName,
					im, false)
				if err != nil {
					return
				}
				_ = initMetaFieldValCache(indexDef.Name, indexDef.SourceName, im, scope)
			}
		}
	}
}

func (c *collMetaFieldCache) getMetaFieldValue(indexName, collName string) string {
	c.m.RLock()
	rv := c.cache[indexName+"$"+collName]
	c.m.RUnlock()
	return rv
}

func encodeCollMetaFieldValue(suid, cuid int64) string {
	return fmt.Sprintf("_$%d_$%d", suid, cuid)
}

func (c *collMetaFieldCache) setValue(indexName, scopeName string,
	suid int64, collName string, cuid int64, multiCollIndex bool) {
	key := indexName + "$" + collName
	c.m.Lock()
	c.cache[key] = encodeCollMetaFieldValue(suid, cuid)

	if multiCollIndex {
		var sd *sourceDetails
		var ok bool
		if sd, ok = c.sourceDetailsMap[indexName]; !ok {
			sd = &sourceDetails{scopeName: scopeName,
				collUIDNameMap: make(map[uint32]string)}
			c.sourceDetailsMap[indexName] = sd
		}
		sd.collUIDNameMap[uint32(cuid)] = collName
	}

	c.m.Unlock()
}

func (c *collMetaFieldCache) reset(indexName string) {
	prefix := indexName + "$"
	c.m.Lock()
	delete(c.sourceDetailsMap, indexName)

	for k := range c.cache {
		if strings.HasPrefix(k, prefix) {
			delete(c.cache, k)
		}
	}
	c.m.Unlock()
}

func (c *collMetaFieldCache) getSourceDetailsMap(indexName string) (
	*sourceDetails, bool) {
	c.m.RLock()
	sdm, multiCollIndex := c.sourceDetailsMap[indexName]
	var ret *sourceDetails
	if sdm != nil {
		// obtain a copy of the sourceDetails
		ret = &sourceDetails{
			scopeName:      sdm.scopeName,
			collUIDNameMap: make(map[uint32]string),
		}

		for k, v := range sdm.collUIDNameMap {
			ret.collUIDNameMap[k] = v
		}
	}
	c.m.RUnlock()
	return ret, multiCollIndex
}

func (c *collMetaFieldCache) getScopeCollectionNames(indexName string) (
	scopeName string, collectionNames []string) {
	c.m.RLock()
	sdm, _ := c.sourceDetailsMap[indexName]
	if sdm != nil {
		scopeName = sdm.scopeName
		collectionNames = make([]string, len(sdm.collUIDNameMap))
		i := 0
		for _, v := range sdm.collUIDNameMap {
			collectionNames[i] = v
			i++
		}
	}
	c.m.RUnlock()
	return
}

func scopeCollTypeMapping(in string) (string, string, string) {
	vals := strings.SplitN(in, ".", 3)
	scope := "_default"
	collection := "_default"
	typeMapping := ""
	if len(vals) == 1 {
		typeMapping = vals[0]
	} else if len(vals) == 2 {
		scope = vals[0]
		collection = vals[1]
	} else {
		scope = vals[0]
		collection = vals[1]
		typeMapping = vals[2]
	}

	return scope, collection, typeMapping
}

// getScopeCollTypeMappings will return a deduplicated
// list of collection names when skipMappings is enabled.
func getScopeCollTypeMappings(im *mapping.IndexMappingImpl,
	skipMappings bool) (scope string,
	cols []string, typeMappings []string, err error) {
	// index the _default/_default scope and collection when
	// default mapping is enabled.
	if im.DefaultMapping.Enabled {
		scope = defaultScopeName
		cols = []string{defaultCollName}
		typeMappings = []string{""}
	}

	hash := make(map[string]struct{}, len(im.TypeMapping))
	for tp, dm := range im.TypeMapping {
		if !dm.Enabled {
			continue
		}
		s, c, t := scopeCollTypeMapping(tp)
		key := c
		if !skipMappings {
			key += t
		}
		if _, exists := hash[key]; !exists {
			hash[key] = struct{}{}
			cols = append(cols, c)
			if !skipMappings {
				typeMappings = append(typeMappings, t)
			}
		}
		if scope == "" {
			scope = s
			continue
		}
		if scope != s {
			return "", nil, nil, fmt.Errorf("collection_utils: multiple scopes"+
				" found: '%s', '%s'; index can only span collections on a single"+
				" scope", scope, s)
		}
	}

	return scope, cols, typeMappings, nil
}

// validateScopeCollFromMappings performs the $scope.$collection
// validations in the type mappings defined. It also performs
// - single scope validation across collections
// - verify scope to collection mapping with kv manifest
func validateScopeCollFromMappings(bucket string,
	im *mapping.IndexMappingImpl, ignoreCollNotFoundErrs bool) (*Scope, error) {
	sName, collNames, typeMappings, err := getScopeCollTypeMappings(im, false)
	if err != nil {
		return nil, err
	}

	manifest, err := GetBucketManifest(bucket)
	if err != nil {
		return nil, err
	}

	rv := &Scope{Collections: make([]Collection, 0, len(collNames))}
	for _, scope := range manifest.Scopes {
		if scope.Name == sName {
			rv.Name = scope.Name
			rv.Uid = scope.Uid
		OUTER:
			for i := range collNames {
				for _, collection := range scope.Collections {
					if collection.Name == collNames[i] {
						rv.Collections = append(rv.Collections, Collection{
							Uid:         collection.Uid,
							Name:        collection.Name,
							typeMapping: typeMappings[i],
						})
						continue OUTER
					}
				}
				if !ignoreCollNotFoundErrs {
					return nil, fmt.Errorf("collection_utils: collection:"+
						" '%s' doesn't belong to scope: '%s' in bucket: '%s'",
						collNames[i], sName, bucket)
				}
			}
			break
		}
	}

	if rv.Name == "" {
		return nil, fmt.Errorf("collection_utils: scope:"+
			" '%s' not found in bucket: '%s' ",
			sName, bucket)
	}

	return rv, nil
}

func enhanceMappingsWithCollMetaField(tm map[string]*mapping.DocumentMapping) error {
OUTER:
	for _, mp := range tm {
		fm := metaFieldMapping()
		for fname := range mp.Properties {
			if fname == CollMetaFieldName {
				continue OUTER
			}
		}
		mp.AddFieldMappingsAt(CollMetaFieldName, fm)
	}
	return nil
}

func metaFieldMapping() *mapping.FieldMapping {
	fm := mapping.NewTextFieldMapping()
	fm.Store = false
	fm.DocValues = false
	fm.IncludeInAll = false
	fm.IncludeTermVectors = false
	fm.Name = CollMetaFieldName
	fm.Analyzer = "keyword"
	return fm
}

// -----------------------------------------------------------------------------

// API to retrieve the scope & unique list of collection names from the
// provided index definition.
func GetScopeCollectionsFromIndexDef(indexDef *cbgt.IndexDef) (
	scope string, collections []string, err error) {
	if indexDef == nil || indexDef.Type != "fulltext-index" {
		return "", nil,
			fmt.Errorf("no fulltext-index definition provided")
	}

	bp := NewBleveParams()
	if len(indexDef.Params) > 0 {
		if err = json.Unmarshal([]byte(indexDef.Params), bp); err != nil {
			return "", nil, err
		}

		if strings.HasPrefix(bp.DocConfig.Mode, ConfigModeCollPrefix) {
			if im, ok := bp.Mapping.(*mapping.IndexMappingImpl); ok {
				var collectionNames []string
				scope, collectionNames, _, err := getScopeCollTypeMappings(im, false)
				if err != nil {
					return "", nil, err
				}

				uniqueCollections := map[string]struct{}{}
				for _, coll := range collectionNames {
					if _, exists := uniqueCollections[coll]; !exists {
						uniqueCollections[coll] = struct{}{}
						collections = append(collections, coll)
					}
				}

				return scope, collections, nil
			}
		}
	}

	return "_default", []string{"_default"}, nil
}

func multiCollection(colls []Collection) bool {
	hash := make(map[string]struct{}, 1)
	for _, c := range colls {
		if _, ok := hash[c.Name]; !ok {
			hash[c.Name] = struct{}{}
		}
	}
	return len(hash) > 1
}

// -----------------------------------------------------------------------------

// obtainIndexSourceFromDefinition obtains the source name from the index definition
// and recursively if the index definition is an alias.
// - input argument `rv` will be updated to contain a map of source names (buckets).
func obtainIndexSourceFromDefinition(mgr *cbgt.Manager, indexName string,
	rv map[string]struct{}) (map[string]struct{}, error) {
	if rv == nil {
		rv = make(map[string]struct{})
	}

	if bucket, _ := getKeyspaceFromScopedIndexName(indexName); len(bucket) > 0 {
		// indexName is a scoped index name
		rv[bucket] = struct{}{}
		return rv, nil
	}

	// else obtain bucket from definition of index
	indexDef, _, err := mgr.GetIndexDef(indexName, false)
	if err != nil || indexDef == nil {
		return rv, fmt.Errorf("failed to retrieve index def for `%v`, err: %v",
			indexName, err)
	}

	if indexDef.Type == "fulltext-index" {
		rv[indexDef.SourceName] = struct{}{}
		return rv, nil
	} else if indexDef.Type == "fulltext-alias" {
		params := AliasParams{}
		err := UnmarshalJSON([]byte(indexDef.Params), &params)
		if err == nil {
			for idxName := range params.Targets {
				if rv, err = obtainIndexSourceFromDefinition(mgr, idxName, rv); err != nil {
					return rv, err
				}
			}
		}
	} else {
		// unrecognized index type
		return rv, fmt.Errorf("unrecognized index type %v", indexDef.Type)
	}

	return rv, nil
}

const maxAliasDepth = 50

// obtainIndexBucketScopesForDefinition obtains the bucket.scope sources for the
// index definition and if it's alias - by recursively looking through the targets.
// it also performs a check for alias cycles, returning an error if an alias“
// cycle is detected. Cycle detection in the directed alias graph is done using the
// three color DFS algorithm from CLRS.
func obtainIndexBucketScopesForDefinition(mgr *cbgt.Manager,
	indexDef *cbgt.IndexDef, rv map[string]int, visitedAndInPath map[string]bool,
	depth int) (map[string]int, error) {
	if rv == nil {
		rv = make(map[string]int)
	}
	if visitedAndInPath == nil {
		visitedAndInPath = make(map[string]bool)
	}
	if depth > maxAliasDepth {
		return rv, fmt.Errorf("alias expansion depth exceeded, maxAliasDepth allowed is %v", maxAliasDepth)
	}
	// else obtain bucket & scope from definition of index
	if indexDef.Type == "fulltext-index" {
		bucketName, scopeName := getKeyspaceFromScopedIndexName(indexDef.Name)
		if len(bucketName) > 0 && len(scopeName) > 0 {
			// indexName is a scoped index name
			key := bucketName + "." + scopeName
			rv[key]++
			return rv, nil
		}

		scope, _, err := GetScopeCollectionsFromIndexDef(indexDef)
		if err != nil {
			return rv, err
		}
		key := indexDef.SourceName + "." + scope
		rv[key]++
		return rv, nil
	} else if indexDef.Type == "fulltext-alias" {
		visitedAndInPath[indexDef.Name] = true
		params, err := parseAliasParams(indexDef.Params)
		if err == nil {
			for idxName := range params.Targets {
				inPath, visited := visitedAndInPath[idxName]
				if inPath && visited {
					return rv, fmt.Errorf("cyclic index alias %s "+
						"detected in alias path", idxName)
				}
				if visited {
					continue
				}
				idxDef, err := mgr.CheckAndGetIndexDef(idxName, true)
				if err != nil {
					return rv, fmt.Errorf("failed to retrieve index defs for `%v`, err: %v",
						idxDef.Name, err)
				}
				bucketName, scopeName := getKeyspaceFromScopedIndexName(idxName)
				if len(bucketName) > 0 && len(scopeName) > 0 {
					// indexName is a scoped index name
					key := bucketName + "." + scopeName
					rv[key]++
					if idxDef == nil {
						return rv, fmt.Errorf("scoped index target %v not found", idxName)
					}
					if idxDef.Type == "fulltext-alias" {
						err := detectIndexAliasCycle(mgr, idxDef, visitedAndInPath, depth+1)
						if err != nil {
							return rv, err
						}
					}
					continue
				}
				// global alias or non-scoped index target does not exist
				if idxDef == nil {
					continue
				}
				if rv, err = obtainIndexBucketScopesForDefinition(mgr, idxDef, rv, visitedAndInPath, depth+1); err != nil {
					return rv, err
				}
			}
			visitedAndInPath[indexDef.Name] = false
		}
	} else {
		// unrecognized index type
		return rv, fmt.Errorf("unrecognized index type %v", indexDef.Type)
	}

	return rv, nil
}

func detectIndexAliasCycle(mgr *cbgt.Manager, indexDef *cbgt.IndexDef,
	visitedAndInPath map[string]bool, depth int) error {
	// Detect cycle in the alias graph using an iterative DFS
	type stackEntry struct {
		indexDef *cbgt.IndexDef
		depth    int
	}
	var stack []*stackEntry
	stack = append(stack, &stackEntry{indexDef: indexDef, depth: depth})
	for len(stack) > 0 {
		indexDef = stack[len(stack)-1].indexDef
		currentDepth := stack[len(stack)-1].depth
		if stack[len(stack)-1].depth > maxAliasDepth {
			return fmt.Errorf("alias expansion depth exceeded, maxAliasDepth allowed is %v", maxAliasDepth)
		}
		if visitedAndInPath[indexDef.Name] {
			visitedAndInPath[indexDef.Name] = false
			stack = stack[:len(stack)-1]
			continue
		}
		visitedAndInPath[indexDef.Name] = true
		params, err := parseAliasParams(indexDef.Params)
		if err != nil {
			return err
		}
		for target := range params.Targets {
			targetIndexDef, err := mgr.CheckAndGetIndexDef(target, false)
			if err != nil {
				return fmt.Errorf("failed to retrieve index defs for `%v`, err: %v",
					targetIndexDef.Name, err)
			}
			if targetIndexDef == nil {
				return fmt.Errorf("scoped index target %v not found", targetIndexDef.Name)
			}
			inPath, visited := visitedAndInPath[targetIndexDef.Name]
			if inPath && visited {
				return fmt.Errorf("cyclic index alias %s "+
					"detected in alias path", targetIndexDef.Name)
			}
			if visited {
				continue
			}
			if targetIndexDef.Type == "fulltext-alias" {
				stack = append(stack, &stackEntry{indexDef: targetIndexDef, depth: currentDepth + 1})
			}
		}
	}
	return nil
}

// getKeyspaceFromScopedIndexName gets the bucket and
// scope names from an index name if available
func getKeyspaceFromScopedIndexName(indexName string) (string, string) {
	pos1 := strings.LastIndex(indexName, ".")
	if pos1 > 0 {
		pos2 := strings.LastIndex(indexName[:pos1], ".")
		if pos2 > 0 {
			return indexName[:pos2], indexName[pos2+1 : pos1]
		}
	}

	return "", ""

}

//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cbft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/mapping"
	"github.com/couchbase/cbgt"
)

const defaultScopeName = "_default"

const defaultCollName = "_default"

type collMetaFieldCache struct {
	m                sync.RWMutex
	cache            map[string]string            // indexName$collName => _$suid_$cuid
	collUIDNameCache map[string]map[uint32]string // indexName => coll uid => coll name
}

// metaFieldValCache holds a runtime volatile cache
// for collection meta field look ups during the collection
// specific query in a multi collection index.
var metaFieldValCache *collMetaFieldCache

func init() {
	metaFieldValCache = &collMetaFieldCache{
		cache:            make(map[string]string),
		collUIDNameCache: make(map[string]map[uint32]string),
	}
}

func (c *collMetaFieldCache) getValue(indexName, collName string) string {
	c.m.RLock()
	rv := c.cache[indexName+"$"+collName]
	c.m.RUnlock()
	return rv
}

func encodeCollMetaFieldValue(suid, cuid int64) string {
	return "_$" + fmt.Sprintf("%d", suid) + "_$" + fmt.Sprintf("%d", cuid)
}

func (c *collMetaFieldCache) setValue(indexName, collName string,
	suid, cuid int64, multiCollIndex bool) {
	key := indexName + "$" + collName
	c.m.Lock()

	c.cache[key] = encodeCollMetaFieldValue(suid, cuid)
	if multiCollIndex {
		var indexMap map[uint32]string
		var ok bool
		if indexMap, ok = c.collUIDNameCache[indexName]; !ok {
			indexMap = make(map[uint32]string)
			c.collUIDNameCache[indexName] = indexMap
		}
		indexMap[uint32(cuid)] = collName
	}

	c.m.Unlock()
}

func (c *collMetaFieldCache) reset(indexName string) {
	prefix := indexName + "$"
	c.m.Lock()
	delete(c.collUIDNameCache, indexName)
	for k := range c.cache {
		if strings.HasPrefix(k, prefix) {
			delete(c.cache, k)
		}
	}
	c.m.Unlock()
}

func (c *collMetaFieldCache) getCollUIDNameMap(indexName string) (
	collUIDNameCache map[uint32]string, multiCollIndex bool) {
	c.m.RLock()
	collUIDNameCache, multiCollIndex = c.collUIDNameCache[indexName]
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
				" found: %s , %s, index can only span collections on a single"+
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
					return nil, fmt.Errorf("collection_utils: collection: "+
						" %s doesn't belong to scope: %s in bucket: %s",
						collNames[i], sName, bucket)
				}
			}
			break
		}
	}

	if rv.Name == "" {
		return nil, fmt.Errorf("collection_utils: scope: "+
			" %s not found in bucket: %s ",
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

const bQuotes = byte('"')
const bSliceEnd = byte(']')
const bCurlyBraceStart = byte('{')
const bCurlyBraceEnd = byte('}')
const bBooleanEnd = byte('e')
const bNumericStart = byte('0')
const bNumericEnd = byte('9')

func metaFieldPosition(input []byte) int {
	for i := bytes.LastIndex(input, []byte("}")) - 1; i > 0; i-- {
		if input[i] == bQuotes || input[i] == bSliceEnd ||
			input[i] == bCurlyBraceEnd || input[i] == bBooleanEnd ||
			(input[i] >= bNumericStart && input[i] <= bNumericEnd) {
			return i + 1
		}
	}
	return -1
}

func metaFieldContents(value string) []byte {
	return []byte(fmt.Sprintf(",\"_$scope_$collection\":\"%s\"}", value))
}

// -----------------------------------------------------------------------------

// API to retrieve the scope & unique list of collection names from the
// provided index definition.
func GetScopeCollectionsFromIndexDef(indexDef *cbgt.IndexDef) (
	scope string, collections []string, err error) {
	if indexDef == nil {
		return "", nil, fmt.Errorf("no index-def provided")
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

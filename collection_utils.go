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
	"fmt"
	"strings"

	"github.com/blevesearch/bleve/mapping"
)

func scopeCollName(in string) (string, string, error) {
	vals := strings.SplitN(in, ".", 3)
	if len(vals) < 2 {
		return "", "",
			fmt.Errorf("collection_utils: invalid type_mod with scope.collection")
	}
	return vals[0], vals[1], nil
}

func getScopeCollNames(tm map[string]*mapping.DocumentMapping) (scope string,
	cols []string, err error) {
	hash := make(map[string]struct{}, len(tm))
	for tp := range tm {
		s, c, err := scopeCollName(tp)
		if err != nil {
			return "", nil, err
		}
		if _, exists := hash[c]; !exists {
			hash[c] = struct{}{}
			cols = append(cols, c)
		}
		if scope == "" {
			scope = s
			continue
		}
		if scope != s {
			return "", nil, fmt.Errorf("collection_utils: multiple scopes"+
				" found: %s , %s, index can only span collections on a single"+
				"scope", scope, s)
		}
	}
	return scope, cols, nil
}

// validateScopeCollFromMappings performs the $scope.$collection
// validations in the type mappings defined. It also performs
// - single scope validation across collections
// - verity of scope to collection mapping against the kv
func validateScopeCollFromMappings(bucket string,
	tm map[string]*mapping.DocumentMapping) (*Scope, error) {
	sName, colNames, err := getScopeCollNames(tm)
	if err != nil {
		return nil, err
	}

	manifest, err := GetBucketManifest(bucket)
	if err != nil {
		return nil, err
	}

	rv := &Scope{Collections: make([]Collection, 0, len(colNames))}
	for _, scope := range manifest.Scopes {
		if scope.Name == sName {
			rv.Name = scope.Name
			rv.Uid = scope.Uid
		OUTER:
			for _, colName := range colNames {
				for _, collection := range scope.Collections {
					if collection.Name == colName {
						rv.Collections = append(rv.Collections,
							Collection{Uid: collection.Uid, Name: collection.Name})
						continue OUTER
					}
				}
				return nil, fmt.Errorf("collection_utils: collection: "+
					" %s doesn't belong to scope: %s in bucket: %s",
					colName, sName, bucket)
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

//  Copyright 2024-Present Couchbase, Inc.
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
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/couchbase/cbgt"
)

// validateSearchRequestAgainstIndex validates the search request against the index.
func validateSearchRequestAgainstIndex(mgr *cbgt.Manager,
	indexName, indexUUID string, collections []string,
	searchRequest *bleve.SearchRequest) error {
	if mgr == nil || len(indexName) == 0 || searchRequest == nil {
		return fmt.Errorf("query_validate:"+
			" Invalid input parameters - indexName: %s, searchRequest: %v",
			indexName, searchRequest)
	}

	// obtain the index definition
	indexDef, _, err := mgr.GetIndexDef(indexName, false)
	if err != nil || (len(indexUUID) > 0 && indexDef.UUID != indexUUID) {
		return fmt.Errorf("query_validate:"+
			" Unable to get index definition or match UUID, err: %v", err)
	}

	if indexDef.Type != "fulltext-index" {
		// validation NOT supported for fulltext-aliases
		return nil
	}

	var collectionsMap map[string]struct{}
	if len(collections) > 0 {
		// create a map of collections for faster lookup
		collectionsMap = make(map[string]struct{})
		for _, collection := range collections {
			collectionsMap[collection] = struct{}{}
		}
	}

	indexedProperties, err := processIndexDef(indexDef, collectionsMap, nil)
	if err != nil {
		return fmt.Errorf("query_validate:"+
			" Unable to process index definition, err: %v", err)
	}

	queryFields, err := processSearchRequest(searchRequest)
	if err != nil {
		return fmt.Errorf("query_validate:"+
			" Unable to process search request, err: %v", err)
	}

OUTER:
	for entry := range queryFields {
		if _, exists := indexedProperties[entry]; exists {
			// field entry for query found, all good
			continue
		}

		if entry.Type == "geopoint" ||
			entry.Type == "geoshape" ||
			entry.Type == "IP" ||
			entry.Type == "datetime" ||
			entry.Type == "vector" {
			// strict field types that require non-dynamic mappings
			msg := fmt.Sprintf("query_validate:"+
				" field not indexed, name: %s, type: %s", entry.Name, entry.Type)
			if entry.Type == "vector" {
				msg += fmt.Sprintf(", dims: %d", entry.Dims)
			}
			return fmt.Errorf(msg)
		}

		// check for dynamic mappings for other field types - "text", "number", "boolean"
		if isDynamic, exists := indexedProperties[indexProperty{Name: ""}]; exists && isDynamic {
			// complete dynamic mapping enabled
			continue OUTER
		}

		queryFieldSplitOnDot := strings.Split(entry.Name, ".")
		if len(queryFieldSplitOnDot) == 1 {
			// no need to check for dynamic mappings
			return fmt.Errorf("query_validate:"+
				" field not indexed, name: %s, type: %s", entry.Name, entry.Type)
		}

		for i := 0; i < len(queryFieldSplitOnDot)-1; i++ {
			dynamicField := strings.Join(queryFieldSplitOnDot[:i+1], ".")
			if isDynamic, exists := indexedProperties[indexProperty{Name: dynamicField}]; exists && isDynamic {
				// dynamic child mapping enabled
				continue OUTER
			}
		}

		return fmt.Errorf("query_validate:"+
			" field not indexed, field: %s, type: %s", entry.Name, entry.Type)
	}

	return nil
}

// -----------------------------------------------------------------------------

// indexProperty represents a property of an index.
type indexProperty struct {
	Name string
	Type string
	Dims int
}

// processIndexDef obtains all the indexed properties from a "fulltext-index" definition.
func processIndexDef(indexDef *cbgt.IndexDef, collections map[string]struct{},
	indexedProperties map[indexProperty]bool) (map[indexProperty]bool, error) {
	if indexDef == nil {
		return indexedProperties, nil
	}

	bp := NewBleveParams()
	if err := json.Unmarshal([]byte(indexDef.Params), bp); err != nil {
		return nil, err
	}

	im, ok := bp.Mapping.(*mapping.IndexMappingImpl)
	if !ok {
		return nil, nil
	}

	if indexedProperties == nil {
		indexedProperties = map[indexProperty]bool{}
	}

	var scopedCollectionMappings bool
	if strings.HasPrefix(bp.DocConfig.Mode, "scope.collection.") {
		scopedCollectionMappings = true
	}

	var isCollectionRequested = func(mappingName string) bool {
		if len(collections) == 0 {
			// request applicable to all collections
			return true
		}

		collectionName := "_default"
		if scopedCollectionMappings {
			dotSplit := strings.Split(mappingName, ".")
			if len(dotSplit) > 1 {
				collectionName = dotSplit[1]
			}
		}

		_, exists := collections[collectionName]
		return exists
	}

	if isCollectionRequested("_default") {
		// check within default mapping if and only if "_default" collection is requested
		// or no collections are requested
		if im.DefaultMapping != nil && im.DefaultMapping.Enabled {
			if im.DefaultMapping.Dynamic {
				// complete dynamic mapping enabled
				indexedProperties[indexProperty{Name: ""}] = true
			}

			indexedProperties = processDocMapping(nil, im.DefaultMapping, indexedProperties)
		}
	}

	for tmName, tm := range im.TypeMapping {
		if !isCollectionRequested(tmName) {
			continue
		}

		if tm != nil && tm.Enabled {
			if tm.Dynamic {
				// complete dynamic mapping enabled
				indexedProperties[indexProperty{Name: ""}] = true
			}

			indexedProperties = processDocMapping(nil, tm, indexedProperties)
		}
	}

	return indexedProperties, nil
}

// processSearchRequest extracts all the query fields from the search request.
func processSearchRequest(sr *bleve.SearchRequest) (map[indexProperty]struct{}, error) {
	if sr == nil {
		return nil, nil
	}

	queryFields, err := extractQueryFields(sr, nil)
	if err != nil {
		return nil, err
	}

	queryFields, err = extractKNNQueryFields(sr, queryFields)
	if err != nil {
		return nil, err
	}

	return queryFields, nil
}

// -----------------------------------------------------------------------------

func processDocMapping(path []string, mapping *mapping.DocumentMapping,
	indexedProperties map[indexProperty]bool) map[indexProperty]bool {
	if mapping == nil {
		return indexedProperties
	}

	for _, f := range mapping.Fields {
		if f == nil || !f.Index || len(path) <= 0 {
			continue
		}

		fpath := append([]string(nil), path...) // Copy.
		fpath[len(fpath)-1] = f.Name

		ip := indexProperty{
			Name: strings.Join(fpath, "."),
			Type: f.Type,
			Dims: f.Dims,
		}
		if ip.Type == "vector_base64" {
			// override to vector
			ip.Type = "vector"
		}

		indexedProperties[ip] = false

		if ip.Type != "vector" {
			// check if field is indexed in _all
			if f.IncludeInAll {
				indexedProperties[indexProperty{
					Name: "_all",
					Type: f.Type,
				}] = false
			}
		}
	}

	for propName, propMapping := range mapping.Properties {
		if propMapping == nil {
			continue
		}

		if propMapping.Dynamic {
			// dynamic child mapping enabled
			indexedProperties[indexProperty{
				Name: strings.Join(append(path, propName), "."),
			}] = true
		}

		indexedProperties = processDocMapping(append(path, propName), propMapping, indexedProperties)
	}

	return indexedProperties
}

// extractQueryFields extracts all the query fields from the search request.
func extractQueryFields(sr *bleve.SearchRequest,
	queryFields map[indexProperty]struct{}) (map[indexProperty]struct{}, error) {
	if sr.Query == nil {
		return nil, nil
	}

	var walk func(que query.Query) error

	walk = func(que query.Query) error {
		switch qq := que.(type) {
		case *query.BooleanQuery:
			walk(qq.Must)
			walk(qq.MustNot)
			walk(qq.Should)
		case *query.ConjunctionQuery:
			for _, childQ := range qq.Conjuncts {
				walk(childQ)
			}
		case *query.DisjunctionQuery:
			for _, childQ := range qq.Disjuncts {
				walk(childQ)
			}
		case *query.QueryStringQuery:
			q, err := qq.Parse()
			if err != nil {
				return err
			}
			walk(q)
		default:
			if fq, ok := que.(query.FieldableQuery); ok {
				fieldDesc := indexProperty{
					Name: fq.Field(),
				}

				switch fq.(type) {
				case *query.BoolFieldQuery:
					fieldDesc.Type = "boolean"
				case *query.NumericRangeQuery:
					fieldDesc.Type = "number"
				case *query.DateRangeStringQuery:
					fieldDesc.Type = "datetime"
				case *query.GeoBoundingBoxQuery,
					*query.GeoDistanceQuery,
					*query.GeoBoundingPolygonQuery:
					fieldDesc.Type = "geopoint"
				case *query.GeoShapeQuery:
					fieldDesc.Type = "geoshape"
				case *query.IPRangeQuery:
					fieldDesc.Type = "IP"
				default:
					// The rest are all of Type: "text"
					//   - *query.MatchQuery
					//   - *query.MatchPhraseQuery
					//   - *query.TermQuery
					//   - *query.PhraseQuery
					//   - *query.MultiPhraseQuery
					//   - *query.FuzzyQuery
					//   - *query.PrefixQuery
					//   - *query.RegexpQuery
					//   - *query.WildcardQuery
					//   - *query.TermRangeQuery
					fieldDesc.Type = "text"
				}

				if queryFields == nil {
					queryFields = map[indexProperty]struct{}{}
				}

				if len(fieldDesc.Name) == 0 {
					fieldDesc.Name = "_all" // look within composite field
				}

				queryFields[fieldDesc] = struct{}{}
			}
			// The following are non-Fieldable queries:
			//   - *query.DocIDQuery
			//   - *query.MatchAllQuery
			//   - *query.MatchNoneQuery
		}

		return nil
	}

	err := walk(sr.Query)
	if err != nil {
		return nil, err
	}

	return queryFields, nil
}

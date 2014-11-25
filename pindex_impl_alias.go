//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/blevesearch/bleve"
)

func init() {
	// Register alias with empty instantiation functions,
	// so that "alias" will show up in valid index types.
	RegisterPIndexImplType("alias", &PIndexImplType{
		Count:       CountAlias,
		Query:       QueryAlias,
		Description: "alias - supports fan-out of queries to multiple index targets",
		StartSample: &AliasSchema{
			Targets: map[string]*AliasSchemaTarget{
				"yourIndexName": &AliasSchemaTarget{},
			},
		},
	})
}

// AliasSchema holds the definition for a user-defined index alias.  A
// user-defined index alias can be used as a level of indirection (the
// "LastQuartersSales" alias points currently to the "2014-Q3-Sales"
// index, but the administrator might repoint it in the future without
// changing the application) or to scatter-gather or fan-out a query
// across multiple real indexes (e.g., to query across customer
// records, product catalog, call-center records, etc, in one shot).
type AliasSchema struct {
	Targets map[string]*AliasSchemaTarget `json:"targets"` // Keyed by indexName.
}

type AliasSchemaTarget struct {
	IndexUUID string `json:"indexUUID"` // Optional.
}

func CountAlias(mgr *Manager, indexName, indexUUID string) (uint64, error) {
	alias, err := bleveIndexAliasForUserIndexAlias(mgr, indexName, indexUUID)
	if err != nil {
		return 0, fmt.Errorf("CountAlias indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	return alias.DocCount()
}

func QueryAlias(mgr *Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	alias, err := bleveIndexAliasForUserIndexAlias(mgr, indexName, indexUUID)
	if err != nil {
		return fmt.Errorf("QueryAlias indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	var searchRequest bleve.SearchRequest
	err = json.Unmarshal(req, &searchRequest)
	if err != nil {
		return fmt.Errorf("QueryBlevePIndexImpl parsing req, err: %v", err)
	}

	err = searchRequest.Query.Validate()
	if err != nil {
		return err
	}

	searchResponse, err := alias.Search(&searchRequest)
	if err != nil {
		return err
	}

	mustEncode(res, searchResponse)

	return nil
}

// The indexName/indexUUID is for a user-defined index alias.
//
// TODO: One day support user-defined aliases for non-bleve indexes.
func bleveIndexAliasForUserIndexAlias(mgr *Manager,
	indexName, indexUUID string) (bleve.IndexAlias, error) {
	alias := bleve.NewIndexAlias()

	indexDefs, _, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return nil, fmt.Errorf("could not get indexDefs, indexName: %s, err: %v",
			indexName, err)
	}
	indexDef := indexDefs.IndexDefs[indexName]
	if indexDef == nil {
		return nil, fmt.Errorf("could not get indexDef, indexName: %s", indexName)
	}

	schema := AliasSchema{}
	err = json.Unmarshal([]byte(indexDef.Schema), &schema)
	if err != nil {
		return nil, fmt.Errorf("could not parse indexDef.Schema: %s, indexName: %s",
			indexDef.Schema, indexName)
	}

	for indexName, source := range schema.Targets {
		subAlias, err := bleveIndexAlias(mgr, indexName, source.IndexUUID)
		if err != nil {
			return nil, fmt.Errorf("could not get subAlias, indexName: %s,"+
				" source: %#v, err: %v", indexName, source, err)
		}
		alias.Add(subAlias)
	}

	return alias, nil
}

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

package cbft

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/query"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

var maxAliasTargets = 100

func init() {
	// Register alias with empty instantiation functions,
	// so that "fulltext-alias" will show up in valid index types.
	cbgt.RegisterPIndexImplType("fulltext-alias", &cbgt.PIndexImplType{
		Validate: ValidateAlias,
		Count:    CountAlias,
		Query:    QueryAlias,
		Description: "advanced/fulltext-alias" +
			" - a full text index alias provides" +
			" a naming level of indirection" +
			" to one or more actual, target full text indexes",
		StartSample: &AliasParams{
			Targets: map[string]*AliasParamsTarget{
				"yourIndexName": &AliasParamsTarget{},
			},
		},
	})
}

// AliasParams holds the definition for a user-defined index alias.  A
// user-defined index alias can be used as a level of indirection (the
// "LastQuartersSales" alias points currently to the "2014-Q3-Sales"
// index, but the administrator might repoint it in the future without
// changing the application) or to scatter-gather or fan-out a query
// across multiple real indexes (e.g., to query across customer
// records, product catalog, call-center records, etc, in one shot).
type AliasParams struct {
	Targets map[string]*AliasParamsTarget `json:"targets"` // Keyed by indexName.
}

type AliasParamsTarget struct {
	IndexUUID string `json:"indexUUID"` // Optional.
}

func ValidateAlias(indexType, indexName, indexParams string) error {
	params := AliasParams{}
	err := json.Unmarshal([]byte(indexParams), &params)
	if err == nil {
		if len(params.Targets) == 0 {
			return fmt.Errorf("ValidateAlias: cannot create index alias" +
				" because no index targets were specified")
		}
	}
	return err
}

func CountAlias(mgr *cbgt.Manager,
	indexName, indexUUID string) (uint64, error) {
	alias, err := bleveIndexAliasForUserIndexAlias(mgr,
		indexName, indexUUID, false, nil, nil, false)
	if err != nil {
		return 0, fmt.Errorf("alias: CountAlias indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v",
			indexName, indexUUID, err)
	}

	return alias.DocCount()
}

func QueryAlias(mgr *cbgt.Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	queryCtlParams := cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
		},
	}

	err := json.Unmarshal(req, &queryCtlParams)
	if err != nil {
		return fmt.Errorf("alias: QueryAlias"+
			" parsing queryCtlParams, req: %s, err: %v", req, err)
	}

	searchRequest := &bleve.SearchRequest{}

	err = json.Unmarshal(req, searchRequest)
	if err != nil {
		return fmt.Errorf("alias: QueryAlias"+
			" parsing searchRequest, req: %s, err: %v", req, err)
	}

	if srqv, ok := searchRequest.Query.(query.ValidatableQuery); ok {
		err = srqv.Validate()
		if err != nil {
			return err
		}
	}

	cancelCh := cbgt.TimeoutCancelChan(queryCtlParams.Ctl.Timeout)

	alias, err := bleveIndexAliasForUserIndexAlias(mgr,
		indexName, indexUUID, true,
		queryCtlParams.Ctl.Consistency, cancelCh, true)
	if err != nil {
		return err
	}

	searchResponse, err := alias.Search(searchRequest)
	if err != nil {
		return err
	}

	rest.MustEncode(res, searchResponse)

	return nil
}

func parseAliasParams(aliasDefParams string) (*AliasParams, error) {
	params := AliasParams{}
	err := json.Unmarshal([]byte(aliasDefParams), &params)
	if err != nil {
		return nil, err
	}
	return &params, nil
}

// The indexName/indexUUID is for a user-defined index alias.
//
// TODO: One day support user-defined aliases for non-bleve indexes.
func bleveIndexAliasForUserIndexAlias(mgr *cbgt.Manager,
	indexName, indexUUID string, ensureCanRead bool,
	consistencyParams *cbgt.ConsistencyParams,
	cancelCh <-chan bool, groupByNode bool) (
	bleve.IndexAlias, error) {
	alias := bleve.NewIndexAlias()

	indexDefs, _, err := mgr.GetIndexDefs(false)
	if err != nil {
		return nil, fmt.Errorf("alias: could not get indexDefs,"+
			" indexName: %s, err: %v", indexName, err)
	}

	num := 0

	var fillAlias func(aliasName, aliasUUID string) error

	fillAlias = func(aliasName, aliasUUID string) error {
		aliasDef := indexDefs.IndexDefs[aliasName]
		if aliasDef == nil {
			return fmt.Errorf("alias: could not get aliasDef,"+
				" aliasName: %s, indexName: %s",
				aliasName, indexName)
		}
		if aliasDef.Type != "fulltext-alias" {
			return fmt.Errorf("alias: not fulltext-alias type: %s,"+
				" aliasName: %s, indexName: %s",
				aliasDef.Type, aliasName, indexName)
		}
		if aliasUUID != "" &&
			aliasUUID != aliasDef.UUID {
			return fmt.Errorf("alias: mismatched aliasUUID: %s,"+
				" aliasDef.UUID: %s, aliasName: %s, indexName: %s",
				aliasUUID, aliasDef.UUID, aliasName, indexName)
		}

		params, err := parseAliasParams(aliasDef.Params)
		if err != nil {
			return fmt.Errorf("alias: could not parse aliasDef.Params: %s,"+
				" aliasName: %s, indexName: %s",
				aliasDef.Params, aliasName, indexName)
		}

		for targetName, targetSpec := range params.Targets {
			if num > maxAliasTargets {
				return fmt.Errorf("alias: too many alias targets,"+
					" perhaps there's a cycle,"+
					" aliasName: %s, indexName: %s",
					aliasName, indexName)
			}
			targetDef := indexDefs.IndexDefs[targetName]
			if targetDef == nil {
				return fmt.Errorf("alias: the alias depends upon"+
					" a target index that does not exist,"+
					" targetName: %q, aliasName: %q",
					targetName, aliasName)
			}
			if targetSpec.IndexUUID != "" &&
				targetSpec.IndexUUID != targetDef.UUID {
				return fmt.Errorf("alias: mismatched targetSpec.UUID: %s,"+
					" targetDef.UUID: %s, targetName: %s,"+
					" aliasName: %s, indexName: %s",
					targetSpec.IndexUUID, targetDef.UUID, targetName,
					aliasName, indexName)
			}

			// TODO: Convert to registered callbacks instead of if-else-if.
			if targetDef.Type == "fulltext-alias" {
				err = fillAlias(targetName, targetSpec.IndexUUID)
				if err != nil {
					return err
				}
			} else if strings.HasPrefix(targetDef.Type, "fulltext-index") {
				subAlias, _, err := bleveIndexAlias(mgr,
					targetName, targetSpec.IndexUUID, ensureCanRead,
					consistencyParams, cancelCh, true, nil)
				if err != nil {
					return err
				}
				alias.Add(subAlias)
				num += 1
			} else {
				return fmt.Errorf("alias: unsupported target type: %s,"+
					" targetName: %s, aliasName: %s, indexName: %s",
					targetDef.Type, targetName, aliasName, indexName)
			}
		}

		return nil
	}

	err = fillAlias(indexName, indexUUID)
	if err != nil {
		return nil, err
	}

	return alias, nil
}

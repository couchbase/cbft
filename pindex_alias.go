//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

var maxAliasTargets = 100

func init() {
	// Register alias with empty instantiation functions,
	// so that "fulltext-alias" will show up in valid index types.
	cbgt.RegisterPIndexImplType("fulltext-alias", &cbgt.PIndexImplType{
		Prepare:  PrepareAlias,
		Validate: ValidateAlias,

		Count: CountAlias,
		Query: QueryAlias,

		Description: "advanced/fulltext-alias" +
			" - a full text index alias provides" +
			" a naming level of indirection" +
			" to one or more actual, target full text indexes",
		StartSample: &AliasParams{
			Targets: map[string]*AliasParamsTarget{
				"yourIndexName": {},
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

func PrepareAlias(mgr *cbgt.Manager, indexDef *cbgt.IndexDef) (*cbgt.IndexDef, error) {
	// Reset plan params for a full-text alias
	indexDef.PlanParams = cbgt.PlanParams{}

	alias := &struct {
		Targets map[string]interface{} `json:"targets"`
	}{}

	err := json.Unmarshal([]byte(indexDef.Params), alias)
	if err != nil {
		return indexDef, err
	}

	// apply alias name decorations if it is for a single index.
	if len(alias.Targets) == 1 {
		var sourceIndexName string
		for sourceIndexName = range alias.Targets {
		}

		// obtain the original alias name without the keyspace and
		// it might be keyspace prefixed during the alias updates.
		undecoratedAliasName := indexDef.Name
		pos := strings.LastIndex(undecoratedAliasName, ".")
		if pos > 0 && pos+1 < len(undecoratedAliasName) {
			undecoratedAliasName = undecoratedAliasName[pos+1:]
		}

		// obtain the keyspace from the sourceIndexName and update
		// the alias with with the sourceIndex's keyspace prefix.
		pos = strings.LastIndex(sourceIndexName, ".")
		if pos > 1 {
			indexDef.Name = sourceIndexName[:pos] + "." + undecoratedAliasName
		}
	}

	return indexDef, nil
}

func ValidateAlias(indexType, indexName, indexParams string) error {
	params := AliasParams{}
	err := UnmarshalJSON([]byte(indexParams), &params)
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
	alias, _, err := bleveIndexAliasForUserIndexAlias(mgr,
		indexName, indexUUID, false, nil, nil, false, "")
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

	err := UnmarshalJSON(req, &queryCtlParams)
	if err != nil {
		return fmt.Errorf("alias: QueryAlias"+
			" parsing queryCtlParams, err: %v", err)
	}

	var sr *SearchRequest
	err = UnmarshalJSON(req, &sr)
	if err != nil {
		return fmt.Errorf("alias: QueryAlias"+
			" parsing searchRequest, err: %v", err)
	}
	searchRequest, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		return fmt.Errorf("alias: QueryAlias"+
			" parsing searchRequest, err: %v", err)
	}

	ctx, cancel, cancelCh := setupContextAndCancelCh(queryCtlParams, nil)
	// defer a call to cancel, this ensures that goroutine from
	// setupContextAndCancelCh always exits
	defer cancel()

	alias, multiCollIndex, err := bleveIndexAliasForUserIndexAlias(mgr,
		indexName, indexUUID, true,
		queryCtlParams.Ctl.Consistency, cancelCh, true,
		queryCtlParams.Ctl.PartitionSelection)
	if err != nil {
		return err
	}

	searchResponse, err := alias.SearchInContext(ctx, searchRequest)
	if err != nil {
		return err
	}
	// additional processing with multi-collection indexes.
	if multiCollIndex {
		processAliasResponse(searchResponse)
	}

	rest.MustEncode(res, searchResponse)

	return nil
}

func processAliasResponse(searchResponse *bleve.SearchResult) {
	if searchResponse != nil {
		if len(searchResponse.Hits) > 0 {
			for _, hit := range searchResponse.Hits {
				// extract indexName for the hit's Index field, which holds the pindex name;
				// a pindex name has the format ..
				//     default_2d9ca20632cb632e_4c1c5584
				// where "default" is the index name
				var indexName string
				if x := strings.LastIndex(hit.Index, "_"); x > 0 && x < len(hit.Index) {
					temp := hit.Index[:x]
					if x = strings.LastIndex(temp, "_"); x > 0 && x < len(hit.Index) {
						indexName = temp[:x]
					}
				}

				// if this is a multi collection index, then strip the collection UID
				// from the hit ID and fill the details of source collection
				if sdm, multiCollIndex :=
					metaFieldValCache.getSourceDetailsMap(indexName); multiCollIndex {
					idBytes := []byte(hit.ID)
					cuid := binary.LittleEndian.Uint32(idBytes[:4])
					if collName, ok := sdm.collUIDNameMap[cuid]; ok {
						hit.ID = string(idBytes[4:])
						if hit.Fields == nil {
							hit.Fields = make(map[string]interface{})
						}
						hit.Fields["_$c"] = collName
					}
				}
			}
		}
	}
}

func parseAliasParams(aliasDefParams string) (*AliasParams, error) {
	params := AliasParams{}
	err := UnmarshalJSON([]byte(aliasDefParams), &params)
	if err != nil {
		return nil, err
	}
	return &params, nil
}

func getRemoteClients(mgr *cbgt.Manager) addRemoteClients {
	var addRemClients addRemoteClients
	if versionTracker != nil &&
		versionTracker.clusterCompatibleForVersion(FeatureGrpcVersion) {
		addRemClients = addGrpcClients
	}

	if _, ok := mgr.Options()["SkipScatterGatherOverGrpc"]; ok ||
		addRemClients == nil {
		addRemClients = addIndexClients
	}

	return addRemClients
}

// The indexName/indexUUID is for a user-defined index alias.
//
// TODO: One day support user-defined aliases for non-bleve indexes.
func bleveIndexAliasForUserIndexAlias(mgr *cbgt.Manager,
	indexName, indexUUID string, ensureCanRead bool,
	consistencyParams *cbgt.ConsistencyParams,
	cancelCh <-chan bool, groupByNode bool,
	partitionSelection string) (
	bleve.IndexAlias, bool, error) {
	alias := bleve.NewIndexAlias()

	indexDefs, _, err := mgr.GetIndexDefs(false)
	if err != nil {
		return nil, false, fmt.Errorf("alias: could not get indexDefs,"+
			" indexName: %s, err: %v", indexName, err)
	}

	num := 0
	var multiCollIndex bool
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

		var params *AliasParams
		params, err = parseAliasParams(aliasDef.Params)
		if err != nil {
			return fmt.Errorf("alias: could not parse aliasDef.Params: %s,"+
				" aliasName: %s, indexName: %s",
				aliasDef.Params, aliasName, indexName)
		}

		for targetName, targetSpec := range params.Targets {
			if !multiCollIndex {
				_, multiCollIndex = metaFieldValCache.getSourceDetailsMap(targetName)
			}

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
				var subAlias bleve.IndexAlias
				subAlias, _, _, err = bleveIndexAlias(mgr,
					targetName, targetSpec.IndexUUID, ensureCanRead,
					consistencyParams, cancelCh, true, nil,
					partitionSelection, getRemoteClients(mgr))
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
		return nil, false, err
	}

	return alias, multiCollIndex, nil
}

//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"io"
	"strings"
	"sync/atomic"

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

func PrepareAlias(indexDef *cbgt.IndexDef) (*cbgt.IndexDef, error) {
	// Reset plan params for a full-text alias
	indexDef.PlanParams = cbgt.PlanParams{}
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
	alias, err := bleveIndexAliasForUserIndexAlias(mgr,
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

	alias, err := bleveIndexAliasForUserIndexAlias(mgr,
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

	rest.MustEncode(res, searchResponse)

	return nil
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
	if atomic.LoadInt32(&compatibleClusterFound) == 1 {
		addRemClients = addGrpcClients
	} else {
		// switch to grpc for scatter gather in an advanced enough cluster
		if ok, _ := cbgt.VerifyEffectiveClusterVersion(mgr.Cfg(), "6.5.0"); ok {
			addRemClients = addGrpcClients
			atomic.StoreInt32(&compatibleClusterFound, 1)
		}
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

		var params *AliasParams
		params, err = parseAliasParams(aliasDef.Params)
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
		return nil, err
	}

	return alias, nil
}

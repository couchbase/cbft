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
	"fmt"
	"regexp"
	"sync/atomic"
)

// INDEX_NAME_REGEXP is used to validate index names.
const INDEX_NAME_REGEXP = `^[A-Za-z][0-9A-Za-z_\-]*$`

// Creates a logical index, which might be comprised of many PIndex objects.
// A non-"" prevIndexUUID means an update to an existing index.
func (mgr *Manager) CreateIndex(sourceType, sourceName, sourceUUID, sourceParams,
	indexType, indexName, indexParams string, planParams PlanParams,
	prevIndexUUID string) error {
	atomic.AddUint64(&mgr.stats.TotCreateIndex, 1)

	matched, err := regexp.Match(INDEX_NAME_REGEXP, []byte(indexName))
	if err != nil {
		return fmt.Errorf("manager_api: CreateIndex, indexName parsing problem,"+
			" indexName: %s, err: %v", indexName, err)
	}
	if !matched {
		return fmt.Errorf("manager_api: CreateIndex, indexName is invalid,"+
			" indexName: %q", indexName)
	}

	pindexImplType, exists := PIndexImplTypes[indexType]
	if !exists {
		return fmt.Errorf("manager_api: CreateIndex, unknown indexType: %s",
			indexType)
	}
	if pindexImplType.Validate != nil {
		err := pindexImplType.Validate(indexType, indexName, indexParams)
		if err != nil {
			return fmt.Errorf("manager_api: CreateIndex, invalid, err: %v", err)
		}
	}

	// First, check that the source exists.
	_, err = DataSourcePartitions(sourceType, sourceName, sourceUUID,
		sourceParams, mgr.server)
	if err != nil {
		return fmt.Errorf("manager_api: failed to connect to"+
			" or retrieve information from source,"+
			" sourceType: %s, sourceName: %s, sourceUUID: %s, err: %v",
			sourceType, sourceName, sourceUUID, err)
	}

	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return fmt.Errorf("manager_api: CfgGetIndexDefs err: %v", err)
	}
	if indexDefs == nil {
		indexDefs = NewIndexDefs(mgr.version)
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return fmt.Errorf("manager_api: could not create index,"+
			" indexDefs.ImplVersion: %s > mgr.version: %s",
			indexDefs.ImplVersion, mgr.version)
	}

	prevIndex, exists := indexDefs.IndexDefs[indexName]
	if prevIndexUUID == "" { // New index creation.
		if exists || prevIndex != nil {
			return fmt.Errorf("manager_api: index exists, indexName: %s",
				indexName)
		}
	} else { // Update index definition.
		if !exists || prevIndex == nil {
			return fmt.Errorf("manager_api: index missing for update,"+
				" indexName: %s", indexName)
		}
		if prevIndex.UUID != prevIndexUUID {
			return fmt.Errorf("manager_api:"+
				" perhaps there was a concurrent index definition update"+
				" - mismatched index UUID,"+
				" indexName: %s, prevIndex.UUID: %s, prevIndexUUID: %s",
				indexName, prevIndex.UUID, prevIndexUUID)
		}
	}

	indexUUID := NewUUID()

	indexDef := &IndexDef{
		Type:         indexType,
		Name:         indexName,
		UUID:         indexUUID,
		Params:       indexParams,
		SourceType:   sourceType,
		SourceName:   sourceName,
		SourceUUID:   sourceUUID,
		SourceParams: sourceParams,
		PlanParams:   planParams,
	}

	indexDefs.UUID = indexUUID
	indexDefs.IndexDefs[indexName] = indexDef
	indexDefs.ImplVersion = mgr.version

	// NOTE: If our ImplVersion is still too old due to a race, we
	// expect a more modern planner to catch it later.

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	if err != nil {
		return fmt.Errorf("manager_api: could not save indexDefs, err: %v", err)
	}

	mgr.PlannerKick("api/CreateIndex, indexName: " + indexName)
	atomic.AddUint64(&mgr.stats.TotCreateIndexOk, 1)
	return nil
}

// Deletes a logical index definition.
//
// TODO: DeleteIndex should check an optional index UUID?
func (mgr *Manager) DeleteIndex(indexName string) error {
	atomic.AddUint64(&mgr.stats.TotDeleteIndex, 1)

	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return err
	}
	if indexDefs == nil {
		return fmt.Errorf("manager_api: no indexes on deletion of indexName: %s",
			indexName)
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return fmt.Errorf("manager_api: could not delete index,"+
			" indexDefs.ImplVersion: %s > mgr.version: %s",
			indexDefs.ImplVersion, mgr.version)
	}
	if _, exists := indexDefs.IndexDefs[indexName]; !exists {
		return fmt.Errorf("manager_api: index to delete missing, indexName: %s",
			indexName)
	}

	indexDefs.UUID = NewUUID()
	delete(indexDefs.IndexDefs, indexName)
	indexDefs.ImplVersion = mgr.version

	// NOTE: if our ImplVersion is still too old due to a race, we
	// expect a more modern planner to catch it later.

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	if err != nil {
		return fmt.Errorf("manager_api: could not save indexDefs, err: %v", err)
	}

	mgr.PlannerKick("api/DeleteIndex, indexName: " + indexName)
	atomic.AddUint64(&mgr.stats.TotDeleteIndexOk, 1)
	return nil
}

// IndexControl is used to change runtime properties of an index.
func (mgr *Manager) IndexControl(indexName, indexUUID, readOp, writeOp,
	planFreezeOp string) error {
	atomic.AddUint64(&mgr.stats.TotIndexControl, 1)

	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return err
	}
	if indexDefs == nil {
		return fmt.Errorf("manager_api: no indexes,"+
			" index read/write control, indexName: %s", indexName)
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return fmt.Errorf("manager_api: index read/write control,"+
			" indexName: %s, indexDefs.ImplVersion: %s > mgr.version: %s",
			indexName, indexDefs.ImplVersion, mgr.version)
	}
	indexDef, exists := indexDefs.IndexDefs[indexName]
	if !exists || indexDef == nil {
		return fmt.Errorf("manager_api: no index to read/write control,"+
			" indexName: %s", indexName)
	}
	if indexUUID != "" && indexDef.UUID != indexUUID {
		return fmt.Errorf("manager_api: index.UUID mismatched")
	}

	if indexDef.PlanParams.NodePlanParams == nil {
		indexDef.PlanParams.NodePlanParams =
			map[string]map[string]*NodePlanParam{}
	}
	if indexDef.PlanParams.NodePlanParams[""] == nil {
		indexDef.PlanParams.NodePlanParams[""] =
			map[string]*NodePlanParam{}
	}
	if indexDef.PlanParams.NodePlanParams[""][""] == nil {
		indexDef.PlanParams.NodePlanParams[""][""] = &NodePlanParam{
			CanRead:  true,
			CanWrite: true,
		}
	}

	npp := indexDef.PlanParams.NodePlanParams[""][""]
	if readOp != "" {
		if readOp == "allow" || readOp == "resume" {
			npp.CanRead = true
		} else {
			npp.CanRead = false
		}
	}
	if writeOp != "" {
		if writeOp == "allow" || writeOp == "resume" {
			npp.CanWrite = true
		} else {
			npp.CanWrite = false
		}
	}

	if npp.CanRead == true && npp.CanWrite == true {
		delete(indexDef.PlanParams.NodePlanParams[""], "")
	}

	if planFreezeOp != "" {
		indexDef.PlanParams.PlanFrozen = planFreezeOp == "freeze"
	}

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	if err != nil {
		return fmt.Errorf("manager_api: could not save indexDefs, err: %v", err)
	}

	atomic.AddUint64(&mgr.stats.TotIndexControlOk, 1)
	return nil
}

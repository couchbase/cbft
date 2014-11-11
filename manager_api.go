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
	"fmt"
)

// Creates a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) CreateIndex(sourceType, sourceName, sourceUUID, sourceParams,
	indexType, indexName, indexSchema string,
	planParams PlanParams) error {
	// TODO: what about auth info to be able to access bucket?
	// TODO: what if user changes pswd to bucket, but it's the same bucket & uuid?
	// TODO: what about hints for # of partitions, etc?

	// First, check that the source exists.
	_, err := DataSourcePartitions(sourceType, sourceName, sourceUUID, sourceParams,
		mgr.server)
	if err != nil {
		return fmt.Errorf("failed to connect to or retrieve information from source,"+
			" sourceType: %s, sourceName: %s, sourceUUID: %s",
			sourceType, sourceName, sourceUUID)
	}

	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return fmt.Errorf("error: CfgGetIndexDefs err: %v", err)
	}
	if indexDefs == nil {
		indexDefs = NewIndexDefs(mgr.version)
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return fmt.Errorf("error: could not create index, indexDefs.ImplVersion: %s"+
			" > mgr.version: %s", indexDefs.ImplVersion, mgr.version)
	}
	if _, exists := indexDefs.IndexDefs[indexName]; exists {
		return fmt.Errorf("error: index exists, indexName: %s", indexName)
	}

	indexUUID := NewUUID()

	indexDef := &IndexDef{
		Type:         indexType,
		Name:         indexName,
		UUID:         indexUUID,
		Schema:       indexSchema,
		SourceType:   sourceType,
		SourceName:   sourceName,
		SourceUUID:   sourceUUID,
		SourceParams: sourceParams,
		PlanParams:   planParams,
	}

	indexDefs.UUID = indexUUID
	indexDefs.IndexDefs[indexName] = indexDef
	indexDefs.ImplVersion = mgr.version

	// NOTE: if our ImplVersion is still too old due to a race, we
	// expect a more modern planner to catch it later.

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	if err != nil {
		return fmt.Errorf("error: could not save indexDefs, err: %v", err)
	}

	mgr.PlannerKick("api/CreateIndex, indexName: " + indexName)

	return nil
}

// Deletes a logical index, which might be comprised of many PIndex objects.
func (mgr *Manager) DeleteIndex(indexName string) error {
	indexDefs, cas, err := CfgGetIndexDefs(mgr.cfg)
	if err != nil {
		return err
	}
	if indexDefs == nil {
		return fmt.Errorf("error: indexes do not exist during deletion of indexName: %s",
			indexName)
	}
	if VersionGTE(mgr.version, indexDefs.ImplVersion) == false {
		return fmt.Errorf("error: could not delete index, indexDefs.ImplVersion: %s"+
			" > mgr.version: %s", indexDefs.ImplVersion, mgr.version)
	}
	if _, exists := indexDefs.IndexDefs[indexName]; !exists {
		return fmt.Errorf("error: index to delete does not exist, indexName: %s",
			indexName)
	}

	indexDefs.UUID = NewUUID()
	delete(indexDefs.IndexDefs, indexName)
	indexDefs.ImplVersion = mgr.version

	// NOTE: if our ImplVersion is still too old due to a race, we
	// expect a more modern planner to catch it later.

	_, err = CfgSetIndexDefs(mgr.cfg, indexDefs, cas)
	if err != nil {
		return fmt.Errorf("error: could not save indexDefs, err: %v", err)
	}

	mgr.PlannerKick("api/DeleteIndex, indexName: " + indexName)

	return nil
}

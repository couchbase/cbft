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
	"io"

	"github.com/rcrowley/go-metrics"
)

var entryKeyPrefix = []byte("{\"key\":")
var entryKeyPrefixSep = append([]byte("\n,"), entryKeyPrefix...)
var entryValPrefix = []byte(", \"val\":")

type PIndexImpl interface{}

type PIndexImplType struct {
	Validate func(indexType, indexName, indexParams string) error

	New func(indexType, indexParams, path string, restart func()) (
		PIndexImpl, Dest, error)

	Open func(indexType, path string, restart func()) (
		PIndexImpl, Dest, error)

	Count func(mgr *Manager, indexName, indexUUID string) (
		uint64, error)

	Query func(mgr *Manager, indexName, indexUUID string,
		req []byte, res io.Writer) error

	Description string
	StartSample interface{}
}

var pindexImplTypes = make(map[string]*PIndexImplType) // Keyed by indexType.

func RegisterPIndexImplType(indexType string, t *PIndexImplType) {
	pindexImplTypes[indexType] = t
}

func NewPIndexImpl(indexType, indexParams, path string, restart func()) (
	PIndexImpl, Dest, error) {
	t, exists := pindexImplTypes[indexType]
	if !exists || t == nil {
		return nil, nil, fmt.Errorf("pindex_impl: NewPIndexImpl indexType: %s",
			indexType)
	}

	return t.New(indexType, indexParams, path, restart)
}

func OpenPIndexImpl(indexType, path string, restart func()) (
	PIndexImpl, Dest, error) {
	t, exists := pindexImplTypes[indexType]
	if !exists || t == nil {
		return nil, nil, fmt.Errorf("pindex_impl: OpenPIndexImpl"+
			" indexType: %s", indexType)
	}

	return t.Open(indexType, path, restart)
}

func PIndexImplTypeForIndex(cfg Cfg, indexName string) (
	*PIndexImplType, error) {
	indexDefs, _, err := CfgGetIndexDefs(cfg)
	if err != nil || indexDefs == nil {
		return nil, fmt.Errorf("pindex_impl: could not get indexDefs,"+
			" indexName: %s, err: %v",
			indexName, err)
	}
	indexDef := indexDefs.IndexDefs[indexName]
	if indexDef == nil {
		return nil, fmt.Errorf("pindex_impl: no indexDef,"+
			" indexName: %s", indexName)
	}
	pindexImplType := pindexImplTypes[indexDef.Type]
	if pindexImplType == nil {
		return nil, fmt.Errorf("pindex_impl: no pindexImplType,"+
			" indexName: %s, indexDef.Type: %s",
			indexName, indexDef.Type)
	}
	return pindexImplType, nil
}

// ------------------------------------------------

type PIndexStoreStats struct {
	TimerBatchStore metrics.Timer
}

func (d *PIndexStoreStats) WriteJSON(w io.Writer) {
	w.Write([]byte(`{"TimerBatchStore":`))
	WriteTimerJSON(w, d.TimerBatchStore)
	w.Write(jsonCloseBrace)
}

var prefixPIndexStoreStats = []byte(`{"pindexStoreStats":`)

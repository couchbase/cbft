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
	"io"
)

type PIndexImpl interface {
	Close() error
}

type ConsistencyParams struct {
	// A Level value of "" means stale is ok; "at_plus" means we need
	// consistency at least at or beyond the consistency vector but
	// not before.
	Level string `json:"level"`

	// Keyed by indexName.
	Vectors map[string]ConsistencyVector `json:"vectors"`

	// TODO: Can user specify certain partition UUID (like vbucket UUID)?
}

// Key is partition, value is seq.
type ConsistencyVector map[string]uint64

// ---------------------------------------------------------------

type PIndexImplType struct {
	Validate func(indexType, indexName, indexParams string) error

	New  func(indexType, indexParams, path string, restart func()) (PIndexImpl, Dest, error)
	Open func(indexType, path string, restart func()) (PIndexImpl, Dest, error)

	Count func(mgr *Manager, indexName, indexUUID string) (uint64, error)
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
		return nil, nil, fmt.Errorf("error: NewPIndexImpl indexType: %s", indexType)
	}

	return t.New(indexType, indexParams, path, restart)
}

func OpenPIndexImpl(indexType, path string, restart func()) (PIndexImpl, Dest, error) {
	t, exists := pindexImplTypes[indexType]
	if !exists || t == nil {
		return nil, nil, fmt.Errorf("error: OpenPIndexImpl indexType: %s", indexType)
	}

	return t.Open(indexType, path, restart)
}

func PIndexImplTypeForIndex(cfg Cfg, indexName string) (*PIndexImplType, error) {
	indexDefs, _, err := CfgGetIndexDefs(cfg)
	if err != nil || indexDefs == nil {
		return nil, fmt.Errorf("could not get indexDefs, indexName: %s, err: %v",
			indexName, err)
	}
	indexDef := indexDefs.IndexDefs[indexName]
	if indexDef == nil {
		return nil, fmt.Errorf("no indexDef, indexName: %s", indexName)
	}
	pindexImplType := pindexImplTypes[indexDef.Type]
	if pindexImplType == nil {
		return nil, fmt.Errorf("no pindexImplType, indexName: %s, indexDef.Type: %s",
			indexName, indexDef.Type)
	}
	return pindexImplType, nil
}

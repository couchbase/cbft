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

type PIndexImplType struct {
	New  func(indexType, indexSchema, path string, restart func()) (PIndexImpl, Dest, error)
	Open func(indexType, path string, restart func()) (PIndexImpl, Dest, error)
}

var pindexImplTypes = make(map[string]*PIndexImplType) // Keyed by indexType.

func RegisterPIndexImplType(indexType string, t *PIndexImplType) {
	pindexImplTypes[indexType] = t
}

func NewPIndexImpl(indexType, indexSchema, path string, restart func()) (
	PIndexImpl, Dest, error) {
	t, exists := pindexImplTypes[indexType]
	if !exists || t == nil {
		return nil, nil, fmt.Errorf("error: NewPIndexImpl indexType: %s", indexType)
	}

	return t.New(indexType, indexSchema, path, restart)
}

func OpenPIndexImpl(indexType, path string, restart func()) (PIndexImpl, Dest, error) {
	t, exists := pindexImplTypes[indexType]
	if !exists || t == nil {
		return nil, nil, fmt.Errorf("error: OpenPIndexImpl indexType: %s", indexType)
	}

	return t.Open(indexType, path, restart)
}

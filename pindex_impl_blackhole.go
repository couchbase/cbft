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
	"io/ioutil"
	"os"
)

func init() {
	RegisterPIndexImplType("blackhole", &PIndexImplType{
		New:  NewBlackHolePIndexImpl,
		Open: OpenBlackHolePIndexImpl,

		Count: func(mgr *Manager, indexName, indexUUID string) (uint64, error) {
			return 0, fmt.Errorf("blackhole is uncountable")
		},
		Search: func(mgr *Manager, indexName, indexUUID string,
			req []byte, res io.Writer) error {
			return fmt.Errorf("blackhole is unsearchable")
		},
	})
}

func NewBlackHolePIndexImpl(indexType, indexSchema, path string, restart func()) (
	PIndexImpl, Dest, error) {
	err := os.MkdirAll(path, 0700)
	if err != nil {
		return nil, nil, err
	}

	err = ioutil.WriteFile(path+string(os.PathSeparator)+"black.hole",
		[]byte{}, 0600)
	if err != nil {
		return nil, nil, err
	}

	dest := &BlackHole{path: path}
	return dest, dest, nil
}

func OpenBlackHolePIndexImpl(indexType, path string, restart func()) (PIndexImpl, Dest, error) {
	buf, err := ioutil.ReadFile(path + string(os.PathSeparator) + "black.hole")
	if err != nil {
		return nil, nil, err
	}
	if len(buf) > 0 {
		return nil, nil, fmt.Errorf("expected black.hole to be empty")
	}

	dest := &BlackHole{path: path}
	return dest, dest, nil
}

// ---------------------------------------------------------

// Implements both Dest and PIndexImpl interfaces.
type BlackHole struct {
	path string
}

func (t *BlackHole) Close() error {
	return nil
}

func (t *BlackHole) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	return nil
}

func (t *BlackHole) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	return nil
}

func (t *BlackHole) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	return nil
}

func (t *BlackHole) SetOpaque(partition string, value []byte) error {
	return nil
}

func (t *BlackHole) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	return nil, 0, nil
}

func (t *BlackHole) Rollback(partition string, rollbackSeq uint64) error {
	return nil
}

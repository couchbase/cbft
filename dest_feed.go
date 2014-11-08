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
	log "github.com/couchbaselabs/clog"
)

// A DestFeed implements both the Feed and Dest interfaces, for
// chainability; and is also useful for testing.
type DestFeed struct {
	name    string
	pf      DestPartitionFunc
	dests   map[string]Dest
	closeCh chan bool
	doneCh  chan bool
	doneErr error
	doneMsg string
}

func NewDestFeed(name string, pf DestPartitionFunc,
	dests map[string]Dest) (*DestFeed, error) {
	return &DestFeed{
		name:    name,
		pf:      pf,
		dests:   dests,
		closeCh: make(chan bool),
		doneCh:  make(chan bool),
		doneErr: nil,
		doneMsg: "",
	}, nil
}

func (t *DestFeed) Name() string {
	return t.name
}

func (t *DestFeed) Start() error {
	log.Printf("DestFeed.Start, name: %s, dests: %#v", t.Name(), t.dests)
	return nil
}

func (t *DestFeed) Close() error {
	return nil
}

func (t *DestFeed) Dests() map[string]Dest {
	return t.dests
}

// -----------------------------------------------------

func (t *DestFeed) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	dest, err := t.pf(partition, key, t.dests)
	if err != nil {
		return fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.OnDataUpdate(partition, key, seq, val)
}

func (t *DestFeed) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	dest, err := t.pf(partition, key, t.dests)
	if err != nil {
		return fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.OnDataDelete(partition, key, seq)
}

func (t *DestFeed) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.OnSnapshotStart(partition, snapStart, snapEnd)
}

func (t *DestFeed) SetOpaque(partition string,
	value []byte) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.SetOpaque(partition, value)
}

func (t *DestFeed) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return nil, 0, fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.GetOpaque(partition)
}

func (t *DestFeed) Rollback(partition string,
	rollbackSeq uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.Rollback(partition, rollbackSeq)
}

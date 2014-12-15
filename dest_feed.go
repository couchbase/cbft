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
	"strconv"
)

func init() {
	RegisterFeedType("dest", &FeedType{
		Start: func(mgr *Manager, feedName, indexName, indexUUID,
			sourceType, sourceName, sourceUUID, params string,
			dests map[string]Dest) error {
			return mgr.registerFeed(NewDestFeed(feedName, BasicPartitionFunc, dests))
		},
		Partitions:  DestFeedPartitions,
		Public:      true,
		Description: "dest - a primary data source",
		StartSample: &DestSourceParams{},
	})
}

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

func NewDestFeed(name string, pf DestPartitionFunc, dests map[string]Dest) *DestFeed {
	return &DestFeed{
		name:    name,
		pf:      pf,
		dests:   dests,
		closeCh: make(chan bool),
		doneCh:  make(chan bool),
		doneErr: nil,
		doneMsg: "",
	}
}

func (t *DestFeed) Name() string {
	return t.name
}

func (t *DestFeed) Start() error {
	return nil
}

func (t *DestFeed) Close() error {
	return nil
}

func (t *DestFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *DestFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}

// -----------------------------------------------------

type DestSourceParams struct {
	NumPartitions int `json:"numPartitions"`
}

func DestFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	server string) ([]string, error) {
	dsp := &DestSourceParams{}
	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), dsp)
		if err != nil {
			return nil, fmt.Errorf("error: DataSourcePartitions/dest"+
				" could not parse sourceParams: %s, err: %v", sourceParams, err)
		}
	}
	numPartitions := dsp.NumPartitions
	rv := make([]string, numPartitions)
	for i := 0; i < numPartitions; i++ {
		rv[i] = strconv.Itoa(i)
	}
	return rv, nil
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

func (t *DestFeed) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh chan string) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("error: DestFeed pf, err: %v", err)
	}
	return dest.ConsistencyWait(partition,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *DestFeed) Count(pindex *PIndex, cancelCh chan string) (
	uint64, error) {
	return 0, fmt.Errorf("DestFeed.Count unimplemented")
}

func (t *DestFeed) Query(pindex *PIndex, req []byte, w io.Writer,
	cancelCh chan string) error {
	return fmt.Errorf("DestFeed.Query unimplemented")
}

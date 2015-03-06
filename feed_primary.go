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
	RegisterFeedType("primary", &FeedType{
		Start: func(mgr *Manager, feedName, indexName, indexUUID,
			sourceType, sourceName, sourceUUID, params string,
			dests map[string]Dest) error {
			return mgr.registerFeed(NewPrimaryFeed(feedName,
				BasicPartitionFunc, dests))
		},
		Partitions:  PrimaryFeedPartitions,
		Public:      true,
		Description: "primary - a primary data source",
		StartSample: &DestSourceParams{},
	})
}

// A PrimaryFeed implements both the Feed and Dest interfaces, for
// chainability; and is also useful for testing.
type PrimaryFeed struct {
	name    string
	pf      DestPartitionFunc
	dests   map[string]Dest
	closeCh chan bool
	doneCh  chan bool
	doneErr error
	doneMsg string
}

func NewPrimaryFeed(name string, pf DestPartitionFunc,
	dests map[string]Dest) *PrimaryFeed {
	return &PrimaryFeed{
		name:    name,
		pf:      pf,
		dests:   dests,
		closeCh: make(chan bool),
		doneCh:  make(chan bool),
		doneErr: nil,
		doneMsg: "",
	}
}

func (t *PrimaryFeed) Name() string {
	return t.name
}

func (t *PrimaryFeed) Start() error {
	return nil
}

func (t *PrimaryFeed) Close() error {
	return nil
}

func (t *PrimaryFeed) Dests() map[string]Dest {
	return t.dests
}

func (t *PrimaryFeed) Stats(w io.Writer) error {
	_, err := w.Write([]byte("{}"))
	return err
}

// -----------------------------------------------------

type DestSourceParams struct {
	NumPartitions int `json:"numPartitions"`
}

func PrimaryFeedPartitions(sourceType, sourceName, sourceUUID, sourceParams,
	server string) ([]string, error) {
	dsp := &DestSourceParams{}
	if sourceParams != "" {
		err := json.Unmarshal([]byte(sourceParams), dsp)
		if err != nil {
			return nil, fmt.Errorf("feed_primary: DataSourcePartitions/dest"+
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

func (t *PrimaryFeed) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	dest, err := t.pf(partition, key, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.OnDataUpdate(partition, key, seq, val)
}

func (t *PrimaryFeed) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	dest, err := t.pf(partition, key, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.OnDataDelete(partition, key, seq)
}

func (t *PrimaryFeed) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.OnSnapshotStart(partition, snapStart, snapEnd)
}

func (t *PrimaryFeed) SetOpaque(partition string,
	value []byte) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.SetOpaque(partition, value)
}

func (t *PrimaryFeed) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return nil, 0, fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.GetOpaque(partition)
}

func (t *PrimaryFeed) Rollback(partition string,
	rollbackSeq uint64) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.Rollback(partition, rollbackSeq)
}

func (t *PrimaryFeed) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	dest, err := t.pf(partition, nil, t.dests)
	if err != nil {
		return fmt.Errorf("feed_primary: PrimaryFeed pf, err: %v", err)
	}
	return dest.ConsistencyWait(partition, partitionUUID,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *PrimaryFeed) Count(pindex *PIndex, cancelCh <-chan bool) (
	uint64, error) {
	return 0, fmt.Errorf("feed_primary: PrimaryFeed.Count unimplemented")
}

func (t *PrimaryFeed) Query(pindex *PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return fmt.Errorf("feed_primary: PrimaryFeed.Query unimplemented")
}

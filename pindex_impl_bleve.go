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
	"encoding/binary"
	"os"
	"sync"

	"github.com/blevesearch/bleve"

	log "github.com/couchbaselabs/clog"
)

type BleveDest struct {
	path    string
	restart func()

	m      sync.Mutex
	bindex bleve.Index
	seqs   map[string]uint64 // To track max seq #'s we saw per partition.

}

func NewBleveDest(path string, bindex bleve.Index, restart func()) Dest {
	return &BleveDest{
		path:    path,
		restart: restart,
		bindex:  bindex,
		seqs:    map[string]uint64{},
	}
}

// TODO: use batching.

func (t *BleveDest) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	log.Printf("bleve dest update, partition: %s, key: %s, seq: %d",
		partition, key, seq)

	t.m.Lock()
	defer t.m.Unlock()

	err := t.bindex.Index(string(key), val)
	if err != nil {
		return err
	}

	return t.updateSeqUnlocked(partition, seq)
}

func (t *BleveDest) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	log.Printf("bleve dest delete, partition: %s, key: %s, seq: %d",
		partition, key, seq)

	t.m.Lock()
	defer t.m.Unlock()

	err := t.bindex.Delete(string(key))
	if err != nil {
		return err
	}

	return t.updateSeqUnlocked(partition, seq)
}

func (t *BleveDest) updateSeqUnlocked(partition string, seq uint64) error {
	if t.seqs[partition] < seq {
		t.seqs[partition] = seq

		seqBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(seqBuf, seq)
		return t.bindex.SetInternal([]byte(partition), seqBuf)
	}

	return nil
}

func (t *BleveDest) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	log.Printf("bleve dest snapshot-start, partition: %s, snapStart: %d, snapEnd: %d",
		partition, snapStart, snapEnd)

	return nil // TODO: optimize batching on snapshot start.
}

func (t *BleveDest) SetOpaque(partition string,
	value []byte) error {
	log.Printf("bleve dest set-opaque, partition: %s, value: %s",
		partition, value)

	t.m.Lock()
	defer t.m.Unlock()

	return t.bindex.SetInternal([]byte("o:"+partition), value)
}

func (t *BleveDest) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	log.Printf("bleve dest get-opaque, partition: %s", partition)

	t.m.Lock()
	defer t.m.Unlock()

	value, err = t.bindex.GetInternal([]byte("o:" + partition))
	if err != nil || value == nil {
		return nil, 0, err
	}

	buf, err := t.bindex.GetInternal([]byte(partition))
	if err != nil || buf == nil {
		return value, 0, err
	}
	if len(buf) < 8 {
		return nil, 0, err
	}

	return value, binary.BigEndian.Uint64(buf[0:8]), nil
}

func (t *BleveDest) Rollback(partition string, rollbackSeq uint64) error {
	log.Printf("bleve dest get-opaque, partition: %s, rollbackSeq: %d",
		partition, rollbackSeq)

	t.m.Lock()
	defer t.m.Unlock()

	// TODO: Implement partial rollback one day.  Implementation
	// sketch: we expect bleve to one day to provide an additional
	// Snapshot() and Rollback() API, where Snapshot() returns some
	// opaque and persistable snapshot ID ("SID"), which cbft can
	// occasionally record into the bleve's Get/SetInternal() "side"
	// storage.  A stream rollback operation then needs to loop
	// through appropriate candidate SID's until a Rollback(SID)
	// succeeds.  Else, we eventually devolve down to
	// restarting/rebuilding everything from scratch or zero.
	//
	// For now, always rollback to zero, in which we close the
	// pindex and have the janitor rebuild from scratch.
	t.seqs = map[string]uint64{}

	t.bindex.Close()

	os.RemoveAll(t.path)

	t.restart()

	return nil
}

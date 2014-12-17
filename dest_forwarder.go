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
	"io"
)

// A DestForwarder forwards method calls on it to the Dest returned by
// the DestProvider.
type DestForwarder struct {
	DestProvider DestProvider
}

type DestProvider interface {
	Dest(partition string) (Dest, error)

	Count(pindex *PIndex, cancelCh chan string) (uint64, error)

	Query(pindex *PIndex, req []byte, res io.Writer,
		cancelCh chan string) error

	Stats(io.Writer) error

	Close() error
}

func (t *DestForwarder) Close() error {
	return t.DestProvider.Close()
}

func (t *DestForwarder) OnDataUpdate(partition string,
	key []byte, seq uint64, val []byte) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.OnDataUpdate(partition, key, seq, val)
}

func (t *DestForwarder) OnDataDelete(partition string,
	key []byte, seq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.OnDataDelete(partition, key, seq)
}

func (t *DestForwarder) OnSnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.OnSnapshotStart(partition, snapStart, snapEnd)
}

func (t *DestForwarder) SetOpaque(partition string, value []byte) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.SetOpaque(partition, value)
}

func (t *DestForwarder) GetOpaque(partition string) (
	value []byte, lastSeq uint64, err error) {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return nil, 0, err
	}

	return dest.GetOpaque(partition)
}

func (t *DestForwarder) Rollback(partition string, rollbackSeq uint64) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.Rollback(partition, rollbackSeq)
}

func (t *DestForwarder) ConsistencyWait(partition string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh chan string) error {
	dest, err := t.DestProvider.Dest(partition)
	if err != nil {
		return err
	}

	return dest.ConsistencyWait(partition,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *DestForwarder) Count(pindex *PIndex, cancelCh chan string) (
	uint64, error) {
	return t.DestProvider.Count(pindex, cancelCh)
}

func (t *DestForwarder) Query(pindex *PIndex, req []byte, res io.Writer,
	cancelCh chan string) error {
	return t.DestProvider.Query(pindex, req, res, cancelCh)
}

func (t *DestForwarder) Stats(w io.Writer) error {
	return t.DestProvider.Stats(w)
}

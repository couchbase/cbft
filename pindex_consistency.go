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
	"container/heap"
	"fmt"
	"sync"
)

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

type ConsistencyWaiter interface {
	ConsistencyWait(partition string,
		consistencyLevel string,
		consistencySeq uint64,
		cancelCh <-chan bool) error
}

type ConsistencyWaitReq struct {
	ConsistencyLevel string
	ConsistencySeq   uint64
	CancelCh         <-chan bool
	DoneCh           chan error
}

type ErrorConsistencyWait struct {
	Err    error  // The underlying, wrapped error.
	Status string // Short status reason, like "timeout", "cancelled", etc.

	// Keyed by partitionId, value is pair of start/end seq's.
	StartEndSeqs map[string][]uint64
}

func (e *ErrorConsistencyWait) Error() string {
	return fmt.Sprintf("ErrorConsistencyWait, startEndSeqs: %#v,"+
		" err: %v", e.StartEndSeqs, e.Err)
}

// ---------------------------------------------------------

func ConsistencyWaitDone(partition string, cancelCh <-chan bool,
	doneCh chan error, currSeq func() uint64) error {
	seqStart := currSeq()

	select {
	case <-cancelCh:
		rv := map[string][]uint64{}
		rv[partition] = []uint64{seqStart, currSeq()}

		err := fmt.Errorf("ConsistencyWaitDone cancelled")

		return &ErrorConsistencyWait{ // TODO: track stats.
			Err:          err,
			Status:       "cancelled",
			StartEndSeqs: rv,
		}

	case err := <-doneCh:
		return err // TODO: track stats.
	}
}

func ConsistencyWaitPartitions(
	t ConsistencyWaiter,
	partitions []string,
	consistencyLevel string,
	consistencyVector map[string]uint64,
	cancelCh <-chan bool) error {
	for _, partition := range partitions {
		consistencySeq := consistencyVector[partition]
		if consistencySeq > 0 {
			err := t.ConsistencyWait(partition,
				consistencyLevel, consistencySeq, cancelCh)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ConsistencyWaitPIndex(pindex *PIndex, t ConsistencyWaiter,
	consistencyParams *ConsistencyParams, cancelCh <-chan bool) error {
	if consistencyParams != nil &&
		consistencyParams.Level != "" &&
		consistencyParams.Vectors != nil {
		consistencyVector := consistencyParams.Vectors[pindex.IndexName]
		if consistencyVector != nil {
			err := ConsistencyWaitPartitions(t, pindex.sourcePartitionsArr,
				consistencyParams.Level, consistencyVector, cancelCh)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ConsistencyWaitGroup(indexName string,
	consistencyParams *ConsistencyParams, cancelCh <-chan bool,
	localPIndexes []*PIndex,
	addLocalPIndex func(*PIndex) error) error {
	var errConsistencyM sync.Mutex
	var errConsistency error

	var wg sync.WaitGroup

	for _, localPIndex := range localPIndexes {
		err := addLocalPIndex(localPIndex)
		if err != nil {
			return err
		}

		if consistencyParams != nil &&
			consistencyParams.Level != "" &&
			consistencyParams.Vectors != nil {
			consistencyVector := consistencyParams.Vectors[indexName]
			if consistencyVector != nil {
				wg.Add(1)
				go func(localPIndex *PIndex,
					consistencyVector map[string]uint64) {
					defer wg.Done()

					err := ConsistencyWaitPartitions(localPIndex.Dest,
						localPIndex.sourcePartitionsArr,
						consistencyParams.Level,
						consistencyVector,
						cancelCh)
					if err != nil {
						errConsistencyM.Lock()
						errConsistency = err
						errConsistencyM.Unlock()
					}
				}(localPIndex, consistencyVector)
			}
		}
	}

	wg.Wait()

	if errConsistency != nil {
		return errConsistency
	}

	if cancelCh != nil {
		select {
		case <-cancelCh:
			return fmt.Errorf("cancelled")
		default:
		}
	}

	// TODO: There's likely a race here where at this point we've now
	// waited for all the (local) pindexes to reach the requested
	// consistency levels, but before we actually can use the
	// constructed alias and kick off a query, an adversary does a
	// rollback.  Using the alias to query after that might now be
	// incorrectly running against data some time back in the past.

	return nil
}

// ---------------------------------------------------------

// A cwrQueue is a consistency wait request queue, implementing the
// heap.Interface for consistencyWaitReq's.
type cwrQueue []*ConsistencyWaitReq

func (pq cwrQueue) Len() int { return len(pq) }

func (pq cwrQueue) Less(i, j int) bool {
	return pq[i].ConsistencySeq < pq[j].ConsistencySeq
}

func (pq cwrQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *cwrQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*ConsistencyWaitReq))
}

func (pq *cwrQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// ---------------------------------------------------------

func RunConsistencyWaitQueue(
	cwrCh chan *ConsistencyWaitReq,
	m *sync.Mutex,
	cwrQueue *cwrQueue,
	currSeq func() uint64) {
	for cwr := range cwrCh {
		m.Lock()

		if cwr.ConsistencyLevel == "" {
			close(cwr.DoneCh) // We treat "" like stale=ok, so we're done.
		} else if cwr.ConsistencyLevel == "at_plus" {
			if cwr.ConsistencySeq > currSeq() {
				heap.Push(cwrQueue, cwr)
			} else {
				close(cwr.DoneCh)
			}
		} else {
			cwr.DoneCh <- fmt.Errorf("consistency wait unsupported level: %s,"+
				" cwr: %#v", cwr.ConsistencyLevel, cwr)
			close(cwr.DoneCh)
		}

		m.Unlock()
	}

	// If we reach here, then we're closing down so cancel/error any
	// callers waiting for consistency.
	m.Lock()
	defer m.Unlock()

	err := fmt.Errorf("consistency wait closed")

	for _, cwr := range *cwrQueue {
		// TODO: Perhaps extra goroutine here isn't necessary, but the
		// motivation is to keep cwrQueue's lock window short.
		go func(cwr *ConsistencyWaitReq) {
			cwr.DoneCh <- err
			close(cwr.DoneCh)
		}(cwr)
	}
}

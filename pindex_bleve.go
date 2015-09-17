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
	"container/list"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"github.com/rcrowley/go-metrics"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	bleveHttp "github.com/blevesearch/bleve/http"
	bleveHttpMapping "github.com/blevesearch/bleve/http/mapping"
	bleveRegistry "github.com/blevesearch/bleve/registry"

	log "github.com/couchbase/clog"

	"github.com/couchbaselabs/cbgt"
	"github.com/couchbaselabs/cbgt/rest"
)

type BleveParams struct {
	Mapping bleve.IndexMapping     `json:"mapping"`
	Store   map[string]interface{} `json:"store"`
}

func NewBleveParams() *BleveParams {
	return &BleveParams{
		Mapping: *bleve.NewIndexMapping(),
		Store: map[string]interface{}{
			"kvStoreName": bleve.Config.DefaultKVStore,
		},
	}
}

type BleveDest struct {
	path string

	// Invoked when mgr should restart this BleveDest, like on rollback.
	restart func()

	m          sync.Mutex // Protects the fields that follow.
	bindex     bleve.Index
	partitions map[string]*BleveDestPartition

	stats cbgt.PIndexStoreStats
}

// Used to track state for a single partition.
type BleveDestPartition struct {
	bdest           *BleveDest
	bindex          bleve.Index
	partition       string
	partitionOpaque []byte // Key used to implement OpaqueSet/OpaqueGet().

	m           sync.Mutex   // Protects the fields that follow.
	seqMax      uint64       // Max seq # we've seen for this partition.
	seqMaxBuf   []byte       // For binary encoded seqMax uint64.
	seqMaxBatch uint64       // Max seq # that got through batch apply/commit.
	seqSnapEnd  uint64       // To track snapshot end seq # for this partition.
	batch       *bleve.Batch // Batch applied when we hit seqSnapEnd.

	lastOpaque []byte // Cache most recent value for OpaqueSet()/OpaqueGet().
	lastUUID   string // Cache most recent partition UUID from lastOpaque.

	cwrQueue cbgt.CwrQueue
}

func NewBleveDest(path string, bindex bleve.Index,
	restart func()) *BleveDest {
	return &BleveDest{
		path:       path,
		restart:    restart,
		bindex:     bindex,
		partitions: make(map[string]*BleveDestPartition),
		stats: cbgt.PIndexStoreStats{
			TimerBatchStore: metrics.NewTimer(),
			Errors:          list.New(),
		},
	}
}

// ---------------------------------------------------------

const bleveQueryHelp = `<a href="https://github.com/blevesearch/bleve/wiki/Query%20String%20Query">
       bleve query syntax help
     </a>`

func init() {
	cbgt.RegisterPIndexImplType("bleve", &cbgt.PIndexImplType{
		Validate: ValidateBlevePIndexImpl,

		New:   NewBlevePIndexImpl,
		Open:  OpenBlevePIndexImpl,
		Count: CountBlevePIndexImpl,
		Query: QueryBlevePIndexImpl,

		Description: "general/full-text (bleve)" +
			" - a full-text index powered by the bleve engine",
		StartSample:  NewBleveParams(),
		QuerySamples: BlevePIndexQuerySamples,
		QueryHelp:    bleveQueryHelp,
		InitRouter:   BlevePIndexImplInitRouter,
		DiagHandlers: []cbgt.DiagHandler{
			{"/api/pindex-bleve", bleveHttp.NewListIndexesHandler(), nil},
		},
		MetaExtra: BleveMetaExtra,
	})
}

func ValidateBlevePIndexImpl(indexType, indexName, indexParams string) error {
	bleveParams := NewBleveParams()
	if len(indexParams) > 0 {
		return json.Unmarshal([]byte(indexParams), bleveParams)
	}
	return nil
}

func NewBlevePIndexImpl(indexType, indexParams, path string,
	restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	bleveParams := NewBleveParams()
	if len(indexParams) > 0 {
		err := json.Unmarshal([]byte(indexParams), bleveParams)
		if err != nil {
			return nil, nil, fmt.Errorf("bleve: parse params, err: %v", err)
		}
	}

	kvStoreName, ok := bleveParams.Store["kvStoreName"].(string)
	if !ok || kvStoreName == "" {
		kvStoreName = bleve.Config.DefaultKVStore
	}

	kvConfig := map[string]interface{}{
		"create_if_missing": true,
		"error_if_exists":   true,
	}
	for k, v := range bleveParams.Store {
		kvConfig[k] = v
	}

	// Always use the "metrics" wrapper KVStore if it's available and
	// also not already configured.
	_, exists := kvConfig["kvStoreName_actual"]
	if !exists &&
		kvStoreName != "metrics" &&
		bleveRegistry.KVStoreConstructorByName("metrics") != nil {
		kvConfig["kvStoreName_actual"] = kvStoreName
		kvStoreName = "metrics"
	}

	bindex, err := bleve.NewUsing(path, &bleveParams.Mapping,
		bleve.Config.DefaultIndexType,
		kvStoreName, kvConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: new index, path: %s,"+
			" kvStoreName: %s, kvConfig: %#v, err: %s",
			path, kvStoreName, kvConfig, err)
	}

	pathMeta := path + string(os.PathSeparator) + "PINDEX_BLEVE_META"
	err = ioutil.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		return nil, nil, err
	}

	return bindex, &cbgt.DestForwarder{
		DestProvider: NewBleveDest(path, bindex, restart),
	}, nil
}

func OpenBlevePIndexImpl(indexType, path string,
	restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	buf, err := ioutil.ReadFile(path +
		string(os.PathSeparator) + "PINDEX_BLEVE_META")
	if err != nil {
		return nil, nil, err
	}

	bleveParams := NewBleveParams()
	err = json.Unmarshal(buf, bleveParams)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: parse params: %v", err)
	}

	// TODO: boltdb sometimes locks on Open(), so need to investigate,
	// where perhaps there was a previous missing or race-y Close().
	bindex, err := bleve.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return bindex, &cbgt.DestForwarder{
		DestProvider: NewBleveDest(path, bindex, restart),
	}, nil
}

// ---------------------------------------------------------------

func CountBlevePIndexImpl(mgr *cbgt.Manager, indexName, indexUUID string) (
	uint64, error) {
	alias, err := bleveIndexAlias(mgr, indexName, indexUUID, false, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("bleve: CountBlevePIndexImpl indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	return alias.DocCount()
}

func QueryBlevePIndexImpl(mgr *cbgt.Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	queryCtlParams := cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
		},
	}

	err := json.Unmarshal(req, &queryCtlParams)
	if err != nil {
		return fmt.Errorf("bleve: QueryBlevePIndexImpl"+
			" parsing queryCtlParams, req: %s, err: %v", req, err)
	}

	searchRequest := &bleve.SearchRequest{}

	err = json.Unmarshal(req, searchRequest)
	if err != nil {
		return fmt.Errorf("bleve: QueryBlevePIndexImpl"+
			" parsing searchRequest, req: %s, err: %v", req, err)
	}

	err = searchRequest.Query.Validate()
	if err != nil {
		return err
	}

	cancelCh := cbgt.TimeoutCancelChan(queryCtlParams.Ctl.Timeout)

	alias, err := bleveIndexAlias(mgr, indexName, indexUUID, true,
		queryCtlParams.Ctl.Consistency, cancelCh)
	if err != nil {
		return err
	}

	doneCh := make(chan struct{})

	var searchResult *bleve.SearchResult

	go func() {
		searchResult, err = alias.Search(searchRequest)

		close(doneCh)
	}()

	select {
	case <-cancelCh:
		err = fmt.Errorf("pindex_bleve: query timeout")

	case <-doneCh:
		if searchResult != nil {
			rest.MustEncode(res, searchResult)
		}
	}

	return err
}

// ---------------------------------------------------------

func (t *BleveDest) Dest(partition string) (cbgt.Dest, error) {
	t.m.Lock()
	d, err := t.getPartitionUnlocked(partition)
	t.m.Unlock()
	return d, err
}

func (t *BleveDest) getPartitionUnlocked(partition string) (
	*BleveDestPartition, error) {
	if t.bindex == nil {
		return nil, fmt.Errorf("bleve: BleveDest already closed")
	}

	bdp, exists := t.partitions[partition]
	if !exists || bdp == nil {
		bdp = &BleveDestPartition{
			bdest:           t,
			bindex:          t.bindex,
			partition:       partition,
			partitionOpaque: []byte("o:" + partition),
			seqMaxBuf:       make([]byte, 8), // Binary encoded seqMax uint64.
			batch:           t.bindex.NewBatch(),
			cwrQueue:        cbgt.CwrQueue{},
		}
		heap.Init(&bdp.cwrQueue)

		t.partitions[partition] = bdp
	}

	return bdp, nil
}

// ---------------------------------------------------------

func (t *BleveDest) Close() error {
	t.m.Lock()
	err := t.closeUnlocked()
	t.m.Unlock()
	return err
}

func (t *BleveDest) closeUnlocked() error {
	if t.bindex == nil {
		return nil // Already closed.
	}

	partitions := t.partitions
	t.partitions = make(map[string]*BleveDestPartition)

	t.bindex.Close()
	t.bindex = nil

	go func() {
		// Cancel/error any consistency wait requests.
		err := fmt.Errorf("bleve: closeUnlocked")

		for _, bdp := range partitions {
			bdp.m.Lock()
			for _, cwr := range bdp.cwrQueue {
				cwr.DoneCh <- err
				close(cwr.DoneCh)
			}
			bdp.m.Unlock()
		}
	}()

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) Rollback(partition string, rollbackSeq uint64) error {
	t.AddError("dest rollback", partition, nil, rollbackSeq, nil, nil)

	t.m.Lock()
	defer t.m.Unlock()

	// NOTE: A rollback of any partition means a rollback of all
	// partitions, since they all share a single bleve.Index backend.
	// That's why we grab and keep BleveDest.m locked.
	//
	// TODO: Implement partial rollback one day.  Implementation
	// sketch: we expect bleve to one day to provide an additional
	// Snapshot() and Rollback() API, where Snapshot() returns some
	// opaque and persistable snapshot ID ("SID"), which cbft can
	// occasionally record into the bleve's Get/SetInternal() storage.
	// A stream rollback operation then needs to loop through
	// appropriate candidate SID's until a Rollback(SID) succeeds.
	// Else, we eventually devolve down to restarting/rebuilding
	// everything from scratch or zero.
	//
	// For now, always rollback to zero, in which we close the pindex,
	// erase files and have the janitor rebuild from scratch.

	err := t.closeUnlocked()
	if err != nil {
		return fmt.Errorf("bleve: can't close during rollback,"+
			" err: %v", err)
	}

	os.RemoveAll(t.path)

	t.restart()

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	if consistencyLevel == "" {
		return nil
	}
	if consistencyLevel != "at_plus" {
		return fmt.Errorf("bleve: unsupported consistencyLevel: %s",
			consistencyLevel)
	}

	cwr := &cbgt.ConsistencyWaitReq{
		PartitionUUID:    partitionUUID,
		ConsistencyLevel: consistencyLevel,
		ConsistencySeq:   consistencySeq,
		CancelCh:         cancelCh,
		DoneCh:           make(chan error, 1),
	}

	t.m.Lock()

	bdp, err := t.getPartitionUnlocked(partition)
	if err != nil {
		t.m.Unlock()
		return err
	}

	bdp.m.Lock()

	uuid, seq := bdp.lastUUID, bdp.seqMaxBatch
	if cwr.PartitionUUID != "" && cwr.PartitionUUID != uuid {
		cwr.DoneCh <- fmt.Errorf("bleve: pindex_consistency"+
			" mismatched partition, uuid: %s, cwr: %#v", uuid, cwr)
		close(cwr.DoneCh)
	} else if cwr.ConsistencySeq > seq {
		heap.Push(&bdp.cwrQueue, cwr)
	} else {
		close(cwr.DoneCh)
	}

	bdp.m.Unlock()

	t.m.Unlock()

	return cbgt.ConsistencyWaitDone(partition, cancelCh, cwr.DoneCh,
		func() uint64 {
			bdp.m.Lock()
			seqMaxBatch := bdp.seqMaxBatch
			bdp.m.Unlock()
			return seqMaxBatch
		})
}

// ---------------------------------------------------------

func (t *BleveDest) Count(pindex *cbgt.PIndex, cancelCh <-chan bool) (
	uint64, error) {
	return t.bindex.DocCount()
}

// ---------------------------------------------------------

func (t *BleveDest) Query(pindex *cbgt.PIndex, req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	queryCtlParams := cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
		},
	}

	err := json.Unmarshal(req, &queryCtlParams)
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query"+
			" parsing queryCtlParams, req: %s, err: %v", req, err)
	}

	searchRequest := &bleve.SearchRequest{}

	err = json.Unmarshal(req, searchRequest)
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query"+
			" parsing searchRequest, req: %s, err: %v", req, err)
	}

	err = cbgt.ConsistencyWaitPIndex(pindex, t,
		queryCtlParams.Ctl.Consistency, cancelCh)
	if err != nil {
		return err
	}

	err = searchRequest.Query.Validate()
	if err != nil {
		return err
	}

	searchResponse, err := t.bindex.Search(searchRequest)
	if err != nil {
		return err
	}

	rest.MustEncode(res, searchResponse)

	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) AddError(op, partition string,
	key []byte, seq uint64, val []byte, err error) {
	log.Printf("bleve: %s, partition: %s, key: %q, seq: %d,"+
		" val: %q, err: %v", op, partition, key, seq, val, err)

	e := struct {
		Time      string
		Op        string
		Partition string
		Key       string
		Seq       uint64
		Val       string
		Err       string
	}{
		Time:      time.Now().Format(time.RFC3339Nano),
		Op:        op,
		Partition: partition,
		Key:       string(key),
		Seq:       seq,
		Val:       string(val),
		Err:       fmt.Sprintf("%v", err),
	}

	buf, err := json.Marshal(&e)
	if err == nil {
		t.m.Lock()
		for t.stats.Errors.Len() >= cbgt.PINDEX_STORE_MAX_ERRORS {
			t.stats.Errors.Remove(t.stats.Errors.Front())
		}
		t.stats.Errors.PushBack(string(buf))
		t.m.Unlock()
	}
}

// ---------------------------------------------------------

type JSONStatsWriter interface {
	WriteJSON(w io.Writer)
}

var prefixPIndexStoreStats = []byte(`{"pindexStoreStats":`)

func (t *BleveDest) Stats(w io.Writer) (err error) {
	var c uint64

	w.Write(prefixPIndexStoreStats)

	t.m.Lock()

	t.stats.WriteJSON(w)

	if t.bindex != nil {
		_, kvs, err := t.bindex.Advanced()
		if err == nil && kvs != nil {
			m, ok := kvs.(JSONStatsWriter)
			if ok {
				w.Write([]byte(`,"bleveKVStoreStats":`))
				m.WriteJSON(w)
			}
		}

		c, err = t.bindex.DocCount()
	}

	t.m.Unlock()

	if err == nil {
		w.Write([]byte(`,"basic":{"DocCount":`))
		w.Write([]byte(strconv.FormatUint(c, 10)))
		w.Write(cbgt.JsonCloseBrace)
	}

	w.Write([]byte(`,"partitions":{`))
	first := true
	t.m.Lock()
	for partition, bdp := range t.partitions {
		if first {
			w.Write([]byte(`"`))
		} else {
			w.Write([]byte(`,"`))
		}
		w.Write([]byte(partition))
		w.Write([]byte(`":{"seq":`))
		w.Write([]byte(strconv.FormatUint(bdp.seqMax, 10)))
		w.Write([]byte(`,"uuid":"`))
		w.Write([]byte(bdp.lastUUID))
		w.Write([]byte(`"}`))
		first = false
	}
	t.m.Unlock()
	w.Write(cbgt.JsonCloseBrace)

	w.Write(cbgt.JsonCloseBrace)

	return nil
}

// ---------------------------------------------------------

func (t *BleveDestPartition) Close() error {
	return t.bdest.Close()
}

func (t *BleveDestPartition) DataUpdate(partition string,
	key []byte, seq uint64, val []byte,
	cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	k := string(key)

	var v interface{}

	var errv error
	var erri error

	t.m.Lock()

	errv = json.Unmarshal(val, &v)
	if errv == nil {
		erri = t.batch.Index(k, v)
	}
	err := t.updateSeqUnlocked(seq)

	t.m.Unlock()

	if errv != nil {
		t.bdest.AddError("json.Unmarshal", partition, key, seq, val, errv)
	}
	if erri != nil {
		t.bdest.AddError("batch.Index", partition, key, seq, val, erri)
	}

	return err
}

func (t *BleveDestPartition) DataDelete(partition string,
	key []byte, seq uint64,
	cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	t.m.Lock()

	t.batch.Delete(string(key)) // TODO: string(key) makes garbage?
	err := t.updateSeqUnlocked(seq)

	t.m.Unlock()
	return err
}

func (t *BleveDestPartition) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	t.m.Lock()

	err := t.applyBatchUnlocked()
	if err != nil {
		t.m.Unlock()
		return err
	}

	t.seqSnapEnd = snapEnd

	t.m.Unlock()
	return nil
}

func (t *BleveDestPartition) OpaqueGet(partition string) ([]byte, uint64, error) {
	t.m.Lock()

	if t.lastOpaque == nil {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		value, err := t.bindex.GetInternal(t.partitionOpaque)
		if err != nil {
			t.m.Unlock()
			return nil, 0, err
		}
		t.lastOpaque = append([]byte(nil), value...) // Note: copies value.
		t.lastUUID = cbgt.ParseOpaqueToUUID(value)
	}

	if t.seqMax <= 0 {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		buf, err := t.bindex.GetInternal([]byte(t.partition))
		if err != nil {
			t.m.Unlock()
			return nil, 0, err
		}
		if len(buf) <= 0 {
			t.m.Unlock()
			return t.lastOpaque, 0, nil // No seqMax buf is a valid case.
		}
		if len(buf) != 8 {
			t.m.Unlock()
			return nil, 0, fmt.Errorf("bleve: unexpected size for seqMax bytes")
		}
		t.seqMax = binary.BigEndian.Uint64(buf[0:8])
		binary.BigEndian.PutUint64(t.seqMaxBuf, t.seqMax)
	}

	lastOpaque, seqMax := t.lastOpaque, t.seqMax

	t.m.Unlock()
	return lastOpaque, seqMax, nil
}

func (t *BleveDestPartition) OpaqueSet(partition string, value []byte) error {
	t.m.Lock()

	t.lastOpaque = append(t.lastOpaque[0:0], value...)
	t.lastUUID = cbgt.ParseOpaqueToUUID(value)

	t.batch.SetInternal(t.partitionOpaque, t.lastOpaque)

	t.m.Unlock()
	return nil
}

func (t *BleveDestPartition) Rollback(partition string,
	rollbackSeq uint64) error {
	return t.bdest.Rollback(partition, rollbackSeq)
}

func (t *BleveDestPartition) ConsistencyWait(
	partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	return t.bdest.ConsistencyWait(partition, partitionUUID,
		consistencyLevel, consistencySeq, cancelCh)
}

func (t *BleveDestPartition) Count(pindex *cbgt.PIndex,
	cancelCh <-chan bool) (
	uint64, error) {
	return t.bdest.Count(pindex, cancelCh)
}

func (t *BleveDestPartition) Query(pindex *cbgt.PIndex,
	req []byte, res io.Writer,
	cancelCh <-chan bool) error {
	return t.bdest.Query(pindex, req, res, cancelCh)
}

func (t *BleveDestPartition) Stats(w io.Writer) error {
	return t.bdest.Stats(w)
}

// ---------------------------------------------------------

func (t *BleveDestPartition) updateSeqUnlocked(seq uint64) error {
	if t.seqMax < seq {
		t.seqMax = seq
		binary.BigEndian.PutUint64(t.seqMaxBuf, t.seqMax)

		// TODO: Perhaps copy partition bytes to buf?
		t.batch.SetInternal([]byte(t.partition), t.seqMaxBuf)
	}

	if seq < t.seqSnapEnd {
		return nil
	}

	return t.applyBatchUnlocked()
}

func (t *BleveDestPartition) applyBatchUnlocked() error {
	err := cbgt.Timer(func() error {
		return t.bindex.Batch(t.batch)
	}, t.bdest.stats.TimerBatchStore)
	if err != nil {
		return err
	}

	t.seqMaxBatch = t.seqMax

	for t.cwrQueue.Len() > 0 &&
		t.cwrQueue[0].ConsistencySeq <= t.seqMaxBatch {
		cwr := heap.Pop(&t.cwrQueue).(*cbgt.ConsistencyWaitReq)
		if cwr != nil && cwr.DoneCh != nil {
			close(cwr.DoneCh)
		}
	}

	// TODO: Would be good to reuse batch's memory; but, would need
	// some public Reset() kind of method on bleve.Batch?
	t.batch = t.bindex.NewBatch()

	return nil
}

// ---------------------------------------------------------

// Returns a bleve.IndexAlias that represents all the PIndexes for the
// index, including perhaps bleve remote client PIndexes.
//
// TODO: Perhaps need a tighter check around indexUUID, as the current
// implementation might have a race where old pindexes with a matching
// (but invalid) indexUUID might be hit.
//
// TODO: If this returns an error, perhaps the caller somewhere up the
// chain should close the cancelCh to help stop any other inflight
// activities.
func bleveIndexAlias(mgr *cbgt.Manager, indexName, indexUUID string,
	ensureCanRead bool, consistencyParams *cbgt.ConsistencyParams,
	cancelCh <-chan bool) (bleve.IndexAlias, error) {
	planPIndexNodeFilter := cbgt.PlanPIndexNodeOk
	if ensureCanRead {
		planPIndexNodeFilter = cbgt.PlanPIndexNodeCanRead
	}

	localPIndexes, remotePlanPIndexes, err :=
		mgr.CoveringPIndexes(indexName, indexUUID, planPIndexNodeFilter,
			"queries")
	if err != nil {
		return nil, fmt.Errorf("bleve: bleveIndexAlias, err: %v", err)
	}

	alias := bleve.NewIndexAlias()

	for _, remotePlanPIndex := range remotePlanPIndexes {
		baseURL := "http://" + remotePlanPIndex.NodeDef.HostPort +
			"/api/pindex/" + remotePlanPIndex.PlanPIndex.Name
		alias.Add(&IndexClient{
			QueryURL:    baseURL + "/query",
			CountURL:    baseURL + "/count",
			Consistency: consistencyParams,
			// TODO: Propagate auth to remote client.
		})
	}

	// TODO: Should kickoff remote queries concurrently before we wait.

	err = cbgt.ConsistencyWaitGroup(indexName, consistencyParams,
		cancelCh, localPIndexes,
		func(localPIndex *cbgt.PIndex) error {
			bindex, ok := localPIndex.Impl.(bleve.Index)
			if !ok || bindex == nil ||
				!strings.HasPrefix(localPIndex.IndexType, "bleve") {
				return fmt.Errorf("bleve: wrong type, localPIndex: %#v",
					localPIndex)
			}
			alias.Add(bindex)
			return nil
		})
	if err != nil {
		return nil, err
	}

	return alias, nil
}

// ---------------------------------------------------------

func BlevePIndexImplInitRouter(r *mux.Router, phase string) {
	if phase == "static.before" {
		// Handlers from bleve/http/mapping need earlier precedence.
		bleveHttpMappingStatic := http.FileServer(bleveHttpMapping.AssetFS())

		r.PathPrefix("/static/partials/analysis").Handler(
			http.StripPrefix("/static/", bleveHttpMappingStatic))
		r.PathPrefix("/static/partials/mapping").Handler(
			http.StripPrefix("/static/", bleveHttpMappingStatic))
		r.PathPrefix("/static/js/mapping").Handler(
			http.StripPrefix("/static/", bleveHttpMappingStatic))

		bleveHttpMapping.RegisterHandlers(r, "/api")
	}

	if phase == "manager.after" {
		// Using standard bleveHttp handlers for /api/pindex-bleve endpoints.
		//
		listIndexesHandler := bleveHttp.NewListIndexesHandler()
		r.Handle("/api/pindex-bleve",
			listIndexesHandler).Methods("GET")

		getIndexHandler := bleveHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}",
			getIndexHandler).Methods("GET")

		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/count",
			docCountHandler).Methods("GET")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/query",
			searchHandler).Methods("POST")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = rest.PIndexNameLookup
		docGetHandler.DocIDLookup = rest.DocIDLookup
		r.Handle("/api/pindex-bleve/{pindexName}/doc/{docID}",
			docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = rest.PIndexNameLookup
		debugDocHandler.DocIDLookup = rest.DocIDLookup
		r.Handle("/api/pindex-bleve/{pindexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle("/api/pindex-bleve/{pindexName}/fields",
			listFieldsHandler).Methods("GET")
	}
}

func BleveMetaExtra(m map[string]interface{}) {
	br := make(map[string]map[string][]string)

	t, i := bleveRegistry.AnalyzerTypesAndInstances()
	br["Analyzer"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.ByteArrayConverterTypesAndInstances()
	br["ByteArrayConverter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.CharFilterTypesAndInstances()
	br["CharFilter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.DateTimeParserTypesAndInstances()
	br["DateTimeParser"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.FragmentFormatterTypesAndInstances()
	br["FragmentFormatter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.FragmenterTypesAndInstances()
	br["Fragmenter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.HighlighterTypesAndInstances()
	br["Highlighter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.KVStoreTypesAndInstances()
	br["KVStore"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.TokenFilterTypesAndInstances()
	br["TokenFilter"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.TokenMapTypesAndInstances()
	br["TokenMap"] = map[string][]string{"types": t, "instances": i}
	t, i = bleveRegistry.TokenizerTypesAndInstances()
	br["Tokenizer"] = map[string][]string{"types": t, "instances": i}

	m["regBleve"] = br
}

// ---------------------------------------------------------

func BlevePIndexQuerySamples() []cbgt.Documentation {
	return []cbgt.Documentation{
		cbgt.Documentation{
			Text: "A simple bleve query POST body:",
			JSON: &struct {
				*cbgt.QueryCtlParams
				*bleve.SearchRequest
			}{
				nil,
				&bleve.SearchRequest{
					From:  0,
					Size:  10,
					Query: bleve.NewQueryStringQuery("a sample query"),
				},
			},
		},
		cbgt.Documentation{
			Text: `An example POST body using from/size for results paging,
using ctl for a timeout and for "at_plus" consistency level.
On consistency, the index must have incorporated at least mutation
sequence-number 123 for partition (vbucket) 0 and mutation
sequence-number 234 for partition (vbucket) 1 (where vbucket 1
should have a vbucketUUID of a0b1c2):`,
			JSON: &struct {
				*cbgt.QueryCtlParams
				*bleve.SearchRequest
			}{
				&cbgt.QueryCtlParams{
					Ctl: cbgt.QueryCtl{
						Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
						Consistency: &cbgt.ConsistencyParams{
							Level: "at_plus",
							Vectors: map[string]cbgt.ConsistencyVector{
								"customerIndex": cbgt.ConsistencyVector{
									"0":        123,
									"1/a0b1c2": 234,
								},
							},
						},
					},
				},
				&bleve.SearchRequest{
					From:      20,
					Size:      10,
					Fields:    []string{"*"},
					Query:     bleve.NewQueryStringQuery("alice smith"),
					Highlight: bleve.NewHighlight(),
					Explain:   true,
				},
			},
		},
	}
}

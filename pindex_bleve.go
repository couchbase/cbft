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

	"golang.org/x/net/context"

	"github.com/gorilla/mux"

	"github.com/rcrowley/go-metrics"

	"github.com/blevesearch/bleve"
	bleveMappingUI "github.com/blevesearch/bleve-mapping-ui"
	_ "github.com/blevesearch/bleve/config"
	bleveHttp "github.com/blevesearch/bleve/http"
	bleveRegistry "github.com/blevesearch/bleve/registry"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

var BleveMaxOpsPerBatch = 200 // Unlimited when <= 0.

var BlevePIndexAllowMoss = false // Unit tests prefer no moss.

type BleveParams struct {
	Mapping   bleve.IndexMapping     `json:"mapping"`
	Store     map[string]interface{} `json:"store"`
	DocConfig BleveDocumentConfig    `json:"doc_config"`
}

func NewBleveParams() *BleveParams {
	rv := &BleveParams{
		Mapping: *bleve.NewIndexMapping(),
		Store: map[string]interface{}{
			"kvStoreName": bleve.Config.DefaultKVStore,
		},
		DocConfig: BleveDocumentConfig{
			Mode:      "type_field",
			TypeField: "type",
		},
	}

	return rv
}

type BleveDest struct {
	path string

	bleveDocConfig BleveDocumentConfig

	// Invoked when mgr should restart this BleveDest, like on rollback.
	restart func()

	m          sync.Mutex // Protects the fields that follow.
	bindex     bleve.Index
	partitions map[string]*BleveDestPartition

	rev uint64 // Incremented whenever bindex changes.

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
	restart func(), bleveDocConfig BleveDocumentConfig) *BleveDest {
	return &BleveDest{
		path:           path,
		bleveDocConfig: bleveDocConfig,
		restart:        restart,
		bindex:         bindex,
		partitions:     make(map[string]*BleveDestPartition),
		stats: cbgt.PIndexStoreStats{
			TimerBatchStore: metrics.NewTimer(),
			Errors:          list.New(),
		},
	}
}

// ---------------------------------------------------------

const bleveQueryHelp = `<a href="http://www.blevesearch.com/docs/Query-String-Query/">
       full text query syntax help
     </a>`

func init() {
	cbgt.RegisterPIndexImplType("fulltext-index", &cbgt.PIndexImplType{
		Validate: ValidateBlevePIndexImpl,

		New:   NewBlevePIndexImpl,
		Open:  OpenBlevePIndexImpl,
		Count: CountBlevePIndexImpl,
		Query: QueryBlevePIndexImpl,

		Description: "general/fulltext-index " +
			" - a full text index powered by the bleve engine",
		StartSample:  NewBleveParams(),
		QuerySamples: BlevePIndexQuerySamples,
		QueryHelp:    bleveQueryHelp,
		InitRouter:   BlevePIndexImplInitRouter,
		DiagHandlers: []cbgt.DiagHandler{
			{"/api/pindex-bleve", bleveHttp.NewListIndexesHandler(), nil},
		},
		MetaExtra: BleveMetaExtra,
		UI: map[string]string{
			"controllerInitName": "blevePIndexInitController",
			"controllerDoneName": "blevePIndexDoneController",
		},
	})
}

func ValidateBlevePIndexImpl(indexType, indexName, indexParams string) error {
	if len(indexParams) <= 0 {
		return nil
	}

	b, err := bleveMappingUI.CleanseJSON([]byte(indexParams))
	if err != nil {
		return fmt.Errorf("bleve: validate CleanseJSON,"+
			" err: %v", err)
	}

	bp := NewBleveParams()

	err = json.Unmarshal(b, bp)
	if err != nil {
		return fmt.Errorf("bleve: validate params, err: %v", err)
	}

	err = bp.Mapping.Validate()
	if err != nil {
		return fmt.Errorf("bleve: validate mapping, err: %v", err)
	}

	return nil
}

func NewBlevePIndexImpl(indexType, indexParams, path string,
	restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	bleveParams := NewBleveParams()

	if len(indexParams) > 0 {
		buf, err := bleveMappingUI.CleanseJSON([]byte(indexParams))
		if err != nil {
			return nil, nil, fmt.Errorf("bleve: cleanse params, err: %v", err)
		}

		err = json.Unmarshal(buf, bleveParams)
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

	// Use the "moss" wrapper KVStore if it's allowed, available
	// and also not already configured.
	kvStoreMossAllow := true
	ksmv, exists := kvConfig["kvStoreMossAllow"]
	if exists {
		v, ok := ksmv.(bool)
		if ok {
			kvStoreMossAllow = v
		}
	}

	if kvStoreMossAllow && BlevePIndexAllowMoss {
		_, exists := kvConfig["mossLowerLevelStoreName"]
		if !exists &&
			kvStoreName != "moss" &&
			bleveRegistry.KVStoreConstructorByName("moss") != nil {
			kvConfig["mossLowerLevelStoreName"] = kvStoreName

			if kvStoreName == "forestdb" {
				if _, exists := kvConfig["skip_batch"]; !exists {
					kvConfig["skip_batch"] = true
				}
			}

			kvStoreName = "moss"
		}

		_, exists = kvConfig["mossCollectionOptionsName"]
		if !exists {
			kvConfig["mossCollectionOptionsName"] = "fts"
		}
	}

	// Use the "metrics" wrapper KVStore if it's allowed, available
	// and also not already configured.
	kvStoreMetricsAllow := true
	ksmv, exists = kvConfig["kvStoreMetricsAllow"]
	if exists {
		v, ok := ksmv.(bool)
		if ok {
			kvStoreMetricsAllow = v
		}
	}

	if kvStoreMetricsAllow {
		_, exists := kvConfig["kvStoreName_actual"]
		if !exists &&
			kvStoreName != "metrics" &&
			bleveRegistry.KVStoreConstructorByName("metrics") != nil {
			kvConfig["kvStoreName_actual"] = kvStoreName
			kvStoreName = "metrics"
		}
	}

	bleveIndexType, ok := bleveParams.Store["indexType"].(string)
	if !ok || bleveIndexType == "" {
		bleveIndexType = bleve.Config.DefaultIndexType
	}

	bindex, err := bleve.NewUsing(path, &bleveParams.Mapping,
		bleveIndexType, kvStoreName, kvConfig)
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
		DestProvider: NewBleveDest(path, bindex, restart, bleveParams.DocConfig),
	}, nil
}

func OpenBlevePIndexImpl(indexType, path string,
	restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	buf, err := ioutil.ReadFile(path +
		string(os.PathSeparator) + "PINDEX_BLEVE_META")
	if err != nil {
		return nil, nil, err
	}

	buf, err = bleveMappingUI.CleanseJSON(buf)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: cleanse params, err: %v", err)
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
		DestProvider: NewBleveDest(path, bindex, restart, bleveParams.DocConfig),
	}, nil
}

// ---------------------------------------------------------------

func CountBlevePIndexImpl(mgr *cbgt.Manager, indexName, indexUUID string) (
	uint64, error) {
	alias, _, err := bleveIndexAlias(mgr, indexName, indexUUID, false, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("bleve: CountBlevePIndexImpl indexAlias error,"+
			" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
	}

	return alias.DocCount()
}

func ValidateConsistencyParams(c *cbgt.ConsistencyParams) error {
	switch c.Level {
	case "":
		return nil
	case "at_plus":
		return nil
	}
	return fmt.Errorf("unsupported consistencyLevel: %s", c.Level)
}

func QueryBlevePIndexImpl(mgr *cbgt.Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {

	// phase 0 - parsing/validating query
	// could return err 400
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

	if queryCtlParams.Ctl.Consistency != nil {
		err = ValidateConsistencyParams(queryCtlParams.Ctl.Consistency)
		if err != nil {
			return fmt.Errorf("bleve: QueryBlevePIndexImpl"+
				" validating consistency, req: %s, err: %v", req, err)
		}
	}

	err = searchRequest.Validate()
	if err != nil {
		return fmt.Errorf("bleve: QueryBlevePIndexImpl"+
			" validating request, req: %s, err: %v", req, err)
	}

	v, exists := mgr.Options()["bleveMaxResultWindow"]
	if exists {
		bleveMaxResultWindow, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("bleve: QueryBlevePIndexImpl"+
				" atoi: %v, err: %v", v, err)
		}

		if searchRequest.From+searchRequest.Size > bleveMaxResultWindow {
			return fmt.Errorf("bleve: bleveMaxResultWindow exceeded,"+
				" from: %d, size: %d, bleveMaxResultWindow: %d",
				searchRequest.From, searchRequest.Size, bleveMaxResultWindow)
		}
	}

	// phase 1 - set up timeouts, wait for local consistency reqiurements
	// to be satisfied, could return err 412

	// create a context with the appropriate timeout
	ctx, cancel, cancelCh := setupContextAndCancelCh(queryCtlParams, nil)
	// defer a call to cancel, this ensures that goroutine from
	// setupContextAndCancelCh always exits
	defer cancel()

	alias, remoteClients, err := bleveIndexAlias(mgr, indexName, indexUUID, true,
		queryCtlParams.Ctl.Consistency, cancelCh)
	if err != nil {
		return err
	}

	searchResult, err := alias.SearchInContext(ctx, searchRequest)
	if searchResult != nil {
		// check to see if any of the remote searches returned anything
		// other than 0, 200 or 412, these are returned to the user as
		// error status 400, and appear as phase 0 errors detected late
		// 0 means we never heard anything back, that is dealt with
		// in the following section
		for _, remoteClient := range remoteClients {
			lastStatus, lastErrBody := remoteClient.GetLast()
			if lastStatus != http.StatusOK &&
				lastStatus != http.StatusPreconditionFailed &&
				lastStatus != 0 {
				return fmt.Errorf("bleve: QueryBlevePIndexImpl remote client"+
					" returned status: %d body: %s", lastStatus, lastErrBody)
			}
		}
		// now see if any of the remote searches returned 412, these should be
		// collated into a single 412 response at this level, these should
		// be presented as phase 1 errors detected late
		remoteConsistencyWaitError := cbgt.ErrorConsistencyWait{
			Status:       "remote consistency error",
			StartEndSeqs: make(map[string][]uint64),
		}
		numRemoteSilent := 0
		for _, remoteClient := range remoteClients {
			lastStatus, lastErrBody := remoteClient.GetLast()
			if lastStatus == 0 {
				numRemoteSilent++
			}
			if lastStatus == http.StatusPreconditionFailed {
				var remoteConsistencyErr = struct {
					StartEndSeqs map[string][]uint64 `json:"startEndSeqs"`
				}{}
				err := json.Unmarshal(lastErrBody, &remoteConsistencyErr)
				if err == nil {
					for k, v := range remoteConsistencyErr.StartEndSeqs {
						remoteConsistencyWaitError.StartEndSeqs[k] = v
					}
				}
			}
		}
		// if we had any explicitly returned consistency errors, return those
		if len(remoteConsistencyWaitError.StartEndSeqs) > 0 {
			return &remoteConsistencyWaitError
		}

		// we had *some* consistency requirements, but we never heard back
		// from some of the remote pindexes, just punt for now and return
		// a mostly empty 412 indicating we aren't sure
		if queryCtlParams.Ctl.Consistency != nil &&
			len(queryCtlParams.Ctl.Consistency.Vectors) > 0 &&
			numRemoteSilent > 0 {
			return &remoteConsistencyWaitError
		}

		rest.MustEncode(res, searchResult)
	}

	return err
}

// ---------------------------------------------------------

func (t *BleveDest) Dest(partition string) (cbgt.Dest, error) {
	t.m.Lock()
	d, err := t.getPartitionLOCKED(partition)
	t.m.Unlock()
	return d, err
}

func (t *BleveDest) getPartitionLOCKED(partition string) (
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
	err := t.closeLOCKED()
	t.m.Unlock()
	return err
}

func (t *BleveDest) closeLOCKED() error {
	if t.bindex == nil {
		return nil // Already closed.
	}

	partitions := t.partitions
	t.partitions = make(map[string]*BleveDestPartition)

	t.bindex.Close()
	t.bindex = nil

	go func() {
		// Cancel/error any consistency wait requests.
		err := fmt.Errorf("bleve: closeLOCKED")

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

	err := t.closeLOCKED()
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

	bdp, err := t.getPartitionLOCKED(partition)
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
	t.m.Lock()
	bindex := t.bindex
	t.m.Unlock()

	if bindex == nil {
		return 0, fmt.Errorf("bleve: Count, bindex already closed")
	}

	return bindex.DocCount()
}

// ---------------------------------------------------------

func (t *BleveDest) Query(pindex *cbgt.PIndex, req []byte, res io.Writer,
	parentCancelCh <-chan bool) error {

	// phase 0 - parsing/validating query
	// could return err 400
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
	err = searchRequest.Validate()
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query"+
			" validating request, req: %s, err: %v", req, err)
	}

	// phase 1 - set up timeouts, wait to satisfy consistency requirements
	// could return err 412

	// create a context with the appropriate timeout
	ctx, cancel, cancelCh := setupContextAndCancelCh(queryCtlParams, parentCancelCh)
	// defer a call to cancel, this ensures that goroutine from
	// setupContextAndCancelCh always exits
	defer cancel()

	err = cbgt.ConsistencyWaitPIndex(pindex, t,
		queryCtlParams.Ctl.Consistency, cancelCh)
	if err != nil {
		if _, ok := err.(*cbgt.ErrorConsistencyWait); !ok {
			// not a consistency wait error
			// check to see if context error
			if ctx.Err() != nil {
				// return this as search response error
				sendSearchResponseErr(searchRequest, res, pindex.Name, ctx.Err())
				return nil
			}
		}
		// some other error occurred return this as 400
		return err
	}

	// phase 2 - execute query
	// always 200, possibly with errors inside status
	t.m.Lock()
	bindex := t.bindex
	t.m.Unlock()

	if bindex == nil {
		err := fmt.Errorf("bleve: Query, bindex already closed")
		sendSearchResponseErr(searchRequest, res, pindex.Name, err)
		return nil
	}

	searchResponse, err := bindex.SearchInContext(ctx, searchRequest)
	if err != nil {
		sendSearchResponseErr(searchRequest, res, pindex.Name, err)
		return nil
	}

	rest.MustEncode(res, searchResponse)
	return nil
}

// ---------------------------------------------------------
func sendSearchResponseErr(req *bleve.SearchRequest, res io.Writer, name string, err error) {
	searchResponse := &bleve.SearchResult{
		Request: req,
		Status: &bleve.SearchStatus{
			Total:      1,
			Failed:     1,
			Successful: 0,
			Errors:     make(map[string]error),
		},
	}
	searchResponse.Status.Errors[name] = err
	rest.MustEncode(res, searchResponse)
}

// ---------------------------------------------------------

func setupContextAndCancelCh(queryCtlParams cbgt.QueryCtlParams, parentCancelCh <-chan bool) (ctx context.Context, cancel context.CancelFunc, cancelChRv <-chan bool) {
	if queryCtlParams.Ctl.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(queryCtlParams.Ctl.Timeout)*time.Millisecond)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	// now create cbgt compatible cancel channel
	cancelCh := make(chan bool, 1)
	cancelChRv = cancelCh
	// spawn a goroutine to close the cancelCh when either:
	//   - the context is Done()
	// or
	//   - the parentCancelCh is closed
	go func() {
		select {
		case <-parentCancelCh:
			close(cancelCh)
		case <-ctx.Done():
			close(cancelCh)
		}
	}()
	return
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
	WriteJSON(w io.Writer) error
}

var prefixPIndexStoreStats = []byte(`{"pindexStoreStats":`)

func (t *BleveDest) Stats(w io.Writer) (err error) {
	var c uint64

	_, err = w.Write(prefixPIndexStoreStats)
	if err != nil {
		return
	}

	t.m.Lock()
	defer t.m.Unlock()

	t.stats.WriteJSON(w)

	if t.bindex != nil {
		_, err = w.Write([]byte(`,"bleveIndexStats":`))
		if err != nil {
			return
		}
		idxStats := t.bindex.StatsMap()
		var idxStatsJSON []byte
		idxStatsJSON, err = json.Marshal(idxStats)
		if err != nil {
			log.Printf("json failed to marshal was: %#v", idxStats)
			return
		}
		_, err = w.Write(idxStatsJSON)
		if err != nil {
			return
		}

		c, err = t.bindex.DocCount()
		if err != nil {
			return
		}
	}

	if err == nil {
		_, err = w.Write([]byte(`,"basic":{"DocCount":`))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(strconv.FormatUint(c, 10)))
		if err != nil {
			return
		}
		_, err = w.Write(cbgt.JsonCloseBrace)
		if err != nil {
			return
		}
	}

	_, err = w.Write([]byte(`,"partitions":{`))
	if err != nil {
		return
	}
	first := true
	for partition, bdp := range t.partitions {
		bdp.m.Lock()
		bdpSeqMax := bdp.seqMax
		bdpSeqMaxBatch := bdp.seqMaxBatch
		bdpLastUUID := bdp.lastUUID
		bdp.m.Unlock()

		if first {
			_, err = w.Write([]byte(`"`))
			if err != nil {
				return
			}
		} else {
			_, err = w.Write([]byte(`,"`))
			if err != nil {
				return
			}
		}
		_, err = w.Write([]byte(partition))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(`":{"seq":`))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(strconv.FormatUint(bdpSeqMaxBatch, 10)))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(`,"seqReceived":`))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(strconv.FormatUint(bdpSeqMax, 10)))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(`,"uuid":"`))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(bdpLastUUID))
		if err != nil {
			return
		}
		_, err = w.Write([]byte(`"}`))
		if err != nil {
			return
		}
		first = false
	}
	_, err = w.Write(cbgt.JsonCloseBrace)
	if err != nil {
		return
	}

	_, err = w.Write(cbgt.JsonCloseBrace)
	if err != nil {
		return
	}

	return nil
}

func (t *BleveDest) StatsMap() (rv map[string]interface{}, err error) {
	rv = make(map[string]interface{})

	t.m.Lock()
	bindex := t.bindex
	t.m.Unlock()

	if bindex != nil {
		rv["bleveIndexStats"] = bindex.StatsMap()

		var c uint64
		c, err = bindex.DocCount()
		if err != nil {
			return
		}
		rv["DocCount"] = c
	}

	return
}

// ---------------------------------------------------------

// Implements the PartitionSeqProvider interface.
func (t *BleveDest) PartitionSeqs() (map[string]cbgt.UUIDSeq, error) {
	rv := map[string]cbgt.UUIDSeq{}

	t.m.Lock()

	for partition, bdp := range t.partitions {
		bdp.m.Lock()
		bdpSeqMaxBatch := bdp.seqMaxBatch
		bdpLastUUID := bdp.lastUUID
		bdp.m.Unlock()

		rv[partition] = cbgt.UUIDSeq{
			UUID: bdpLastUUID,
			Seq:  bdpSeqMaxBatch,
		}
	}

	t.m.Unlock()

	return rv, nil
}

// ---------------------------------------------------------

func (t *BleveDestPartition) Close() error {
	return t.bdest.Close()
}

func (t *BleveDestPartition) DataUpdate(partition string,
	key []byte, seq uint64, val []byte, cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {

	t.m.Lock()

	if t.batch == nil {
		t.m.Unlock()
		return fmt.Errorf("bleve: DataUpdate nil batch")
	}

	cbftDoc, errv := t.bdest.bleveDocConfig.buildDocument(key, val,
		t.bindex.Mapping().DefaultType)

	erri := t.batch.Index(string(key), cbftDoc)

	revNeedsUpdate, err := t.updateSeqLOCKED(seq)

	t.m.Unlock()

	if err == nil && revNeedsUpdate {
		t.incRev()
	}
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

	if t.batch == nil {
		t.m.Unlock()
		return fmt.Errorf("bleve: DataDelete nil batch")
	}

	t.batch.Delete(string(key)) // TODO: string(key) makes garbage?

	revNeedsUpdate, err := t.updateSeqLOCKED(seq)

	t.m.Unlock()

	if err == nil && revNeedsUpdate {
		t.incRev()
	}

	return err
}

func (t *BleveDestPartition) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	t.m.Lock()

	revNeedsUpdate, err := t.applyBatchLOCKED()
	if err != nil {
		t.m.Unlock()
		return err
	}

	t.seqSnapEnd = snapEnd

	t.m.Unlock()

	if revNeedsUpdate {
		t.incRev()
	}

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

		if t.seqMaxBatch <= 0 {
			t.seqMaxBatch = t.seqMax
		}
	}

	lastOpaque, seqMax := t.lastOpaque, t.seqMax

	t.m.Unlock()
	return lastOpaque, seqMax, nil
}

func (t *BleveDestPartition) OpaqueSet(partition string, value []byte) error {
	t.m.Lock()

	if t.batch == nil {
		t.m.Unlock()
		return fmt.Errorf("bleve: OpaqueSet nil batch")
	}

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

func (t *BleveDestPartition) updateSeqLOCKED(seq uint64) (bool, error) {
	if t.seqMax < seq {
		t.seqMax = seq
		binary.BigEndian.PutUint64(t.seqMaxBuf, t.seqMax)

		t.batch.SetInternal([]byte(t.partition), t.seqMaxBuf)
	}

	if seq < t.seqSnapEnd &&
		(BleveMaxOpsPerBatch <= 0 || BleveMaxOpsPerBatch > t.batch.Size()) {
		return false, nil
	}

	return t.applyBatchLOCKED()
}

func (t *BleveDestPartition) applyBatchLOCKED() (bool, error) {
	if t.batch == nil {
		return false, fmt.Errorf("bleve: applyBatch batch nil")
	}

	if t.bindex == nil {
		return false, fmt.Errorf("bleve: applyBatch bindex already closed")
	}

	err := cbgt.Timer(func() error {
		// At this point, there should be no other concurrent batch
		// activity on this BleveDestPartition (BDP), since a BDP
		// represents a single vbucket.  Since we don't want to block
		// stats gathering or readers trying to read
		// seqMax/seqMaxBatch/lastUUID, we unlock before entering the
		// (perhaps time consuming) t.bindex.Batch() operation.  By
		// clearing the t.batch to nil, we can also detect concurrent
		// mutations due to errors returned from other methods when
		// they see a nil t.batch.
		batch := t.batch
		t.batch = nil

		bindex := t.bindex
		t.m.Unlock()
		err := bindex.Batch(batch)
		t.m.Lock()

		return err
	}, t.bdest.stats.TimerBatchStore)
	if err != nil {
		return false, err
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
	if t.bindex != nil {
		t.batch = t.bindex.NewBatch()
	}

	return true, nil
}

// ---------------------------------------------------------

func (t *BleveDestPartition) incRev() {
	t.bdest.m.Lock()
	t.bdest.rev++
	t.bdest.m.Unlock()
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
	cancelCh <-chan bool) (bleve.IndexAlias, []*IndexClient, error) {
	alias := bleve.NewIndexAlias()

	remoteClients, err := bleveIndexTargets(mgr, indexName, indexUUID, ensureCanRead,
		consistencyParams, cancelCh, alias)
	if err != nil {
		return nil, nil, err
	}

	return alias, remoteClients, nil
}

// BleveIndexCollector interface is a subset of the bleve.IndexAlias
// interface, with just the Add() method, allowing alternative
// implementations that need to collect "backend" bleve indexes based
// on a user defined index.
type BleveIndexCollector interface {
	Add(i ...bleve.Index)
}

func bleveIndexTargets(mgr *cbgt.Manager, indexName, indexUUID string,
	ensureCanRead bool, consistencyParams *cbgt.ConsistencyParams,
	cancelCh <-chan bool, collector BleveIndexCollector) ([]*IndexClient, error) {
	planPIndexFilterName := "ok"
	if ensureCanRead {
		planPIndexFilterName = "canRead"
	}

	localPIndexes, remotePlanPIndexes, missingPIndexNames, err :=
		mgr.CoveringPIndexesEx(cbgt.CoveringPIndexesSpec{
			IndexName: indexName,
			IndexUUID: indexUUID,
			PlanPIndexFilterName: planPIndexFilterName,
		}, nil, false)
	if err != nil {
		return nil, fmt.Errorf("bleve: bleveIndexTargets, err: %v", err)
	}

	for _, missingPIndexName := range missingPIndexNames {
		missingPIndex := &MissingPIndex{
			name: missingPIndexName,
		}
		collector.Add(missingPIndex)
	}

	prefix := mgr.Options()["urlPrefix"]

	remoteClients := make([]*IndexClient, 0)
	for _, remotePlanPIndex := range remotePlanPIndexes {
		baseURL := "http://" + remotePlanPIndex.NodeDef.HostPort +
			prefix + "/api/pindex/" + remotePlanPIndex.PlanPIndex.Name
		remoteClient := &IndexClient{
			mgr:         mgr,
			name:        fmt.Sprintf("IndexClient - %s", baseURL),
			QueryURL:    baseURL + "/query",
			CountURL:    baseURL + "/count",
			Consistency: consistencyParams,
			// TODO: Propagate auth to remote client.
		}
		remoteClients = append(remoteClients, remoteClient)
		collector.Add(remoteClient)
	}

	// TODO: Should kickoff remote queries concurrently before we wait.

	return remoteClients, cbgt.ConsistencyWaitGroup(indexName, consistencyParams,
		cancelCh, localPIndexes,
		func(localPIndex *cbgt.PIndex) error {
			if !strings.HasPrefix(localPIndex.IndexType, "fulltext-index") {
				return fmt.Errorf("bleve: bleveIndexTargets, wrong type,"+
					" localPIndex: %#v", localPIndex)
			}

			destFwd, ok := localPIndex.Dest.(*cbgt.DestForwarder)
			if !ok || destFwd == nil {
				return fmt.Errorf("bleve: bleveIndexTargets, wrong destFwd type,"+
					" localPIndex: %#v", localPIndex)
			}

			bdest, ok := destFwd.DestProvider.(*BleveDest)
			if !ok || bdest == nil {
				return fmt.Errorf("bleve: bleveIndexTargets, wrong provider type,"+
					" localPIndex: %#v", localPIndex)
			}

			bdest.m.Lock()
			bindex := bdest.bindex
			rev := bdest.rev
			bdest.m.Unlock()

			if bindex == nil {
				return fmt.Errorf("bleve: bleveIndexTargets, nil bindex,"+
					" localPIndex: %#v", localPIndex)
			}

			collector.Add(&cacheBleveIndex{
				pindex: localPIndex,
				bindex: bindex,
				rev:    rev,
				name:   bindex.Name(),
			})

			return nil
		})
}

// ---------------------------------------------------------

func BlevePIndexImplInitRouter(r *mux.Router, phase string,
	mgr *cbgt.Manager) {
	prefix := ""
	if mgr != nil {
		prefix = mgr.Options()["urlPrefix"]
	}

	if phase == "static.before" {
		staticBleveMapping := http.FileServer(bleveMappingUI.AssetFS())

		r.PathPrefix(prefix + "/static-bleve-mapping/").Handler(
			http.StripPrefix(prefix+"/static-bleve-mapping/",
				staticBleveMapping))
	}

	if phase == "manager.after" {
		bleveMappingUI.RegisterHandlers(r, prefix+"/api")

		// Using standard bleveHttp handlers for /api/pindex-bleve endpoints.
		//
		listIndexesHandler := bleveHttp.NewListIndexesHandler()
		r.Handle(prefix+"/api/pindex-bleve",
			listIndexesHandler).Methods("GET")

		getIndexHandler := bleveHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}",
			getIndexHandler).Methods("GET")

		docCountHandler := bleveHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/count",
			docCountHandler).Methods("GET")

		searchHandler := bleveHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/query",
			searchHandler).Methods("POST")

		docGetHandler := bleveHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = rest.PIndexNameLookup
		docGetHandler.DocIDLookup = rest.DocIDLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/doc/{docID}",
			docGetHandler).Methods("GET")

		debugDocHandler := bleveHttp.NewDebugDocumentHandler("")
		debugDocHandler.IndexNameLookup = rest.PIndexNameLookup
		debugDocHandler.DocIDLookup = rest.DocIDLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/docDebug/{docID}",
			debugDocHandler).Methods("GET")

		listFieldsHandler := bleveHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/fields",
			listFieldsHandler).Methods("GET")
	}
}

func BleveMetaExtra(m map[string]interface{}) {
	br := make(map[string]map[string][]string)

	t, i := bleveRegistry.AnalyzerTypesAndInstances()
	br["Analyzer"] = map[string][]string{"types": t, "instances": i}
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

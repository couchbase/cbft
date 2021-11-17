//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	log "github.com/couchbase/clog"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbft/http"
	"github.com/couchbase/cbgt"
)

// CopyPartition is an overridable implementation
// for copying the remote partition contents.
var CopyPartition func(mgr *cbgt.Manager,
	moveReq *CopyPartitionRequest) error

// IsCopyPartitionPreferred is an overridable implementation
// for deciding whether to build a partition from scratch over DCP
// or by copying index partition files from potential remote nodes
// during a rebalance operation.
var IsCopyPartitionPreferred func(mgr *cbgt.Manager,
	pindexName, path, sourceParams string) bool

// IsFeedable is an implementation of cbgt.Feedable interface.
// It returns true if the dest is ready for feed ingestion.
func (t *BleveDest) IsFeedable() (bool, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	if _, ok := t.bindex.(*noopBleveIndex); ok {
		return false, nil
	} else if _, ok := t.bindex.(bleve.Index); ok {
		return true, nil
	}
	return false, fmt.Errorf("pindex_bleve_copy: failed creating bleve"+
		" index for pindex: %s", cbgt.PIndexNameFromPath(t.path))
}

func NewBlevePIndexImplEx(indexType, indexParams, sourceParams, path string,
	mgr *cbgt.Manager, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	pindexName := cbgt.PIndexNameFromPath(path)
	// validate the index params and exit early on errors.
	bleveParams, kvConfig, bleveIndexType, _, err := parseIndexParams(indexParams)
	if err != nil {
		return nil, nil, err
	}

	if CopyPartition == nil || IsCopyPartitionPreferred == nil ||
		bleveIndexType == "upside_down" ||
		!IsCopyPartitionPreferred(mgr, pindexName, path, sourceParams) {
		return NewBlevePIndexImpl(indexType, indexParams, path, restart)
	}

	if path != "" {
		err := os.MkdirAll(path, 0700)
		if err != nil {
			return nil, nil, err
		}
	}

	pathMeta := filepath.Join(path, "PINDEX_BLEVE_META")
	err = ioutil.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		return nil, nil, err
	}

	// create a noop index and wrap that inside the dest.
	noopImpl := &noopBleveIndex{name: pindexName}
	dest := &BleveDest{
		path:           path,
		bleveDocConfig: bleveParams.DocConfig,
		restart:        restart,
		bindex:         noopImpl,
		partitions:     make(map[string]*BleveDestPartition),
		stats:          cbgt.NewPIndexStoreStats(),
		copyStats:      &CopyPartitionStats{},
		stopCh:         make(chan struct{}),
	}
	dest.batchReqChs = make([]chan *batchRequest, asyncBatchWorkerCount)
	destfwd := &cbgt.DestForwarder{DestProvider: dest}

	go tryCopyBleveIndex(indexType, indexParams, path, kvConfig,
		restart, dest, mgr)

	return nil, destfwd, nil
}

// tryCopyBleveIndex tries to copy the pindex files and open it,
// and upon errors falls back to a fresh index creation.
func tryCopyBleveIndex(indexType, indexParams, path string,
	kvConfig map[string]interface{}, restart func(),
	dest *BleveDest, mgr *cbgt.Manager) (err error) {
	pindexName := cbgt.PIndexNameFromPath(path)

	defer func() {
		// fallback to fresh new creation upon errors.
		if err != nil {
			createNewBleveIndex(indexType, indexParams,
				path, restart, dest, mgr)
			return
		}
	}()

	err = copyBleveIndex(pindexName, path, dest, mgr)
	if err != nil {
		return err
	}

	// check whether the dest is already closed.
	if isClosed(dest.stopCh) {
		log.Printf("pindex_bleve_copy: tryCopyBleveIndex pindex: %s"+
			" has already closed", pindexName)
		// It is cleaner to remove the path as there could be corner/racy
		// cases where it is still desirable, for eg: when the copy partition
		// operation performs a rename of the temp download dir to pindex path
		// that might have missed the clean up performed during the dest closure.
		_ = os.RemoveAll(path)
		return nil
	}

	var bindex bleve.Index
	bindex, err = openBleveIndex(path, kvConfig)
	if err != nil {
		return err
	}

	updateBleveIndex(pindexName, mgr, bindex, dest)
	return nil
}

func copyBleveIndex(pindexName, path string, dest *BleveDest,
	mgr *cbgt.Manager) error {
	startTime := time.Now()

	log.Printf("pindex_bleve_copy: pindex: %s, CopyPartition"+
		" started", pindexName)

	// build the remote partition copy request.
	req, err := buildCopyPartitionRequest(pindexName, dest.copyStats,
		mgr, dest.stopCh)
	if req == nil || err != nil {
		log.Printf("pindex_bleve_copy: buildCopyPartitionRequest,"+
			" no source nodes found for partition: %s, err: %v",
			pindexName, err)
		return err
	}

	err = CopyPartition(mgr, req)
	if err != nil {
		log.Printf("pindex_bleve_copy: CopyPartition failed, err: %v", err)
		return err
	}

	log.Printf("pindex_bleve_copy: pindex: %s, CopyPartition"+
		" finished, took: %s", pindexName, time.Since(startTime).String())

	return err
}

func openBleveIndex(path string, kvConfig map[string]interface{}) (
	bleve.Index, error) {
	startTime := time.Now()

	log.Printf("pindex_bleve_copy: start open using: %s", path)
	bindex, err := bleve.OpenUsing(path, kvConfig)
	if err != nil {
		log.Printf("pindex_bleve_copy: bleve.OpenUsing,"+
			" err: %v", err)
		return nil, err
	}

	log.Printf("pindex_bleve_copy: finished open using: %s"+
		" took: %s", path, time.Since(startTime).String())

	return bindex, nil
}

func createNewBleveIndex(indexType, indexParams, path string,
	restart func(), dest *BleveDest, mgr *cbgt.Manager) {
	pindexName := cbgt.PIndexNameFromPath(path)
	// check whether the dest is already closed.
	if isClosed(dest.stopCh) {
		log.Printf("pindex_bleve_copy: createNewBleveIndex pindex: %s"+
			" has already closed", pindexName)
		// It is cleaner to remove the path as there could be corner/racy
		// cases where it is still desirable, for eg: when the copy partition
		// operation performs a rename of the temp download dir to pindex path
		// that might have missed the clean up performed during the dest closure.
		_ = os.RemoveAll(path)
		return
	}

	impl, destWrapper, err := NewBlevePIndexImpl(indexType, indexParams, path, restart)
	if err != nil {
		var ok bool
		var pi *cbgt.PIndex

		// fetch the pindex again to ensure that pindex is still live.
		_, pindexes := mgr.CurrentMaps()
		if pi, ok = pindexes[pindexName]; !ok {
			log.Printf("pindex_bleve_copy: pindex: %s"+
				" no longer exists", pindexName)
			return
		}

		// remove the currently registered noop powered pindex.
		_ = mgr.RemovePIndex(pi)
		mgr.JanitorKick(fmt.Sprintf("restart kick for pindex: %s",
			pindexName))

		return
	}

	// stop the old workers.
	if fwder, ok := destWrapper.(*cbgt.DestForwarder); ok {
		if bdest, ok := fwder.DestProvider.(*BleveDest); ok {
			bdest.stopBatchWorkers()
		}
	}

	// update the new index into the pindex.
	if bindex, ok := impl.(bleve.Index); ok {
		updateBleveIndex(pindexName, mgr, bindex, dest)
	} else {
		log.Errorf("pindex_bleve_copy: no bleve.Index implementation"+
			"found: %s", pindexName)
	}
}

func updateBleveIndex(pindexName string, mgr *cbgt.Manager,
	index bleve.Index, dest *BleveDest) {
	dest.resetBIndex(index)
	dest.startBatchWorkers()
	http.RegisterIndexName(pindexName, index)

	mgr.JanitorKick(fmt.Sprintf("feed init kick for pindex: %s", pindexName))
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

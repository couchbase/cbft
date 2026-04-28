//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build vectors
// +build vectors

package cbft

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
)

func init() {
	workerRegistry = &samplingWorkerRegistry{
		workers: make(map[string]*samplingWorker),
	}
}

const (
	// the minimum number of samples we want per centroid. this multiplied by the
	// number of centroids gives us the minimum number of samples we'd want from
	// a source.
	minSamplesPerCentroid = 39
)

type vectorIndexTrainer struct {
	indexName     string
	partitionName string

	mgr       *cbgt.Manager
	worker    *samplingWorker
	bleveDest *BleveDest
	doneCh    chan struct{}
}

func initTrainer(bleveDest *BleveDest, kvconfig map[string]interface{}) trainer {
	// create the trainer only if the config is enabled with 'vector_index_fast_merge'
	f, ok := kvconfig[scorch.IndexTrainedWithFastMerge]
	if ok {
		if feature, ok := f.(bool); ok && feature {
			if CurrentNodeDefsFetcher == nil || CurrentNodeDefsFetcher.GetManager() == nil {
				log.Errorf("trainer_vector: no manager available to create trainer for"+
					" partition %s", bleveDest.bindex.Name())
				return nil
			}
			mgr := CurrentNodeDefsFetcher.GetManager()

			return &vectorIndexTrainer{
				mgr:       mgr,
				bleveDest: bleveDest,
				doneCh:    make(chan struct{}),
			}
		}
	}
	return nil
}

func (t *vectorIndexTrainer) wait() {
	// this channel is blocked until released at the end of trySampling()
	<-t.doneCh
	log.Printf("trainer_vector: finished sampling documents from KV for "+
		"%s", t.partitionName)
}

func (t *vectorIndexTrainer) close() error {
	// remove the worker from registry - this signifies an index is deleted.
	// even in case of updates, it needs to be cleared out since the collections
	// may have changed
	workerRegistry.m.Lock()
	defer workerRegistry.m.Unlock()
	if t.worker != nil {
		t.worker.ref--
		if t.worker.ref == 0 {
			log.Printf("trainer_vector: closing and remove worker for index %s "+
				"from registry", t.indexName)
			delete(workerRegistry.workers, t.indexName)
		}
	}
	return nil
}

// workerRegistry maps index name to the sampling worker that runs sampling
// and coordinates trained index creation/copy for that index.
var workerRegistry *samplingWorkerRegistry

// samplingWorkerRegistry holds one samplingWorker per index name. Per-node
// vector index count is expected to be low, so a simple map is used.
type samplingWorkerRegistry struct {
	m       sync.Mutex
	workers map[string]*samplingWorker
}

// getOrCreateWorker returns the worker for indexName, creating it if missing.
// The second return is true if the worker already existed (retrieved), false if newly created.
func (r *samplingWorkerRegistry) getOrCreateWorker(indexName string) (*samplingWorker, bool) {
	r.m.Lock()
	defer r.m.Unlock()
	worker, ok := r.workers[indexName]
	if !ok {
		worker = &samplingWorker{
			copyCh: make(chan struct{}),
			ref:    1,
		}
		r.workers[indexName] = worker
		return worker, false
	}
	worker.ref++
	return worker, true
}

// samplingWorker runs sampling for one index with a single goroutine other
// BleveDests for the same index wait on copyCh and then copy the trained index.
type samplingWorker struct {
	samplingWorkerRunConfig

	success bool
	ref     int64
	cv      *sync.Cond

	sampleCh chan *sample
	doneCh   chan struct{} // closed when sampling iteration is finished
	copyCh   chan struct{} // closed when trained index is ready to copy
}

// sample is a single document sampled from KV for vector training.
type sample struct {
	cid   int // collection id
	id    string
	value []byte
}

// samplingWorkerRunConfig holds the inputs for configuring a samplingWorker obj.
type samplingWorkerRunConfig struct {
	vecIndex        bleve.TrainableIndex
	sampleLimit     []int
	collectionNames []string
	scopeName       string
	sourceName      string
	cluster         *gocb.Cluster
}

func (w *samplingWorker) configure(c *samplingWorkerRunConfig) {
	w.vecIndex = c.vecIndex
	w.sampleLimit = c.sampleLimit
	w.collectionNames = c.collectionNames
	w.scopeName = c.scopeName
	w.sourceName = c.sourceName
	w.cluster = c.cluster
	w.cv = sync.NewCond(new(sync.Mutex))
	w.sampleCh = make(chan *sample, 1)
	w.doneCh = make(chan struct{})
}

// run performs the sampling run: opens a sampling scan per collection, reads
// up to sampleLimit[i] documents from each, and sends them on receiveChs.
// When done it closes all receiveChs and doneCh.
func (w *samplingWorker) run() {
	defer func() {
		close(w.sampleCh)
		close(w.doneCh)
	}()

	collectionScanners := make([]*gocb.Collection, len(w.collectionNames))
	for i, collectionName := range w.collectionNames {
		collectionScanners[i] = w.cluster.
			Bucket(w.sourceName).Scope(w.scopeName).Collection(collectionName)
	}

	iterators := make([]*gocb.ScanResult, len(w.collectionNames))
	for i, collectionScanner := range collectionScanners {
		scan := gocb.SamplingScan{
			Limit: uint64(w.sampleLimit[i]),
			Seed:  0, // gocb generates a random seed internally when 0
		}
		var err error
		iterators[i], err = collectionScanner.Scan(scan, nil)
		if err != nil {
			log.Printf("trainer_vector: error opening scan for collection %s: %v",
				w.collectionNames[i], err)
			return
		}
	}

	// Drain each collection's iterator and stream the samples to the bindex
	// for document building + training.
	var wg sync.WaitGroup
	errs := make([]error, len(w.collectionNames))
	for i := range w.collectionNames {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < w.sampleLimit[i]; j++ {
				d := iterators[i].Next()
				if d == nil {
					continue
				}
				var v json.RawMessage
				if err := d.Content(&v); err != nil {
					errs[i] = err
					return
				}
				w.sampleCh <- &sample{
					cid:   i,
					id:    d.ID(),
					value: v,
				}
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			log.Printf("trainer_vector: error sampling collection %s: %v",
				w.collectionNames[i], err)
		}
	}
}

func fetchMemcachedURL(mgr *cbgt.Manager) (string, error) {
	nsServerURL := mgr.Server() + "/pools/default/nodeServices"
	u, err := cbgt.CBAuthURL(nsServerURL)
	if err != nil {
		return "", err
	}

	resp, err := HttpGet(cbgt.HttpClient(), u)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	serverList := struct {
		NodesExt []struct {
			NodeUUID string         `json:"nodeUUID"`
			Services map[string]int `json:"services"`
			ThisNode bool           `json:"thisNode"`
			HostName string         `json:"hostname"`
		} `json:"nodesExt"`
	}{}

	err = json.Unmarshal(respBuf, &serverList)
	if err != nil {
		return "", err
	}
	var rv string
	for _, node := range serverList.NodesExt {
		if node.Services["kv"] > 0 {
			if node.HostName == "" && node.ThisNode {
				mgrURL, err := url.Parse(mgr.Server())
				if err != nil {
					return "", err
				}
				node.HostName = strings.Split(mgrURL.Host, ":")[0]
			}
			rv = strings.Join([]string{node.HostName, strconv.Itoa(node.Services["kv"])}, ":")
			break
		}
	}
	return rv, nil
}

// getClusterAndKVConnection returns a gocb cluster (and optionally gocbcore agent)
// for the current node using manager server URL and cbauth credentials.
// TODO: tighten API and consider reusing connections.
func (t *vectorIndexTrainer) getClusterAndKVConnection(mgr *cbgt.Manager, memcachedHost string) (*gocb.Cluster, *gocbcore.Agent, error) {
	u, err := url.Parse(mgr.Server())
	if err != nil {
		return nil, nil, err
	}
	couchbaseURL := strings.Join([]string{"couchbase://", memcachedHost}, "")
	username, password, err := cbauth.GetMemcachedServiceAuth(u.Host)
	if err != nil {
		return nil, nil, err
	}

	cluster, err := gocb.Connect(couchbaseURL, gocb.ClusterOptions{
		Authenticator: &gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})

	if err != nil {
		return nil, nil, err
	}

	return cluster, nil, err
}

// computeSampleLimitsForSources uses gocbcore stats to get item count per
// collection and returns a sample limit per collection: 4 * sqrt(docCount) * minSamplesPerCentroid.
func computeSampleLimitsForSources(agent *gocbcore.Agent, scopeName string,
	collections []string) (rv []int, err error) {
	rv = make([]int, len(collections))
	signal := make(chan error, 1)

	for i, collectionName := range collections {
		key := fmt.Sprintf("collections %s.%s", scopeName, collectionName)
		var docCount int64
		_, err = agent.Stats(gocbcore.StatsOptions{Key: key},
			func(resp *gocbcore.StatsResult, er error) {
				if resp == nil || er != nil {
					signal <- er
					return
				}

				for _, nodeStat := range resp.Servers {
					for k, v := range nodeStat.Stats {
						if nodeStat.Error != nil {
							continue
						}
						if strings.Contains(k, ":items") {
							c, err := strconv.ParseInt(v, 10, 64)
							if err != nil {
								continue
							}
							docCount += c
						}
					}
				}
				signal <- nil
			})
		if err != nil {
			return nil, err
		}

		err = <-signal
		if err != nil {
			return nil, err
		}
		// Heuristic sample limit: scale with sqrt of collection size.
		rv[i] = int(4 * math.Sqrt(float64(docCount)) * minSamplesPerCentroid)
	}
	return rv, nil
}

// trainOnSamples consumes samples from ch, builds Bleve documents with
// collection scope/UID in extras[sample.cid], indexes them into a training
// batch, and calls vecIndex.Train(batch) when doneCh is closed. returns error
func (t *vectorIndexTrainer) trainOnSamples(ch chan *sample, defaultType string,
	extras [][]byte, vecIndex bleve.TrainableIndex, doneCh chan struct{}) error {
	batch := vecIndex.NewBatch()
	for {
		select {
		case <-doneCh:
			err := vecIndex.Train(batch)
			if err != nil {
				return err
			}
			return nil
		case sample := <-ch:
			if sample != nil {
				doc, key, err := t.bleveDest.bleveDocConfig.BuildDocumentEx(
					[]byte(sample.id), sample.value, defaultType,
					cbgt.DEST_EXTRAS_TYPE_GOCBCORE_SCOPE_COLLECTION, nil, extras[sample.cid])
				if err != nil {
					return err
				}

				err = batch.Index(string(key), doc)
				if err != nil {
					return err
				}
			}
		}
	}
}

// trainedIndexConfig carries everything needed to create and train a trained index.
type trainedIndexConfig struct {
	worker          *samplingWorker
	vecIndex        bleve.TrainableIndex
	defaultType     string
	scopeName       string
	collectionNames []string
}

// createTrainedIndex connects to KV, computes per-collection sample limits,
// configures the worker, and runs 4 goroutines that consume samples from the
// worker's channels and train the vector index. When training finishes, copyCh
// is closed so other BleveDests can copy the trained index.
func (t *vectorIndexTrainer) createTrainedIndex(cfg *trainedIndexConfig) error {
	var err error
	defer func() {
		cfg.worker.success = (err == nil)
		close(cfg.worker.copyCh)
	}()

	log.Printf("trainer_vector: creating trained index in partition: "+
		"%s", t.partitionName)

	memcachedURL, err := fetchMemcachedURL(t.mgr)
	if err != nil {
		return fmt.Errorf("error fetching memcached URL: %w", err)
	}
	cluster, agent, err := t.getClusterAndKVConnection(t.mgr, memcachedURL)
	if err != nil {
		return fmt.Errorf("error getting cluster and KV connection: %w", err)
	}
	// handles closing of the cluster and agent connections
	defer cluster.Close(nil)

	bucket := cluster.Bucket(t.bleveDest.sourceName)
	err = bucket.WaitUntilReady(10*time.Second, &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	})
	if err != nil {
		return err
	}
	agent, err = bucket.Internal().IORouter()
	if err != nil {
		return err
	}

	sampleLimits, err := computeSampleLimitsForSources(agent,
		cfg.scopeName, cfg.collectionNames)
	if err != nil {
		return fmt.Errorf("error getting total source doc count: %w", err)
	}

	manifest, err := GetBucketManifest(t.bleveDest.sourceName)
	if err != nil {
		return err
	}
	scopeUID, collectionUIDs, err := manifest.GetScopeCollectionUIDs(cfg.scopeName, cfg.collectionNames)
	if err != nil {
		return err
	}

	// Encode scope UID and collection UID per collection for document building.
	extraOpts := make([][]byte, len(cfg.collectionNames))
	for i := range cfg.collectionNames {
		extraOpts[i] = make([]byte, 8)
		binary.LittleEndian.PutUint32(extraOpts[i][0:], uint32(scopeUID))
		binary.LittleEndian.PutUint32(extraOpts[i][4:], uint32(collectionUIDs[i]))
	}

	cfg.worker.configure(&samplingWorkerRunConfig{
		vecIndex:        cfg.vecIndex,
		sampleLimit:     sampleLimits,
		collectionNames: cfg.collectionNames,
		scopeName:       cfg.scopeName,
		sourceName:      t.bleveDest.sourceName,
		cluster:         cluster,
	})
	go cfg.worker.run()

	// single training thread
	return t.trainOnSamples(cfg.worker.sampleCh, cfg.defaultType, extraOpts,
		cfg.vecIndex, cfg.worker.doneCh)
}

func (t *vectorIndexTrainer) markTrainingComplete(index bleve.TrainableIndex) error {
	// mark training as complete so that scorch releases its resources
	batch := index.NewBatch()
	batch.SetInternal(util.BoltTrainCompleteKey, []byte("true"))
	err := index.Train(batch)
	if err != nil {
		return fmt.Errorf("error setting train complete flag in bolt: %w", err)
	}
	return nil
}

// isTrained reports whether this partition's trained index is already built
// by reading the internal Bolt train-complete flag.
func (t *vectorIndexTrainer) isTrained() (bool, error) {
	found := false
	val, err := t.bleveDest.bindex.GetInternal(util.BoltTrainCompleteKey)
	if err != nil {
		return false, err
	}

	if val != nil {
		found, err = strconv.ParseBool(string(val))
		if err != nil {
			return false, err
		}
	}

	return found, nil
}

func (t *vectorIndexTrainer) extractIndexNameFromPath(path string) (string, error) {
	partitionName, err := t.mgr.GetPIndexName(filepath.Base(path), false)
	if err != nil {
		return "", err
	}
	t.partitionName = partitionName
	var indexName string
	if x := strings.LastIndex(partitionName, "_"); x > 0 && x < len(partitionName) {
		temp := partitionName[:x]
		if x = strings.LastIndex(temp, "_"); x > 0 && x < len(temp) {
			indexName = temp[:x]
		}
	}
	return indexName, nil
}

// getIndexSourceInfo extracts index name, default type, scope, and collection
// names from the Bleve index mapping (vector type mapping only). Returns an
// error if the index is not a vector index.
func (t *vectorIndexTrainer) getIndexSourceInfo() (string, string, []string, error) {
	defaultType := "_default"
	var collectionNames []string
	var scopeName string
	var err error
	if imi, ok := t.bleveDest.bindex.Mapping().(*mapping.IndexMappingImpl); ok {
		defaultType = imi.DefaultType
		scopeName, collectionNames, _, err = getScopeCollTypeMappings(
			imi, false, vectorTypeMappingFilter)
		if err != nil {
			return "", "", nil, err
		}
	}
	if len(collectionNames) == 0 {
		return "", "", nil, nil
	}

	return defaultType, scopeName, collectionNames, nil
}

// acquireSamples runs the vector sampling flow for this BleveDest. If this
// partition is already trained, it returns. Otherwise it gets or creates the
// index's sampling worker: if this dest creates the worker (first for that
// index), then create the trained index; otherwise wait on worker.copyCh and
// then copy the trained index from the source. Always close doneCh when done.
func (t *vectorIndexTrainer) acquireSamples() {
	var err error
	vecIndex, ok := t.bleveDest.bindex.(bleve.TrainableIndex)
	if !ok {
		err = fmt.Errorf("index doesnt implement bleve.TrainableIndex")
		return
	}
	defer func() {
		if err != nil {
			log.Errorf("trainer_vector: error while sampling vectors from KV: %v", err)
		}
		// mark training as complete and release channel to continue data ingestion
		t.markTrainingComplete(vecIndex)
		close(t.doneCh)
	}()

	// before the partition is registered with the manager, the bindex.Name() is
	// the full path to the partition directory, so we need to handle the extraction
	// carefully
	indexName, err := t.extractIndexNameFromPath(t.bleveDest.bindex.Name())
	if err != nil {
		err = fmt.Errorf("error extracting index name from path: %w", err)
		return
	}
	t.indexName = indexName

	trained, err := t.isTrained()
	if err != nil {
		err = fmt.Errorf("error checking if index is already trained: %w", err)
		return
	}
	if trained {
		log.Printf("trainer_vector: skipping the training phase, since we already"+
			" have a trained index for %s", t.partitionName)
		return
	}

	defaultType, scopeName, collectionNames, err := t.getIndexSourceInfo()
	if err != nil {
		err = fmt.Errorf("error getting the vector index info: %w", err)
		return
	}
	if len(collectionNames) == 0 {
		// not a vector index, return
		return
	}

	worker, ok := workerRegistry.getOrCreateWorker(indexName)
	if !ok {
		t.worker = worker
		// track the index name to clear out the entry during an index delete operation
		err = t.createTrainedIndex(&trainedIndexConfig{
			worker:          worker,
			vecIndex:        vecIndex,
			defaultType:     defaultType,
			scopeName:       scopeName,
			collectionNames: collectionNames,
		})
		if err != nil {
			err = fmt.Errorf("error creating trained index: %w", err)
		}
		return
	}
	if worker == nil {
		return
	}

	t.worker = worker
	// Wait for the source partition to finish building the trained index, then copy it.
	trainedIndexWriter := &trainedIndexWriter{
		rootpath: t.bleveDest.path,
	}
	// the remaining bleve dests will wait here for the trained index to be created,
	// after which they will file transfer it from the source bleveDest
	<-worker.copyCh
	if ic, ok := worker.vecIndex.(bleve.IndexFileCopyable); ok && worker.success {
		dest, ok := t.bleveDest.bindex.(bleve.IndexFileCopyable)
		if !ok {
			err = fmt.Errorf("error getting index file copyable: %w", err)
			return
		}
		trainedIndexWriter.destIndex = dest
		err = ic.CopyFile(index.TrainedIndexFileName, trainedIndexWriter)
		if err != nil {
			err = fmt.Errorf("error copying trained index: %w", err)
			return
		}
		log.Printf("trainer_vector: finished copying trained index from"+
			" source partition %s", worker.vecIndex.Name())
	}

}

// trainedIndexWriter implements the writer used when copying the trained index
// file from the source partition into this partition's index directory.
type trainedIndexWriter struct {
	rootpath  string
	destIndex bleve.IndexFileCopyable
}

// GetWriter returns a file writer for the given path; only trained index file
// is allowed.
func (c *trainedIndexWriter) GetWriter(path string) (io.WriteCloser, error) {
	if !(strings.HasSuffix(path, index.TrainedIndexFileName)) {
		return nil, fmt.Errorf("write not allowed on path %s", path)
	}
	return os.OpenFile(filepath.Join(c.rootpath, path), os.O_CREATE|os.O_WRONLY, 0600)
}

// SetPathInBolt forwards the update to the destination index's bolt store.
func (c *trainedIndexWriter) SetPathInBolt(key []byte, value []byte) error {
	return c.destIndex.SetPathInBolt(key, value)
}

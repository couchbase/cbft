//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"container/heap"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	bleveMappingUI "github.com/blevesearch/bleve-mapping-ui"
	"github.com/blevesearch/bleve/v2"
	_ "github.com/blevesearch/bleve/v2/config"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	bleveRegistry "github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	ftsHttp "github.com/couchbase/cbft/http"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
	log "github.com/couchbase/clog"
)

// Use sync/atomic to access these stats
var BatchBytesAdded uint64
var BatchBytesRemoved uint64
var NumBatchesIntroduced uint64

var TotBatchesFlushedOnMaxOps uint64
var TotBatchesFlushedOnTimer uint64
var TotBatchesNew uint64
var TotBatchesMerged uint64

var TotRollbackPartial uint64
var TotRollbackFull uint64

var featureIndexType = "indexType"
var FeatureScorchIndex = featureIndexType + ":" + scorch.Name

var FeatureCollections = cbgt.SOURCE_GOCBCORE + ":collections"

var FeatureGeoSpatial = "geoSpatial"

var FeatureVectorSearch = "vectors"

var FeatureBlevePreferredSegmentVersion = fmt.Sprintf("segmentVersion:%d", BlevePreferredZapVersion)

var BleveMaxOpsPerBatch = 200 // Unlimited when <= 0.

var BleveBatchFlushDuration = time.Duration(100 * time.Millisecond)

var BleveKVStoreMetricsAllow = false // Use metrics wrapper KVStore by default.

const DefaultBleveMaxClauseCount = 1024

// represents the number of async batch workers per pindex
var asyncBatchWorkerCount = 4 // need to make it configurable,

var TotBleveDestOpened uint64
var TotBleveDestClosed uint64

// BleveDefaultZapVersion represents the default zap version.
// This version is expected to remain a constant as all the
// future indexes are going to have a default segment version.
// Only pre CC indexes are expected to have an empty segment version
// which would be treated like the default zap version.
const BleveDefaultZapVersion = int(11)

// BlevePreferredZapVersion is the recommended zap version for newer indexes.
// This version needs to be bumped to reflect the latest recommended zap
// version in any given release.
const BlevePreferredZapVersion = int(15)

// Preview ZapVersion for indexes that come with vector search support.
// FIXME: This is a temporary placeholder which will be removed once the
// BlevePreferredZapVersion is updated to this. See: MB-59918
const BleveVectorZapVersion = int(16)

var defaultLimitingMinTime = 500
var defaultLimitingMaxTime = 120000

// BleveParams represents the bleve index params.  See also
// cbgt.IndexDef.Params.  A JSON'ified BleveParams looks like...
//
//	{
//	   "mapping": {
//	      // See bleve.mapping.IndexMapping.
//	   },
//	   "store": {
//	      // See BleveParamsStore.
//	   },
//	   "doc_config": {
//	      // See BleveDocumentConfig.
//	   }
//	}
type BleveParams struct {
	Mapping   mapping.IndexMapping   `json:"mapping"`
	Store     map[string]interface{} `json:"store"`
	DocConfig BleveDocumentConfig    `json:"doc_config"`
}

// BleveParamsStore represents some of the publically available
// options in the "store" part of a bleve index params.  See also the
// BleveParams.Store field.
type BleveParamsStore struct {
	// The indexType defaults to bleve.Config.DefaultIndexType.
	// Example: "scorch".  See bleve.index.scorch.Name and
	// bleve.registry.RegisterIndexType().
	IndexType string `json:"indexType"`

	// The kvStoreName defaults to bleve.Config.DefaultKVStore.
	// See also bleve.registry.RegisterKVStore().
	KvStoreName string `json:"kvStoreName"`

	// The kvStoreMetricsAllow flag defaults to
	// cbft.BleveKVStoreMetricsAllow.  When true, an
	// interposing wrapper that captures additional metrics will be
	// initialized as part of a bleve index's KVStore.
	//
	// Note: the interposing metrics wrapper might introduce
	// additional performance costs.
	KvStoreMetricsAllow bool `json:"kvStoreMetricsAllow"`
}

func NewBleveParams() *BleveParams {
	rv := &BleveParams{
		Mapping: bleve.NewIndexMapping(),
		Store: map[string]interface{}{
			"indexType":   bleve.Config.DefaultIndexType,
			"kvStoreName": bleve.Config.DefaultKVStore,
		},
		DocConfig: BleveDocumentConfig{
			Mode:      "type_field",
			TypeField: "type",
		},
	}

	return rv
}

func (sr *SearchRequest) decorateQuery(indexName string, q query.Query,
	cache *collMetaFieldCache) (query.Query, query.Query) {
	var docIDQuery *query.DocIDQuery
	var ok bool
	// bail out early if the query is not a docID one and there are
	// no target collections requested in search request.
	if docIDQuery, ok = q.(*query.DocIDQuery); !ok && len(sr.Collections) == 0 {
		return nil, q
	}
	if cache == nil {
		cache = metaFieldValCache
	}
	// bail out early if the index is a single collection index as there
	// won't be any docID decorations done during indexing as well as the
	// collection scoping during the querying also redundant.
	var sdm *sourceDetails
	if sdm, ok = cache.getSourceDetailsMap(indexName); !ok ||
		len(sdm.collUIDNameMap) <= 1 {
		return nil, q
	}

	// if this is a multi collection index and the query is for docID,
	// then decorate the target docIDs with cuid prefixes.
	if docIDQuery != nil {
		decoratedQuery := *docIDQuery

		hash := make(map[string]struct{})
		for _, cname := range sr.Collections {
			hash[cname] = struct{}{}
		}
		newIDs := make([]string, 0, len(decoratedQuery.IDs))
		for cuid, cname := range sdm.collUIDNameMap {
			if _, ok := hash[cname]; !ok && len(hash) > 0 {
				continue
			}
			cBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(cBytes, cuid)
			for _, docID := range decoratedQuery.IDs {
				newIDs = append(newIDs, string(append(cBytes,
					[]byte(docID)...)))
			}
		}
		decoratedQuery.IDs = newIDs
		return q, &decoratedQuery
	}

	// if the search is scoped to specific collections then add
	// collection specific conjunctions with multi collection indexes.
	cjnq := query.NewConjunctionQuery([]query.Query{q})
	djnq := query.NewDisjunctionQuery(nil)

	for _, col := range sr.Collections {
		queryStr := cache.getMetaFieldValue(indexName, col)
		mq := query.NewMatchQuery(queryStr)
		mq.Analyzer = "keyword"
		mq.SetField(CollMetaFieldName)
		djnq.AddQuery(mq)
	}
	djnq.SetMin(1)
	cjnq.AddQuery(djnq)
	return q, cjnq
}

type SearchRequest struct {
	Q                json.RawMessage         `json:"query"`
	Size             *int                    `json:"size"`
	From             *int                    `json:"from"`
	Highlight        *bleve.HighlightRequest `json:"highlight"`
	Fields           []string                `json:"fields"`
	Facets           bleve.FacetsRequest     `json:"facets"`
	Explain          bool                    `json:"explain"`
	Sort             []json.RawMessage       `json:"sort"`
	IncludeLocations bool                    `json:"includeLocations"`
	Score            string                  `json:"score,omitempty"`
	SearchAfter      []string                `json:"search_after,omitempty"`
	SearchBefore     []string                `json:"search_before,omitempty"`
	Limit            *int                    `json:"limit,omitempty"`
	Offset           *int                    `json:"offset,omitempty"`
	Collections      []string                `json:"collections,omitempty"`
	KNN              json.RawMessage         `json:"knn,omitempty"`
	KNNOperator      json.RawMessage         `json:"knn_operator,omitempty"`
	PreSearchData    json.RawMessage         `json:"pre_search_data,omitempty"`
}

func (sr *SearchRequest) ConvertToBleveSearchRequest() (*bleve.SearchRequest, error) {
	// size/from take precedence, but if not specified, overwrite with
	// limit/offset settings
	r := &bleve.SearchRequest{
		Highlight:        sr.Highlight,
		Fields:           sr.Fields,
		Facets:           sr.Facets,
		Explain:          sr.Explain,
		IncludeLocations: sr.IncludeLocations,
		Score:            sr.Score,
		SearchAfter:      sr.SearchAfter,
		SearchBefore:     sr.SearchBefore,
	}

	var err error
	r.Query, err = query.ParseQuery(sr.Q)
	if err != nil {
		return nil, err
	}
	if sr.PreSearchData != nil {
		r.PreSearchData, err = query.ParsePreSearchData(sr.PreSearchData)
		if err != nil {
			return nil, err
		}
	}

	if sr.Size == nil {
		if sr.Limit == nil || *sr.Limit < 0 {
			r.Size = 10
		} else {
			r.Size = *sr.Limit
		}
	} else if *sr.Size < 0 {
		r.Size = 10
	} else {
		r.Size = *sr.Size
	}

	if sr.From == nil {
		if sr.Offset == nil || *sr.Offset < 0 {
			r.From = 0
		} else {
			r.From = *sr.Offset
		}
	} else if *sr.From < 0 {
		r.From = 0
	} else {
		r.From = *sr.From
	}

	if sr.Sort == nil {
		r.Sort = search.SortOrder{&search.SortScore{Desc: true}}
	} else {
		r.Sort, err = search.ParseSortOrderJSON(sr.Sort)
		if err != nil {
			return nil, err
		}
	}

	if r, err = interpretKNNForRequest(sr.KNN, sr.KNNOperator, r); err != nil {
		return nil, err
	}

	return r, r.Validate()
}

type RollbackInfo struct {
	partition   string
	vBucketUUID uint64
	rollbackSeq uint64
}

type BleveDest struct {
	path       string
	sourceName string

	bleveDocConfig BleveDocumentConfig

	// Invoked when mgr should rollback this BleveDest
	rollback     func()
	rollbackInfo *RollbackInfo

	stats     *cbgt.PIndexStoreStats
	copyStats *CopyPartitionStats

	m                    sync.RWMutex // Protects the fields that follow.
	bindex               bleve.Index
	partitions           map[string]*BleveDestPartition
	rev                  uint64 // Incremented whenever bindex changes.
	batchReqChs          []chan *batchRequest
	stopCh               chan struct{}
	removeCh             chan struct{}
	isRollbackInProgress bool
}

// Used to track state for a single partition.
type BleveDestPartition struct {
	bdest           *BleveDest
	bindex          bleve.Index
	partition       string
	partitionBytes  []byte
	partitionOpaque []byte // Key used to implement OpaqueSet/OpaqueGet().

	seqMax      uint64       // Max seq # we've seen for this partition.
	seqMaxBatch uint64       // Max seq # that got through batch apply/commit.
	lastUUID    atomic.Value // type: string; Cache most recent partition UUID from lastOpaque.

	m                 sync.Mutex   // Protects the fields that follow.
	seqSnapEnd        uint64       // To track snapshot end seq # for this partition.
	osoSnapshot       bool         // Flag to track if current seq # is within an OSO Snapshot.
	osoSeqMax         uint64       // Max seq # received within an OSO Snapshot.
	batch             *bleve.Batch // Batch applied when we hit seqSnapEnd.
	lastOpaque        []byte       // Cache most recent value for OpaqueSet()/OpaqueGet().
	cwrQueue          cbgt.CwrQueue
	lastAsyncBatchErr error // for returning async batch err on next call
}

type batchRequest struct {
	bdp    *BleveDestPartition
	bindex bleve.Index
	batch  *bleve.Batch
	seqMax uint64 // a local copy of seqMax
}

func NewBleveDestEx(path string, bindex bleve.Index,
	rollback func(), bleveDocConfig BleveDocumentConfig, sourceName string) *BleveDest {

	bleveDest := &BleveDest{
		path:           path,
		bleveDocConfig: bleveDocConfig,
		rollback:       rollback,
		bindex:         bindex,
		partitions:     make(map[string]*BleveDestPartition),
		stats:          cbgt.NewPIndexStoreStats(),
		copyStats:      &CopyPartitionStats{},
		stopCh:         make(chan struct{}),
		removeCh:       make(chan struct{}),
		sourceName:     sourceName,
	}

	bleveDest.batchReqChs = make([]chan *batchRequest, asyncBatchWorkerCount)

	bleveDest.startBatchWorkers()

	atomic.AddUint64(&TotBleveDestOpened, 1)

	return bleveDest
}

func NewBleveDest(path string, bindex bleve.Index,
	rollback func(), bleveDocConfig BleveDocumentConfig) *BleveDest {

	bleveDest := &BleveDest{
		path:           path,
		bleveDocConfig: bleveDocConfig,
		rollback:       rollback,
		bindex:         bindex,
		partitions:     make(map[string]*BleveDestPartition),
		stats:          cbgt.NewPIndexStoreStats(),
		copyStats:      &CopyPartitionStats{},
		stopCh:         make(chan struct{}),
		removeCh:       make(chan struct{}),
	}

	bleveDest.batchReqChs = make([]chan *batchRequest, asyncBatchWorkerCount)

	bleveDest.startBatchWorkers()

	atomic.AddUint64(&TotBleveDestOpened, 1)

	return bleveDest
}

func (t *BleveDest) startBatchWorkers() {
	for i := 0; i < asyncBatchWorkerCount; i++ {
		t.batchReqChs[i] = make(chan *batchRequest, 1)
		go runBatchWorker(t.batchReqChs[i], t.stopCh, t.bindex, i)
	}
}

func (t *BleveDest) stopBatchWorkers() {
	t.m.Lock()
	ch := t.stopCh
	t.stopCh = make(chan struct{})
	t.m.Unlock()
	close(ch)
}

// ---------------------------------------------------------

var CurrentNodeDefsFetcher *NodeDefsFetcher

type NodeDefsFetcher struct {
	mgr *cbgt.Manager
}

func (ndf *NodeDefsFetcher) SetManager(mgr *cbgt.Manager) {
	ndf.mgr = mgr
}

func (ndf *NodeDefsFetcher) GetManager() *cbgt.Manager {
	return ndf.mgr
}

func (ndf *NodeDefsFetcher) Get() (*cbgt.NodeDefs, error) {
	if ndf.mgr != nil {
		return ndf.mgr.GetNodeDefs(cbgt.NODE_DEFS_WANTED, true)
	}
	return nil, fmt.Errorf("NodeDefsFetcher Get(): mgr is nil!")
}

// ---------------------------------------------------------

const bleveQueryHelp = `
<a href="https://docs.couchbase.com/server/7.1/fts/fts-supported-queries.html"
   target="_blank">
   query syntax help
</a>
`

func init() {
	cbgt.RegisterPIndexImplType("fulltext-index", &cbgt.PIndexImplType{
		Prepare:  PrepareIndexDef,
		Validate: ValidateBleve,
		OnDelete: OnDeleteIndex,

		New:       NewBlevePIndexImpl,
		NewEx:     NewBlevePIndexImplEx,
		Rollback:  RollbackBleve,
		Open:      OpenBlevePIndexImpl,
		OpenUsing: OpenBlevePIndexImplUsing,

		Count: CountBleve,
		Query: QueryBleve,

		Description: "general/fulltext-index " +
			" - a full text index powered by the bleve engine",
		StartSample:  NewBleveParams(),
		QuerySamples: BleveQuerySamples,
		QueryHelp:    bleveQueryHelp,
		InitRouter:   BleveInitRouter,
		DiagHandlers: []cbgt.DiagHandler{
			{Name: "/api/pindex-bleve", Handler: ftsHttp.NewListIndexesHandler(),
				HandlerFunc: nil},
		},
		MetaExtra: BleveMetaExtra,
		UI: map[string]string{
			"controllerInitName": "blevePIndexInitController",
			"controllerDoneName": "blevePIndexDoneController",
		},
		AnalyzeIndexDefUpdates: RestartOnIndexDefChanges,
		SubmitTaskRequest:      SubmitTaskRequest,
	})

}

func PrepareIndexDef(mgr *cbgt.Manager, indexDef *cbgt.IndexDef) (
	*cbgt.IndexDef, error) {
	if indexDef == nil {
		return nil, cbgt.NewBadRequestError("PrepareIndex, indexDef is nil")
	}

	if CurrentNodeDefsFetcher == nil {
		rv, err := LimitIndexDef(mgr, indexDef)
		if err != nil {
			return rv, cbgt.NewBadRequestError("%v", err)
		}
		return rv, nil
	}

	nodeDefs, err := CurrentNodeDefsFetcher.Get()
	if err != nil {
		return nil, cbgt.NewInternalServerError("PrepareIndex, nodeDefs unavailable: err: %v", err)
	}

	var collectionsSupported, s2SpatialSupported bool
	if isClusterCompatibleFor(FeatureGeoSpatialVersion) {
		s2SpatialSupported =
			cbgt.IsFeatureSupportedByCluster(FeatureGeoSpatial, nodeDefs)

		// check whether the spatial plugin usage is disabled for geo points.
		if v := mgr.GetOption("disableGeoPointSpatialPlugin"); v == "true" {
			s2SpatialSupported = false
		}

		// as its implicit since spatial is already on an advanced version.
		collectionsSupported = true
	} else if isClusterCompatibleFor(FeatureCollectionVersion) {
		collectionsSupported =
			cbgt.IsFeatureSupportedByCluster(FeatureCollections, nodeDefs)
	}

	if collectionsSupported {
		// Use "gocbcore" for DCP streaming if cluster is 7.0+
		indexDef.SourceType = cbgt.SOURCE_GOCBCORE
	}

	var vectorFieldsSpecifiedInMapping bool

	bp := NewBleveParams()
	if len(indexDef.Params) > 0 {
		b, err := bleveMappingUI.CleanseJSON([]byte(indexDef.Params))
		if err != nil {
			return nil, cbgt.NewBadRequestError("PrepareIndex, CleanseJSON err: %v", err)
		}

		err = json.Unmarshal(b, bp)
		if err != nil {
			if typeErr, ok := err.(*json.UnmarshalTypeError); ok {
				if typeErr.Type.String() == "map[string]json.RawMessage" {
					return nil, cbgt.NewBadRequestError("PrepareIndex,"+
						" JSON parse was expecting a string key/field-name"+
						" but instead saw a %s", typeErr.Value)
				}
			}
			return nil, cbgt.NewBadRequestError("bleve: Prepare, err: %v", err)
		}

		if indexType, ok := bp.Store["indexType"].(string); ok {
			if indexType == "scorch" {
				// If indexType were "scorch", the "kvStoreName" setting isn't
				// really applicable, so drop the setting.
				delete(bp.Store, "kvStoreName")

				// insert spatial config only if geopoint field exists in the mapping.
				if s2SpatialSupported && isGeoPointFieldInMapping(bp) {
					bp.Store["spatialPlugin"] = "s2"
				} else {
					delete(bp.Store, "spatialPlugin")
				}
			}
		}

		// figure out the scope/collection details from mappings
		// and perform the validation checks.
		if strings.HasPrefix(bp.DocConfig.Mode, ConfigModeCollPrefix) {
			if !collectionsSupported {
				return nil, cbgt.NewBadRequestError("PrepareIndex, collections not supported" +
					" across all nodes in the cluster")
			}

			if im, ok := bp.Mapping.(*mapping.IndexMappingImpl); ok {
				scope, err := validateScopeCollFromMappings(indexDef.SourceName,
					im, false)
				if err != nil {
					return nil, cbgt.NewBadRequestError("%v", err)
				}

				bucketName, scopeName := getKeyspaceFromScopedIndexName(indexDef.Name)
				if len(bucketName) > 0 && len(scopeName) > 0 {
					if !isClusterCompatibleFor(FeatureScopedIndexNamesVersion) {
						return nil, cbgt.NewBadRequestError("PrepareIndex, scoped indexes NOT" +
							" supported in mixed version cluster")
					}
					if bucketName != indexDef.SourceName || scopeName != scope.Name {
						return nil, cbgt.NewBadRequestError("PrepareIndex, validation of bucket" +
							" and/or scope names against index definition failed")
					}
				}
			}
		} else {
			bucketName, scopeName := getKeyspaceFromScopedIndexName(indexDef.Name)
			if len(bucketName) > 0 && len(scopeName) > 0 {
				if bucketName != indexDef.SourceName || scopeName != "_default" {
					return nil, cbgt.NewBadRequestError("PrepareIndex, changing a scoped index's" +
						" bucket/scope name is NOT allowed")
				}
			}
		}

		vectorFieldsSpecifiedInMapping = vectorFieldsExistWithinIndexMapping(bp.Mapping)
		if !isClusterCompatibleFor(FeatureVectorSearchSupportVersion) ||
			!cbgt.IsFeatureSupportedByCluster(FeatureVectorSearch, nodeDefs) {
			// Vector indexing & search is NOT supported on this cluster
			// (lower version or mixed lower version)
			if vectorFieldsSpecifiedInMapping {
				return nil, cbgt.NewBadRequestError("PrepareIndex, err: vector typed fields " +
					"not supported in mixed version cluster")
			}
		}
	}

	segmentVersionSupported := cbgt.IsFeatureSupportedByCluster(
		FeatureBlevePreferredSegmentVersion, nodeDefs)
	// if segment version is specified then perform the validations.
	if v, ok := bp.Store["segmentVersion"]; ok {
		if zv, ok := v.(float64); ok {
			if !segmentVersionSupported && int(zv) >= BlevePreferredZapVersion {
				// if the cluster isn't advanced enough then err out
				// on latest zap version request for new indexes.
				return nil, cbgt.NewBadRequestError("PrepareIndex, err: zap version %d isn't "+
					"supported in mixed version cluster", int(zv))
			}
			if int(zv) > BleveVectorZapVersion || int(zv) < BleveDefaultZapVersion {
				return nil, cbgt.NewBadRequestError("PrepareIndex, err: zap version %d isn't "+
					"supported", int(zv))
			}

			if vectorFieldsSpecifiedInMapping && int(zv) < BleveVectorZapVersion {
				// overrride segmentVersion to minimum version needed to support vector mappings
				bp.Store["segmentVersion"] = BleveVectorZapVersion
			}
		} else {
			return nil, cbgt.NewBadRequestError("PrepareIndex, err: segmentVersion %v "+
				"should be a numeric value", v)
		}
	} else {
		// if no zap version is specified then assume the preferred
		// zap version for newer indexes in a sufficiently advanced
		// cluster, else consider the default zap version.
		if segmentVersionSupported {
			if vectorFieldsSpecifiedInMapping {
				bp.Store["segmentVersion"] = BleveVectorZapVersion
			} else {
				bp.Store["segmentVersion"] = BlevePreferredZapVersion
			}
		} else {
			bp.Store["segmentVersion"] = BleveDefaultZapVersion
		}
	}

	updatedParams, err := json.Marshal(bp)
	if err != nil {
		return nil, cbgt.NewBadRequestError("PrepareIndex, Marshal err: %v", err)
	}
	indexDef.Params = string(updatedParams)

	if err := checkSourceCompatability(mgr, indexDef.SourceName); err != nil {
		return nil, cbgt.NewInternalServerError("PrepareIndex, err: %v", err)
	}

	rv, err := LimitIndexDef(mgr, indexDef)
	if err != nil {
		return rv, cbgt.NewInternalServerError("%v", err)
	}
	return rv, nil
}

// Utility function check if a "vector" typed field is present within
// the index mapping
func vectorFieldsExistWithinIndexMapping(m mapping.IndexMapping) bool {
	im, ok := m.(*mapping.IndexMappingImpl)
	if !ok {
		// cannot interpret index mapping
		return false
	}

	var vectorFieldExistsWithinDocMapping func(*mapping.DocumentMapping) bool
	vectorFieldExistsWithinDocMapping = func(d *mapping.DocumentMapping) bool {
		if d != nil && d.Enabled {
			for _, v := range d.Properties {
				if vectorFieldExistsWithinDocMapping(v) {
					return true
				}
			}

			for _, field := range d.Fields {
				if field.Type == "vector" {
					return true
				}
			}
		}

		return false
	}

	// Check DefaultMapping
	if vectorFieldExistsWithinDocMapping(im.DefaultMapping) {
		return true
	}

	// Iterate over TypeMapping(s)
	for _, d := range im.TypeMapping {
		if vectorFieldExistsWithinDocMapping(d) {
			return true
		}
	}

	return false
}

func ValidateBleve(indexType, indexName, indexParams string) error {
	if len(indexParams) <= 0 {
		return nil
	}

	validateBleveIndexType := func(content interface{}) error {
		if CurrentNodeDefsFetcher == nil {
			return nil
		}

		nodeDefs, err := CurrentNodeDefsFetcher.Get()
		if err != nil {
			return cbgt.NewInternalServerError("ValidateIndex, nodeDefs unavailable: err: %v", err)
		}

		indexType := ""
		if entries, ok := content.(map[string]interface{}); ok {
			indexType = entries["indexType"].(string)
		}

		// Validate indexType
		if !cbgt.IsFeatureSupportedByCluster(featureIndexType+":"+indexType, nodeDefs) {
			return cbgt.NewBadRequestError("ValidateIndex, index validation failed:"+
				" indexType: %v not supported on all nodes in"+
				" cluster", indexType)
		}

		return nil
	}

	// Validate token filters in indexParams
	validateIndexParams := func() error {
		var iParams map[string]interface{}
		err := json.Unmarshal([]byte(indexParams), &iParams)
		if err != nil {
			// Ignore the JSON unmarshalling error, if in the case
			// indexParams isn't JSON.
			return nil
		}

		store, found := iParams["store"]
		if found {
			err = validateBleveIndexType(store)
			if err != nil {
				return err
			}
		}

		mapping, found := iParams["mapping"]
		if !found {
			// Entry for mapping not found
			return nil
		}

		analysis, found := mapping.(map[string]interface{})["analysis"]
		if !found {
			// No sub entry with the name analysis within mapping
			return nil
		}

		tokenfilters, found := analysis.(map[string]interface{})["token_filters"]
		if !found {
			// No entry for token_filters within mapping/analysis
			return nil
		}

		for _, val := range tokenfilters.(map[string]interface{}) {
			param := val.(map[string]interface{})
			switch param["type"] {
			case "edge_ngram", "length", "ngram", "shingle":
				if param["min"].(float64) > param["max"].(float64) {
					return cbgt.NewBadRequestError("bleve: token_filter validation failed"+
						" for %v => min(%v) > max(%v)", param["type"],
						param["min"], param["max"])
				}
			case "truncate_token":
				if param["length"].(float64) < 0 {
					return cbgt.NewBadRequestError("bleve: token_filter validation failed"+
						" for %v => length(%v) < 0", param["type"], param["length"])
				}
			default:
				break
			}
		}

		return nil
	}

	err := validateIndexParams()
	if err != nil {
		return cbgt.NewBadRequestError("ValidateIndex, err: %v", err)
	}

	b, err := bleveMappingUI.CleanseJSON([]byte(indexParams))
	if err != nil {
		return cbgt.NewBadRequestError("ValidateIndex, CleanseJSON err: %v", err)
	}

	bp := NewBleveParams()

	err = json.Unmarshal(b, bp)
	if err != nil {
		if typeErr, ok := err.(*json.UnmarshalTypeError); ok {
			if typeErr.Type.String() == "map[string]json.RawMessage" {
				return cbgt.NewBadRequestError("ValidateIndex, Params:"+
					" JSON parse was expecting a string key/field-name"+
					" but instead saw a %s", typeErr.Value)
			}
		}

		return cbgt.NewBadRequestError("ValidateIndex, Params err: %v", err)
	}

	// err out if there are no active type mapping.
	if im, ok := bp.Mapping.(*mapping.IndexMappingImpl); ok {
		var found bool
		for tp, dm := range im.TypeMapping {
			if !dm.Enabled || len(tp) == 0 {
				continue
			}
			found = true
			break
		}
		if !im.DefaultMapping.Enabled && !found {
			return cbgt.NewBadRequestError("ValidateIndex, Params: no valid type mappings found")
		}
	}

	err = bp.Mapping.Validate()
	if err != nil {
		return cbgt.NewBadRequestError("ValidateIndex, Mapping err: %v", err)
	}

	return nil
}

func OnDeleteIndex(indexDef *cbgt.IndexDef) {
	if indexDef == nil {
		return
	}

	// Reset focusStats of the index
	for k := range queryPaths {
		if indexQueryPathStats, exists := MapRESTPathStats[k]; exists {
			indexQueryPathStats.ResetFocusStats(indexDef.Name)
		}
	}

	// Reset gRPC focusStats of the index
	GrpcPathStats.ResetFocusStats(indexDef.Name)

	// Reset the metaFieldValCache
	metaFieldValCache.reset(indexDef.Name)

	// Reset QuerySupervisor's entry for last_access_time
	querySupervisor.deleteEntryForIndex(indexDef.Name)

	// Clear out source partition seqs cache for this index
	DropSourcePartitionSeqs(indexDef.SourceName, indexDef.SourceUUID)

	// Refresh the regulator stats. For eg, Deletion of a bucket leads
	// to index deletions, in which case the stale regulator stats
	// needs to be removed.
	RefreshRegulatorStats()

	// When deleting an index, remove the corresponding bucket from the manifest cache.
	// If other indexes still use the same bucket, the cache will be refreshed as needed.
	// Otherwise, unnecessary monitoring of the bucket can be avoided.
	removeBucketFromManifestCache(indexDef.SourceName)
}

func parseIndexParams(indexParams string) (
	bleveParams *BleveParams, kvConfig map[string]interface{},
	bleveIndexType string, kvStoreName string, err error) {
	var ip cbgt.IndexPrepParams
	err = json.Unmarshal([]byte(indexParams), &ip)
	if err != nil {
		return nil, nil, "", "",
			fmt.Errorf("bleve: new index, json marshal"+
				" err: %v", err)
	}

	indexParams = ip.Params
	bleveParams = NewBleveParams()
	if len(indexParams) > 0 {
		buf, err := bleveMappingUI.CleanseJSON([]byte(indexParams))
		if err != nil {
			return nil, nil, "", "",
				fmt.Errorf("bleve: cleanse params, err: %v", err)
		}

		err = json.Unmarshal(buf, bleveParams)
		if err != nil {
			return nil, nil, "", "",
				fmt.Errorf("bleve: parse params, err: %v", err)
		}
	}

	if strings.HasPrefix(bleveParams.DocConfig.Mode, ConfigModeCollPrefix) {
		if im, ok := bleveParams.Mapping.(*mapping.IndexMappingImpl); ok {
			scope, err := validateScopeCollFromMappings(ip.SourceName,
				im, false)
			if err != nil {
				return nil, nil, "", "", err
			}
			// if there are more than 1 collection then need to
			// insert $scope#$collection field into the mappings
			if multiCollection(scope.Collections) {
				err = enhanceMappingsWithCollMetaField(im.TypeMapping)
				if err != nil {
					return nil, nil, "", "", err
				}
				ipBytes, err := json.Marshal(bleveParams)
				if err != nil {
					return nil, nil, "", "",
						fmt.Errorf("bleve: new , json marshal,"+
							" err: %v", err)
				}
				indexParams = string(ipBytes)
			}

			bleveParams.DocConfig.CollPrefixLookup =
				initMetaFieldValCache(ip.IndexName, ip.SourceName, im, scope)
		}
	}

	kvConfig, bleveIndexType, kvStoreName = bleveRuntimeConfigMap(bleveParams)
	return bleveParams, kvConfig, bleveIndexType, kvStoreName, nil
}

func NewBlevePIndexImpl(indexType, indexParams, path string,
	rollback func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	bleveParams, kvConfig, bleveIndexType, kvStoreName, err :=
		parseIndexParams(indexParams)
	if err != nil {
		return nil, nil, err
	}

	if bleveIndexType == "upside_down" {
		return nil, nil, fmt.Errorf("bleve: new index, path: %s,"+
			" uses index type upside_down which is no longer supported", path)
	}

	bindex, err := bleve.NewUsing(path, bleveParams.Mapping,
		bleveIndexType, kvStoreName, kvConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: new index, path: %s,"+
			" kvStoreName: %s, kvConfig: %#v, err: %s",
			path, kvStoreName, kvConfig, err)
	}

	pathMeta := path + string(os.PathSeparator) + "PINDEX_BLEVE_META"
	err = os.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		return nil, nil, err
	}
	tmp := struct {
		SourceName string `json:"sourceName"`
	}{}

	err = json.Unmarshal([]byte(indexParams), &tmp)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: parse params: %v", err)
	}
	return bindex, &cbgt.DestForwarder{
		DestProvider: NewBleveDestEx(path, bindex, rollback, bleveParams.DocConfig,
			tmp.SourceName),
	}, nil
}

func RollbackBleve(indexType, indexParams, sourceParams, path string,
	mgr *cbgt.Manager, rollback func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	bleveParams, kvConfig, bleveIndexType, kvStoreName, err :=
		parseIndexParams(indexParams)
	if err != nil {
		return nil, nil, err
	}

	if bleveIndexType == "upside_down" {
		return nil, nil, fmt.Errorf("bleve: new index, path: %s,"+
			" uses index type upside_down which is no longer supported", path)
	}

	pindexName := cbgt.PIndexNameFromPath(path)

	pathMeta := path + string(os.PathSeparator) + "PINDEX_BLEVE_META"
	err = os.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		return nil, nil, err
	}

	dest := newNoOpBleveDest(pindexName, path, bleveParams, rollback)
	destfwd := &cbgt.DestForwarder{DestProvider: dest}

	go func() {
		// Check if the pindex is closed before building it.
		if isClosed(dest.stopCh) {
			// Check if the pindex still exists.
			_, pindexes := mgr.CurrentMaps()
			pi, ok := pindexes[pindexName]
			if !ok {
				log.Printf("pindex_bleve: pindex: %s"+
					" no longer exists", pindexName)
				return
			}

			// Remove the pindex if it's still live.
			// No need to create a new bleve index if the underlying
			// pindex isn't live.
			_ = mgr.RemovePIndex(pi)
			return
		}

		bindex, err := bleve.NewUsing(path, bleveParams.Mapping,
			bleveIndexType, kvStoreName, kvConfig)
		if err != nil {
			log.Errorf("bleve: new index, path: %s,"+
				" kvStoreName: %s, kvConfig: %#v, err: %s",
				path, kvStoreName, kvConfig, err)

			// If there's an error creating the bleve index,
			// remove the directory and attempt to re-rollback.
			_ = os.RemoveAll(path)

			_, pindexes := mgr.CurrentMaps()
			pi, ok := pindexes[pindexName]
			if !ok {
				log.Printf("pindex_bleve: pindex: %s"+
					" no longer exists", pindexName)
				return
			}

			// Will be a full rollback since the path was removed.
			// As part of rollback, the pindex will be closed
			mgr.JanitorRollbackKick("rollback:"+pi.Name, pi)

			return
		}

		updateBleveIndex(pindexName, mgr, bindex, dest)
	}()

	return nil, destfwd, nil
}

// To be called ONLY when docConfig.Mode has "scope.collection" prefix
func initMetaFieldValCache(indexName, sourceName string,
	im *mapping.IndexMappingImpl, scope *Scope) map[uint32]*collMetaField {
	if im == nil {
		return nil
	}
	rv := make(map[uint32]*collMetaField, 1)
	// clean up any old data
	metaFieldValCache.reset(indexName)

	multiCollIndex := multiCollection(scope.Collections)
	for _, coll := range scope.Collections {
		cuid, err := strconv.ParseInt(coll.Uid, 16, 32)
		if err != nil {
			return nil
		}
		suid, err := strconv.ParseInt(scope.Uid, 16, 32)
		if err != nil {
			return nil
		}
		if cmf, ok := rv[uint32(cuid)]; !ok {
			rv[uint32(cuid)] = &collMetaField{
				scopeDotColl: scope.Name + "." + coll.Name,
				typeMappings: []string{coll.typeMapping},
				value:        encodeCollMetaFieldValue(suid, cuid),
			}
		} else {
			cmf.typeMappings = append(cmf.typeMappings, coll.typeMapping)
		}

		metaFieldValCache.setValue(indexName, scope.Name, suid,
			coll.Name, cuid, multiCollIndex)
	}
	return rv
}

func OpenBlevePIndexImpl(indexType, path string,
	rollback func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	return OpenBlevePIndexImplUsing(indexType, path, "", rollback)
}

func bleveRuntimeConfigMap(bleveParams *BleveParams) (map[string]interface{},
	string, string) {
	// check the indexType
	bleveIndexType, ok := bleveParams.Store["indexType"].(string)
	if !ok || bleveIndexType == "" {
		bleveIndexType = bleve.Config.DefaultIndexType
	}

	kvConfig := map[string]interface{}{
		"create_if_missing":        true,
		"error_if_exists":          true,
		"unsafe_batch":             true,
		"eventCallbackName":        "scorchEventCallbacks",
		"asyncErrorCallbackName":   "scorchAsyncErrorCallbacks",
		"numSnapshotsToKeep":       3,
		"rollbackSamplingInterval": "10m",
		"forceSegmentType":         "zap",
		"bolt_timeout":             "30s",
		// enable_concurrency will be added here.
	}
	for k, v := range bleveParams.Store {
		if k == "segmentVersion" {
			kvConfig["forceSegmentVersion"] = v
			continue
		}
		kvConfig[k] = v
	}

	if bleveIndexType != "scorch" {
		log.Warnf("unsupported index type: %s, overwriting to scorch", bleveIndexType)
		bleveIndexType = "scorch"
	}
	kvStoreName := "scorch"
	kvConfig["kvStoreName"] = "scorch"

	return kvConfig, bleveIndexType, kvStoreName
}

func OpenBlevePIndexImplUsing(indexType, path, indexParams string,
	rollback func()) (cbgt.PIndexImpl, cbgt.Dest, error) {
	buf := []byte(indexParams)
	var err error
	if len(buf) == 0 {
		buf, err = os.ReadFile(path +
			string(os.PathSeparator) + "PINDEX_BLEVE_META")
		if err != nil {
			return nil, nil, err
		}
	}

	bleveParams := NewBleveParams()
	if len(buf) > 0 {
		// It is possible that buf is empty when index params aren't set as
		// part of the index definition.
		buf, err = bleveMappingUI.CleanseJSON(buf)
		if err != nil {
			return nil, nil, fmt.Errorf("bleve: cleanse params, err: %v", err)
		}

		err = json.Unmarshal(buf, bleveParams)
		if err != nil {
			return nil, nil, fmt.Errorf("bleve: parse params: %v", err)
		}
	}

	if strings.HasPrefix(bleveParams.DocConfig.Mode, ConfigModeCollPrefix) {
		if am, ok := bleveParams.Mapping.(*mapping.IndexMappingImpl); ok {
			buf, err = os.ReadFile(path +
				string(os.PathSeparator) + "PINDEX_META")
			if err != nil {
				return nil, nil, err
			}
			tmp := struct {
				SourceName string `json:"sourceName"`
				IndexName  string `json:"indexName"`
			}{}

			err = json.Unmarshal(buf, &tmp)
			if err != nil {
				return nil, nil, fmt.Errorf("bleve: parse params: %v", err)
			}
			scope, err := validateScopeCollFromMappings(tmp.SourceName,
				am, false)
			if err != nil {
				return nil, nil, fmt.Errorf("bleve: validate scope/collection: %v", err)
			}
			// populate the collection meta field look up cache.
			bleveParams.DocConfig.CollPrefixLookup =
				initMetaFieldValCache(tmp.IndexName, tmp.SourceName, am, scope)
		}
	}

	kvConfig, _, _ := bleveRuntimeConfigMap(bleveParams)
	// TODO: boltdb sometimes locks on Open(), so need to investigate,
	// where perhaps there was a previous missing or race-y Close().
	startTime := time.Now()
	log.Printf("bleve: start open using: %s", path)
	bindex, err := bleve.OpenUsing(path, kvConfig)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("bleve: finished open using: %s took: %s", path, time.Since(startTime).String())

	buf, err = os.ReadFile(path +
		string(os.PathSeparator) + "PINDEX_META")
	if err != nil {
		return nil, nil, err
	}
	tmp := struct {
		SourceName string `json:"sourceName"`
		IndexName  string `json:"indexName"`
	}{}

	err = json.Unmarshal(buf, &tmp)
	if err != nil {
		return nil, nil, fmt.Errorf("bleve: parse params: %v", err)
	}

	return bindex, &cbgt.DestForwarder{
		DestProvider: NewBleveDestEx(path, bindex, rollback, bleveParams.DocConfig,
			tmp.SourceName),
	}, nil
}

// ---------------------------------------------------------------

func CountBleve(mgr *cbgt.Manager, indexName, indexUUID string) (
	uint64, error) {
	alias, _, _, err := bleveIndexAlias(mgr, indexName, indexUUID, false, nil, nil,
		false, nil, "", addIndexClients)
	if err != nil {
		if _, ok := err.(*cbgt.ErrorLocalPIndexHealth); !ok {
			return 0, fmt.Errorf("bleve: CountBleve indexAlias error,"+
				" indexName: %s, indexUUID: %s, err: %v", indexName, indexUUID, err)
		}
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

// SubmitTaskRequest helps requesting for asynchronous tasks on
// indexes like force merge, cancel merge etc.
func SubmitTaskRequest(mgr *cbgt.Manager, indexName, indexUUID string,
	requestBody []byte) (*cbgt.TaskRequestStatus, error) {
	var reqMap map[string]interface{}
	err := UnmarshalJSON(requestBody, &reqMap)
	if err != nil {
		return nil, fmt.Errorf("bleve: SubmitTaskRequest"+
			" parsing request, err: %v", err)
	}
	var onlyPIndexes map[string]bool
	var targetPIndexes []string
	if vals, ok := reqMap["partitionNames"].([]interface{}); ok {
		for _, v := range vals {
			targetPIndexes = append(targetPIndexes, v.(string))
		}
		onlyPIndexes = cbgt.StringsToMap(targetPIndexes)
	}

	var op string
	var ok bool
	if op, ok = reqMap["op"].(string); !ok ||
		(op != "merge" && op != "cancel" && op != "get") {
		return nil, fmt.Errorf("bleve: SubmitTaskRequest"+
			" unsupported task type: %s", op)
	}

	var uuid string
	if uuid, ok = reqMap["uuid"].(string); !ok && op == "cancel" {
		return nil, fmt.Errorf("bleve: SubmitTaskRequest" +
			" missing task uuid from cancel request")
	}

	// uuid for tracking the task for cancellations or potential
	// progress monitoring etc
	if uuid == "" && op != "get" {
		uuid = cbgt.NewUUID()
		reqMap["uuid"] = uuid
		requestBody, err = json.Marshal(reqMap)
		if err != nil {
			return nil, err
		}
	}

	alias, _, _, err1 := bleveIndexAlias(mgr, indexName,
		indexUUID, true, nil, nil, true,
		onlyPIndexes, "", addIndexClients)
	if err1 != nil {
		if _, ok := err1.(*cbgt.ErrorLocalPIndexHealth); !ok {
			return nil, err1
		}
	}

	if bic, ok := alias.(BleveIndexCollector); ok {
		return cbgt.ScatterTaskRequest(requestBody, indexPartitions(bic))
	}

	return nil, fmt.Errorf("bleve: no BleveIndexCollector implementation found")
}

func indexPartitions(bic BleveIndexCollector) (handlers []cbgt.TaskRequestHandler) {
	gatherHandlers := func(i bleve.Index) {
		if h, ok := i.(cbgt.TaskRequestHandler); ok {
			handlers = append(handlers, h)
		}
	}
	bic.VisitIndexes(gatherHandlers)
	return handlers
}

// ---------------------------------------------------------
// Query Error Counters, use atomic/Sync* for access
var (
	// totQueryRejectOnNotEnoughQuota tracks the number of rejected
	// search requests on hitting the memory threshold for query
	totQueryRejectOnNotEnoughQuota uint64

	// bleve.IndexAlias's SearchInContext() failed
	totQuerySearchInContextErr uint64
	// requested query could not not be parsed, bad syntax
	totQueryBadRequestErr uint64
	// requested query could not meet consistency requirements
	totQueryConsistencyErr uint64
	// (Query.From + Query.Size) exceeded bleveMaxResultWindow
	totQueryMaxResultWindowExceededErr uint64
	// requested query could not be executed by some pindex(es)
	totQueryPartialResultsErr uint64
)

// number of search requests that carry a kNN request within them
// (only recorded at coordinator node - where req was received)
var numKNNSearchRequests uint64

// QueryPIndexes defines the part of the JSON query request that
// allows the client to specify which pindexes the server should
// consider during query processing.
type QueryPIndexes struct {
	// An empty or nil PIndexNames means the query should use all
	// the pindexes of the index.
	PIndexNames []string `json:"pindexNames,omitempty"`
}

func fireQueryEvent(depth int, kind QueryEventKind, dur time.Duration, size uint64) error {
	if RegistryQueryEventCallback != nil {
		return RegistryQueryEventCallback(depth, QueryEvent{Kind: kind, Duration: dur}, size)
	}
	return nil
}

// forcing the searcher creation to app_herder checks so that
// if there is high memory usage we don't do an expensive searcher creation
func bleveCtxSearcherStartCallback(size uint64) error {
	return fireQueryEvent(0, EventQueryStart, 0, size)
}

func bleveCtxSearcherEndCallback(size uint64) error {
	return fireQueryEvent(0, EventQueryEnd, 0, size)
}

func bleveCtxQueryStartCallback(size uint64) error {
	return fireQueryEvent(1, EventQueryStart, 0, size)
}

func bleveCtxQueryEndCallback(size uint64) error {
	return fireQueryEvent(1, EventQueryEnd, 0, size)
}

func QueryBleve(mgr *cbgt.Manager, indexName, indexUUID string,
	req []byte, res io.Writer) error {
	// phase 0 - parsing/validating query
	// could return err 400
	queryCtlParams := cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
		},
	}

	err := UnmarshalJSON(req, &queryCtlParams)
	if err != nil {
		atomic.AddUint64(&totQueryBadRequestErr, 1)
		return fmt.Errorf("bleve: QueryBleve"+
			" parsing queryCtlParams, err: %v", err)
	}

	queryPIndexes := QueryPIndexes{}
	err = UnmarshalJSON(req, &queryPIndexes)
	if err != nil {
		atomic.AddUint64(&totQueryBadRequestErr, 1)
		return fmt.Errorf("bleve: QueryBleve"+
			" parsing queryPIndexes, err: %v", err)
	}

	var sr *SearchRequest
	err = UnmarshalJSON(req, &sr)
	if err != nil {
		atomic.AddUint64(&totQueryBadRequestErr, 1)
		return fmt.Errorf("bleve: QueryBleve"+
			" parsing searchRequest, err: %v", err)
	}
	searchRequest, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		atomic.AddUint64(&totQueryBadRequestErr, 1)
		return fmt.Errorf("bleve: QueryBleve"+
			" parsing searchRequest, err: %v", err)
	}

	if sr.KNN != nil {
		atomic.AddUint64(&numKNNSearchRequests, 1)
	}

	var undecoratedQuery query.Query
	// pre process the query with collections if applicable.
	if strings.Compare(cbgt.CfgAppVersion, "7.0.0") >= 0 {
		undecoratedQuery, searchRequest.Query = sr.decorateQuery(indexName,
			searchRequest.Query, nil)
	}

	if queryCtlParams.Ctl.Consistency != nil {
		err = ValidateConsistencyParams(queryCtlParams.Ctl.Consistency)
		if err != nil {
			atomic.AddUint64(&totQueryConsistencyErr, 1)
			return fmt.Errorf("bleve: QueryBleve"+
				" validating consistency, err: %v", err)
		}
	}

	if v := mgr.GetOption("bleveMaxResultWindow"); len(v) > 0 {
		var bleveMaxResultWindow int
		bleveMaxResultWindow, err = strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("bleve: QueryBleve atoi: %v, err: %v", v, err)
		}

		if (searchRequest.From+searchRequest.Size > bleveMaxResultWindow) ||
			(searchRequest.Size > bleveMaxResultWindow &&
				(searchRequest.SearchAfter != nil || searchRequest.SearchBefore != nil)) {
			atomic.AddUint64(&totQueryMaxResultWindowExceededErr, 1)
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

	var onlyPIndexes map[string]bool
	if len(queryPIndexes.PIndexNames) > 0 {
		onlyPIndexes = cbgt.StringsToMap(queryPIndexes.PIndexNames)
	}

	alias, remoteClients, numPIndexes, err1 := bleveIndexAlias(mgr, indexName,
		indexUUID, true, queryCtlParams.Ctl.Consistency, cancelCh, true,
		onlyPIndexes, queryCtlParams.Ctl.PartitionSelection, getRemoteClients(mgr))
	if err1 != nil {
		if _, ok := err1.(*cbgt.ErrorLocalPIndexHealth); !ok {
			return err1
		}
	}

	// estimate memory needed for merging search results from all
	// the pindexes
	mergeEstimate := uint64(numPIndexes) * bleve.MemoryNeededForSearchResult(searchRequest)
	err = fireQueryEvent(0, EventQueryStart, 0, mergeEstimate)
	if err != nil {
		atomic.AddUint64(&totQueryRejectOnNotEnoughQuota, 1)
		return err
	}

	defer fireQueryEvent(0, EventQueryEnd, 0, mergeEstimate)

	// set query start/end callbacks
	ctx = context.WithValue(ctx, bleve.SearchQueryStartCallbackKey,
		bleve.SearchQueryStartCallbackFn(bleveCtxQueryStartCallback))
	ctx = context.WithValue(ctx, bleve.SearchQueryEndCallbackKey,
		bleve.SearchQueryEndCallbackFn(bleveCtxQueryEndCallback))

	// set the callbacks for searcher creation
	ctx = context.WithValue(ctx, search.SearcherStartCallbackKey,
		search.SearcherStartCallbackFn(bleveCtxSearcherStartCallback))
	ctx = context.WithValue(ctx, search.SearcherEndCallbackKey,
		search.SearcherEndCallbackFn(bleveCtxSearcherEndCallback))

	// register with the QuerySupervisor
	id := querySupervisor.AddEntry(&QuerySupervisorContext{
		Query:     searchRequest.Query,
		Cancel:    cancel,
		Size:      searchRequest.Size,
		From:      searchRequest.From,
		Timeout:   queryCtlParams.Ctl.Timeout,
		IndexName: indexName,
	})

	defer querySupervisor.DeleteEntry(id)

	searchResult, err := alias.SearchInContext(ctx, searchRequest)
	if err != nil {
		atomic.AddUint64(&totQuerySearchInContextErr, 1)
	}
	if searchResult != nil {
		// if the query decoration happens for collection targeted or docID
		// queries for multi collection indexes, then restore the original
		// user query in the search response, if the search request was echo'd
		// back in the search result.
		// Note: searchResult.Request will be non nil only when searchRequest.Explain is true
		// and its a bleve level setting
		if undecoratedQuery != nil && searchResult.Request != nil {
			searchResult.Request.Query = undecoratedQuery
		}
		err = processSearchResult(&queryCtlParams, indexName, searchResult,
			remoteClients, err, err1)

		if searchResult.Status != nil &&
			len(searchResult.Status.Errors) > 0 &&
			queryCtlParams.Ctl.Consistency != nil &&
			queryCtlParams.Ctl.Consistency.Results == "complete" {
			atomic.AddUint64(&totQueryPartialResultsErr, 1)

			// complete results expected, do not propagate partial results
			return fmt.Errorf("bleve: results weren't retrieved from some"+
				" index partitions: %d", len(searchResult.Status.Errors))
		}

		mustEncode(res, searchResult)

		// update return error status to indicate any errors within the
		// search result that was already propagated as response.
		if searchResult.Status != nil && len(searchResult.Status.Errors) > 0 {
			err = rest.ErrorAlreadyPropagated
		}
	}

	return err
}

func processSearchResult(queryCtlParams *cbgt.QueryCtlParams, indexName string,
	searchResult *bleve.SearchResult, remoteClients []RemoteClient,
	searchErr, aliasErr error) error {
	if searchResult != nil {
		if len(searchResult.Hits) > 0 {
			// if this is a multi collection index, then strip the collection UID
			// from the hit ID and fill the details of source collection
			if sdm, multiCollIndex :=
				metaFieldValCache.getSourceDetailsMap(indexName); multiCollIndex {
				for _, hit := range searchResult.Hits {
					if _, exists := hit.Fields["_$c"]; exists {
						// collection name has already been retrieved for this hit;
						// on the non-coordinating node (scatter-gather)
						continue
					}
					idBytes := []byte(hit.ID)
					cuid := binary.LittleEndian.Uint32(idBytes[:4])
					if collName, ok := sdm.collUIDNameMap[cuid]; ok {
						hit.ID = string(idBytes[4:])
						if hit.Fields == nil {
							hit.Fields = make(map[string]interface{})
						}
						hit.Fields["_$c"] = collName
					}
				}
			}
		}

		// check to see if any of the remote searches returned anything
		// other than 0, 200, 412, 429, these are returned to the user as
		// error status 400, and appear as phase 0 errors detected late.
		// 0 means we never heard anything back, and that is dealt with
		// in the following section
		for _, remoteClient := range remoteClients {
			lastStatus, lastErrBody := remoteClient.GetLast()
			if lastStatus == http.StatusTooManyRequests {
				log.Printf("bleve: remoteClient: %s query reject, statusCode: %d,"+
					" err: %v", remoteClient.GetHostPort(), lastStatus, searchErr)
				continue
			}
			if lastStatus != http.StatusOK &&
				lastStatus != http.StatusPreconditionFailed &&
				lastStatus != 0 {
				return fmt.Errorf("bleve: QueryBleve remote client"+
					" returned status: %d body: %s", lastStatus, lastErrBody)
			}
		}
		// now see if any of the remote searches returned 412; these should be
		// collated into a single 412 response at this level and will
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
				err := UnmarshalJSON(lastErrBody, &remoteConsistencyErr)
				if err == nil {
					for k, v := range remoteConsistencyErr.StartEndSeqs {
						remoteConsistencyWaitError.StartEndSeqs[k] = v
					}
				}
			}
		}
		// if we had any explicitly returned consistency errors, return those
		if len(remoteConsistencyWaitError.StartEndSeqs) > 0 {
			atomic.AddUint64(&totQueryConsistencyErr, 1)
			return &remoteConsistencyWaitError
		}

		// we had *some* consistency requirements, but we never heard back
		// from some of the remote pindexes; just punt for now and return
		// a mostly empty 412 indicating we aren't sure
		if queryCtlParams.Ctl.Consistency != nil &&
			len(queryCtlParams.Ctl.Consistency.Vectors) > 0 &&
			numRemoteSilent > 0 {
			atomic.AddUint64(&totQueryConsistencyErr, 1)
			return &remoteConsistencyWaitError
		}

		if aliasErr != nil {
			if err2, ok := aliasErr.(*cbgt.ErrorLocalPIndexHealth); ok && len(err2.IndexErrMap) > 0 {
				// populate the searchResuls with the details of
				// pindexes not searched/covered in this query.
				if searchResult.Status.Errors == nil {
					searchResult.Status.Errors = make(map[string]error)
				}
				for pi, e := range err2.IndexErrMap {
					searchResult.Status.Errors[pi] = e
					searchResult.Status.Failed++
					searchResult.Status.Total++
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------

func (t *BleveDest) resetBIndex(bindex bleve.Index) {
	t.m.Lock()
	t.bindex = bindex
	t.m.Unlock()
}

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
			partitionBytes:  []byte(partition),
			partitionOpaque: []byte("o:" + partition),
			batch:           t.bindex.NewBatch(),
			cwrQueue:        cbgt.CwrQueue{},
		}
		heap.Init(&bdp.cwrQueue)

		t.partitions[partition] = bdp
	}

	return bdp, nil
}

// ---------------------------------------------------------

func (t *BleveDest) Close(remove bool) error {
	t.m.Lock()
	err := t.closeLOCKED(remove)
	t.m.Unlock()
	return err
}

func (t *BleveDest) removeLOCKED() {
	if _, ok := t.bindex.(*noopBleveIndex); ok {
		if !isClosed(t.removeCh) {
			close(t.removeCh)
		}
	} else {
		os.RemoveAll(t.path)
	}
}

func (t *BleveDest) closeLOCKED(remove bool) error {
	if t.bindex == nil {
		return nil // Already closed.
	}

	atomic.AddUint64(&TotBleveDestClosed, 1)

	close(t.stopCh)

	partitions := t.partitions
	t.partitions = make(map[string]*BleveDestPartition)

	t.bindex.Close()
	if remove {
		t.removeLOCKED()
	}
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

	uuid, _ := bdp.lastUUID.Load().(string)
	seq := atomic.LoadUint64(&bdp.seqMaxBatch)

	bdp.m.Lock()
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
			return atomic.LoadUint64(&bdp.seqMaxBatch)
		})
}

// ---------------------------------------------------------

func (t *BleveDest) Count(pindex *cbgt.PIndex, cancelCh <-chan bool) (
	uint64, error) {
	t.m.RLock()
	defer t.m.RUnlock()

	if t.bindex == nil {
		return 0, fmt.Errorf("bleve: Count, bindex already closed")
	}

	return t.bindex.DocCount()
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
	err := UnmarshalJSON(req, &queryCtlParams)
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query"+
			" parsing queryCtlParams, err: %v", err)
	}

	var sr *SearchRequest
	err = UnmarshalJSON(req, &sr)
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query"+
			" parsing searchRequest, err: %v", err)
	}
	searchRequest, err := sr.ConvertToBleveSearchRequest()
	if err != nil {
		return fmt.Errorf("bleve: BleveDest.Query"+
			" parsing searchRequest, err: %v", err)
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
				sendSearchResultErr(searchRequest, res, []string{pindex.Name}, ctx.Err())
				return nil
			}
		}
		// some other error occurred return this as 400
		return err
	}

	// phase 2 - execute query
	// always 200, possibly with errors inside status
	t.m.RLock()
	bindex := t.bindex
	t.m.RUnlock()

	if bindex == nil {
		err = fmt.Errorf("bleve: Query, bindex already closed")
		sendSearchResultErr(searchRequest, res, []string{pindex.Name}, err)
		return nil
	}

	// register with the QuerySupervisor
	id := querySupervisor.AddEntry(&QuerySupervisorContext{
		Query:   searchRequest.Query,
		Cancel:  cancel,
		Size:    searchRequest.Size,
		From:    searchRequest.From,
		Timeout: queryCtlParams.Ctl.Timeout,
	})

	defer querySupervisor.DeleteEntry(id)

	searchResponse, err := bindex.SearchInContext(ctx, searchRequest)
	if err != nil {
		sendSearchResultErr(searchRequest, res, []string{pindex.Name}, err)
		return nil
	}

	rest.MustEncode(res, searchResponse)
	return nil
}

// ---------------------------------------------------------

func sendSearchResultErr(req *bleve.SearchRequest, res io.Writer,
	pindexNames []string, err error) {
	rest.MustEncode(res, makeSearchResultErr(req, pindexNames, err))
}

func makeSearchResultErr(req *bleve.SearchRequest,
	pindexNames []string, err error) *bleve.SearchResult {
	rv := &bleve.SearchResult{
		Request: req,
		Status: &bleve.SearchStatus{
			Total:      len(pindexNames),
			Failed:     len(pindexNames),
			Successful: 0,
			Errors:     make(map[string]error),
		},
	}
	for _, pindexName := range pindexNames {
		rv.Status.Errors[pindexName] = err
	}
	return rv
}

// ---------------------------------------------------------

func setupContextAndCancelCh(queryCtlParams cbgt.QueryCtlParams,
	parentCancelCh <-chan bool) (ctx context.Context, cancel context.CancelFunc,
	cancelChRv <-chan bool) {
	if queryCtlParams.Ctl.Timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(),
			time.Duration(queryCtlParams.Ctl.Timeout)*time.Millisecond)
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
	// avoid log flooding from non-json inputs in bucket
	if !strings.HasPrefix(op, "json") {
		log.Printf("bleve: %s, partition: %s, key: %q, seq: %d,"+
			" err: %v", op, partition, log.Tag(log.UserData, key), seq, err)
	}

	e := struct {
		Time      string
		Op        string
		Partition string
		Key       string
		Seq       uint64
		Err       string
	}{
		Time:      time.Now().Format(time.RFC3339Nano),
		Op:        op,
		Partition: partition,
		Key:       string(key),
		Seq:       seq,
		Err:       fmt.Sprintf("%v", err),
	}

	buf, err := json.Marshal(&e)
	if err == nil {
		t.stats.AddError(string(buf))
	}
}

// ---------------------------------------------------------

type JSONStatsWriter interface {
	WriteJSON(w io.Writer) error
}

var prefixPIndexStoreStats = []byte(`{"pindexStoreStats":`)

var prefixCopyPartitionStats = []byte(`,"copyPartitionStats":`)

func (t *BleveDest) Stats(w io.Writer) (err error) {
	var vbstats, verbose bool
	var indexDef *cbgt.IndexDef
	var sourcePartitionSeqs map[string]cbgt.UUIDSeq
	if w, ok := w.(rest.PartitionStatsWriter); ok {
		vbstats = w.VbStats()
		verbose = w.Verbose()
		indexDef = w.IndexDef()
		sourcePartitionSeqs = w.SourcePartitionSeqs()
	}

	// exit early if all details are disabled.
	if !verbose && !vbstats {
		return
	}

	// if verbose stats is requested then send most of the index stats.
	if verbose {
		_, err = w.Write(prefixPIndexStoreStats)
		if err != nil {
			return
		}

		t.stats.WriteJSON(w)

		var statsMap map[string]interface{}
		var docCount uint64
		t.m.RLock()
		if t.bindex != nil {
			statsMap = t.bindex.StatsMap()
			docCount, err = t.bindex.DocCount()
		}
		t.m.RUnlock()

		if err != nil {
			return
		}

		if statsMap != nil {
			_, err = w.Write([]byte(`,"bleveIndexStats":`))
			if err != nil {
				return
			}
			var idxStatsJSON []byte
			idxStatsJSON, err = MarshalJSON(statsMap)
			if err != nil {
				log.Errorf("json failed to marshal was: %#v", statsMap)
				return
			}
			_, err = w.Write(idxStatsJSON)
			if err != nil {
				return
			}
		}

		_, err = w.Write([]byte(`,"basic":{"DocCount":` + strconv.FormatUint(docCount, 10)))
		if err != nil {
			return
		}
		_, err = w.Write(cbgt.JsonCloseBrace)
		if err != nil {
			return
		}

		// skip the vbucket stats if vbstats is not requested.
		if !vbstats {
			_, _ = w.Write(cbgt.JsonCloseBrace)
			return
		}

		_, err = w.Write([]byte(`,`))
		if err != nil {
			return
		}
	} else {
		// in case only vbstats are requested.
		_, err = w.Write(cbgt.JsonOpenBrace)
		if err != nil {
			return
		}
	}

	_, err = w.Write([]byte(`"partitions":{`))
	if err != nil {
		return
	}

	// obtain scope, collection names
	scope, collections, _ := GetScopeCollectionsFromIndexDef(indexDef)

	t.m.RLock()
	partitionSeqs := make([][]byte, len(t.partitions))
	i := 0
	for partition, bdp := range t.partitions {
		bdpSeqMax := atomic.LoadUint64(&bdp.seqMax)
		bdpSeqMaxBatch := atomic.LoadUint64(&bdp.seqMaxBatch)
		bdpLastUUID, _ := bdp.lastUUID.Load().(string)

		partitionSeq := partition +
			`":{"seq":` + strconv.FormatUint(bdpSeqMaxBatch, 10) +
			`,"seqReceived":` + strconv.FormatUint(bdpSeqMax, 10)

		if len(scope) > 0 && len(collections) > 0 && sourcePartitionSeqs != nil {
			var highSeq uint64
			// determine which collection holds the highest sequence number
			for _, coll := range collections {
				if uuidSeq, exists :=
					sourcePartitionSeqs[partition+":"+scope+":"+coll]; exists {
					if highSeq < uuidSeq.Seq {
						highSeq = uuidSeq.Seq
					}
				}
			}
			partitionSeq += `,"sourceSeq":` + strconv.FormatUint(highSeq, 10)
		}

		partitionSeq += `,"uuid":"` + bdpLastUUID + `"}`

		partitionSeqs[i] = []byte(partitionSeq)

		i++
	}
	t.m.RUnlock()

	for i, partitionSeq := range partitionSeqs {
		if i == 0 {
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

		_, err = w.Write(partitionSeq)
		if err != nil {
			return
		}
	}
	_, err = w.Write(cbgt.JsonCloseBrace)
	if err != nil {
		return
	}

	_, err = w.Write(prefixCopyPartitionStats)
	if err != nil {
		return err
	}

	t.copyStats.WriteJSON(w)

	_, err = w.Write(cbgt.JsonCloseBrace)
	if err != nil {
		return
	}

	return nil
}

func (t *BleveDest) StatsMap() (rv map[string]interface{}, err error) {
	rv = make(map[string]interface{})

	t.m.RLock()
	defer t.m.RUnlock()

	if t.bindex != nil {
		rv["bleveIndexStats"] = t.bindex.StatsMap()
		var c uint64
		c, err = t.bindex.DocCount()
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
	t.m.RLock()
	for partition, bdp := range t.partitions {
		bdpSeqMaxBatch := atomic.LoadUint64(&bdp.seqMaxBatch)
		bdpLastUUID, _ := bdp.lastUUID.Load().(string)

		rv[partition] = cbgt.UUIDSeq{
			UUID: bdpLastUUID,
			Seq:  bdpSeqMaxBatch,
		}
	}
	t.m.RUnlock()

	return rv, nil
}

// ---------------------------------------------------------

func (t *BleveDestPartition) Close(remove bool) error {
	return t.bdest.Close(remove)
}

func (t *BleveDestPartition) PrepareFeedParams(partition string,
	params *cbgt.DCPFeedParams) error {
	// nothing to be done for bucket based indexes.
	if !strings.HasPrefix(t.bdest.bleveDocConfig.Mode, ConfigModeCollPrefix) {
		return nil
	}
	// if already set, then return early.
	if params != nil && params.Scope != "" && len(params.Collections) > 0 {
		return nil
	}

	buf, err := os.ReadFile(t.bdest.path +
		string(os.PathSeparator) + "PINDEX_META")
	if err != nil {
		return err
	}
	if len(buf) == 0 {
		return fmt.Errorf("bleve: empty PINDEX_META contents")
	}

	in := struct {
		IndexParams string `json:"indexParams"`
		SourceName  string `json:"sourceName"`
	}{}
	err = json.Unmarshal(buf, &in)
	if err != nil {
		return fmt.Errorf("bleve: parse params: %v", err)
	}

	tmp := struct {
		Mapping mapping.IndexMapping `json:"mapping"`
	}{Mapping: bleve.NewIndexMapping()}

	err = json.Unmarshal([]byte(in.IndexParams), &tmp)
	if err != nil {
		return fmt.Errorf("bleve: parse params: %v", err)
	}

	if im, ok := tmp.Mapping.(*mapping.IndexMappingImpl); ok {
		scope, err := validateScopeCollFromMappings(in.SourceName,
			im, true)
		if err != nil {
			return err
		}

		if scope != nil && len(scope.Collections) > 0 {
			params.Scope = scope.Name
			uniqueCollections := map[string]struct{}{}
			for _, coll := range scope.Collections {
				if _, exists := uniqueCollections[coll.Name]; !exists {
					uniqueCollections[coll.Name] = struct{}{}
					params.Collections = append(params.Collections, coll.Name)
				}
			}
		}
	}

	return nil
}

func (t *BleveDestPartition) DataUpdate(partition string,
	key []byte, seq uint64, val []byte, cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	atomic.AddUint64(&aggregateBDPStats.TotDataUpdateBeg, 1)

	t.m.Lock()
	if t.batch == nil {
		t.m.Unlock()
		atomic.AddUint64(&aggregateBDPStats.TotDataUpdateEnd, 1)
		return fmt.Errorf("bleve: DataUpdate nil batch")
	}

	defaultType := "_default"
	if imi, ok := t.bindex.Mapping().(*mapping.IndexMappingImpl); ok {
		defaultType = imi.DefaultType
	}

	cbftDoc, key, errv := t.bdest.bleveDocConfig.BuildDocumentEx(key, val,
		defaultType, extrasType, extras)

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

	atomic.AddUint64(&aggregateBDPStats.TotDataUpdateEnd, 1)
	return err
}

func (t *BleveDestPartition) DataDelete(partition string,
	key []byte, seq uint64,
	cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	atomic.AddUint64(&aggregateBDPStats.TotDataDeleteBeg, 1)

	t.m.Lock()

	if t.batch == nil {
		t.m.Unlock()
		atomic.AddUint64(&aggregateBDPStats.TotDataDeleteEnd, 1)
		return fmt.Errorf("bleve: DataDelete nil batch")
	}

	// need to apply the key decoration with multicollection indexes.
	if t.bdest.bleveDocConfig.multiCollection() {
		key = append(extras[4:8], key...)
	}

	t.batch.Delete(string(key)) // TODO: string(key) makes garbage?

	revNeedsUpdate, err := t.updateSeqLOCKED(seq)

	t.m.Unlock()

	if err == nil && revNeedsUpdate {
		t.incRev()
	}

	atomic.AddUint64(&aggregateBDPStats.TotDataDeleteEnd, 1)
	return err
}

// ---------------------------------------------------------

const osoSnapshotStart = uint32(1)
const osoSnapshotEnd = uint32(2)

func (t *BleveDestPartition) OSOSnapshot(partition string,
	snapshotType uint32) error {
	if snapshotType == osoSnapshotStart {
		t.m.Lock()
		t.osoSnapshot = true
		t.osoSeqMax = atomic.LoadUint64(&t.seqMax)
		revNeedsUpdate, err := t.submitAsyncBatchRequestLOCKED()
		t.m.Unlock()
		if err == nil && revNeedsUpdate {
			t.incRev()
		}

		return err
	} else if snapshotType == osoSnapshotEnd {
		t.m.Lock()
		t.osoSnapshot = false
		// When the OSO snapshot end message is received, update seqMax with
		// the max seq received while in the OSO snapshot and flush the batch.
		if atomic.LoadUint64(&t.seqMax) < t.osoSeqMax {
			atomic.StoreUint64(&t.seqMax, t.osoSeqMax)
		}
		revNeedsUpdate, err := t.submitAsyncBatchRequestLOCKED()
		t.m.Unlock()
		if err == nil && revNeedsUpdate {
			t.incRev()
		}

		return err
	}

	return fmt.Errorf("bleve: OSOSnapshot unknown snapshotType: %v",
		snapshotType)
}

func (t *BleveDestPartition) SeqNoAdvanced(partition string, seq uint64) error {
	// Can be received when the feed is subscribed to collections;
	// This message is to be viewed as a snapshot END message, indicating that
	// no other sequence numbers will be received in that snapshot;
	// Also, this seqno is to be used in the event of re-connection.
	t.m.Lock()
	revNeedsUpdate, err := t.updateSeqLOCKED(seq)
	t.m.Unlock()
	if err == nil && revNeedsUpdate {
		t.incRev()
	}

	return err
}

func (t *BleveDestPartition) CreateCollection(partition string,
	manifestUid uint64, scopeId, collecitonId uint32, seq uint64) error {
	t.m.Lock()
	revNeedsUpdate, err := t.updateSeqLOCKED(seq)
	t.m.Unlock()
	if err == nil && revNeedsUpdate {
		t.incRev()
	}

	return err
}

func (t *BleveDestPartition) DeleteCollection(partition string,
	manifestUid uint64, scopeId, collecitonId uint32, seq uint64) error {
	// NO-OP
	return nil
}

func (t *BleveDestPartition) FlushCollection(partition string,
	manifestUid uint64, scopeId, collecitonId uint32, seq uint64) error {
	// NO-OP
	return nil
}

func (t *BleveDestPartition) ModifyCollection(partition string,
	manifestUid uint64, scopeId, collecitonId uint32, seq uint64) error {
	// NO-OP
	return nil
}

// ---------------------------------------------------------

type bleveDestPartitionStats struct {
	TotDataUpdateBeg uint64
	TotDataUpdateEnd uint64

	TotDataDeleteBeg uint64
	TotDataDeleteEnd uint64

	TotExecuteBatchBeg uint64
	TotExecuteBatchEnd uint64
}

var aggregateBDPStats bleveDestPartitionStats

func AggregateBleveDestPartitionStats() map[string]interface{} {
	return map[string]interface{}{
		"TotDataUpdateBeg": atomic.LoadUint64(&aggregateBDPStats.TotDataUpdateBeg),
		"TotDataUpdateEnd": atomic.LoadUint64(&aggregateBDPStats.TotDataUpdateEnd),

		"TotDataDeleteBeg": atomic.LoadUint64(&aggregateBDPStats.TotDataDeleteBeg),
		"TotDataDeleteEnd": atomic.LoadUint64(&aggregateBDPStats.TotDataDeleteEnd),

		"TotExecuteBatchBeg": atomic.LoadUint64(&aggregateBDPStats.TotExecuteBatchBeg),
		"TotExecuteBatchEnd": atomic.LoadUint64(&aggregateBDPStats.TotExecuteBatchEnd),
	}
}

// ---------------------------------------------------------

func (t *BleveDestPartition) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	t.m.Lock()
	t.osoSnapshot = false
	revNeedsUpdate, err := t.submitAsyncBatchRequestLOCKED()
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
		t.lastUUID.Store(cbgt.ParseOpaqueToUUID(value))
	}

	lastOpaque := append([]byte(nil), t.lastOpaque...) // Another copy of value.

	if atomic.LoadUint64(&t.seqMax) <= 0 {
		// TODO: Need way to control memory alloc during GetInternal(),
		// perhaps with optional memory allocator func() parameter?
		buf, err := t.bindex.GetInternal(t.partitionBytes)
		if err != nil {
			t.m.Unlock()
			return nil, 0, err
		}
		if len(buf) <= 0 {
			t.m.Unlock()
			return lastOpaque, 0, nil // No seqMax buf is a valid case.
		}
		if len(buf) != 8 {
			t.m.Unlock()
			return nil, 0, fmt.Errorf("bleve: unexpected size for seqMax bytes")
		}

		atomic.StoreUint64(&t.seqMax, binary.BigEndian.Uint64(buf[0:8]))
		atomic.CompareAndSwapUint64(&t.seqMaxBatch, 0, atomic.LoadUint64(&t.seqMax))
	}

	lastSeq := atomic.LoadUint64(&t.seqMax)
	lastCommittedSeq := atomic.LoadUint64(&t.seqMaxBatch)
	if lastSeq > lastCommittedSeq {
		lastSeq = lastCommittedSeq
	}

	t.m.Unlock()

	return lastOpaque, lastSeq, nil
}

func (t *BleveDestPartition) OpaqueSet(partition string, value []byte) error {
	t.m.Lock()

	if t.batch == nil {
		t.m.Unlock()
		return fmt.Errorf("bleve: OpaqueSet nil batch")
	}

	t.lastOpaque = append(t.lastOpaque[0:0], value...)
	t.lastUUID.Store(cbgt.ParseOpaqueToUUID(value))

	anotherCopy := append([]byte(nil), value...)
	t.batch.SetInternal(t.partitionOpaque, anotherCopy)

	t.m.Unlock()
	return nil
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
	if t.osoSnapshot {
		if t.osoSeqMax < seq {
			t.osoSeqMax = seq
		}
	} else if atomic.LoadUint64(&t.seqMax) < seq {
		atomic.StoreUint64(&t.seqMax, seq)
	}

	if (t.osoSnapshot || seq < t.seqSnapEnd) &&
		(BleveMaxOpsPerBatch <= 0 || BleveMaxOpsPerBatch > t.batch.Size()) {
		return false, t.lastAsyncBatchErr
	}

	return t.submitAsyncBatchRequestLOCKED()
}

func (t *BleveDestPartition) submitAsyncBatchRequestLOCKED() (bool, error) {
	// fetch the needed parameters and remain unlocked until requestCh
	// is ready to accommodate this request
	bindex := t.bindex
	seqMax := atomic.LoadUint64(&t.seqMax)
	seqMaxBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(seqMaxBuf, seqMax)
	t.batch.SetInternal(t.partitionBytes, seqMaxBuf)
	batch := t.batch
	t.batch = t.bindex.NewBatch()
	p := t.partition
	batchReqChs := t.bdest.batchReqChs
	stopCh := t.bdest.stopCh
	t.m.Unlock()

	// ensure that batch requests from a given partition always goes
	// to the same worker queue so that the order of seq numbers are maintained
	partition, err := strconv.Atoi(p)
	if err != nil {
		log.Errorf("pindex_bleve: submitAsyncBatchRequestLOCKED over `%v`, err: %v",
			bindex.Name(), err)
		t.m.Lock()
		return false, err
	}

	reqChIndex := partition % asyncBatchWorkerCount
	br := &batchRequest{bdp: t, bindex: bindex,
		batch: batch, seqMax: seqMax,
	}
	select {
	case <-stopCh:
		log.Printf("pindex_bleve: submitAsyncBatchRequestLOCKED stopped for `%v`",
			bindex.Name())
		t.m.Lock()
		return false, t.lastAsyncBatchErr

	case batchReqChs[reqChIndex] <- br:
	}

	// acquire lock
	t.m.Lock()
	return false, t.lastAsyncBatchErr
}

func (t *BleveDestPartition) setLastAsyncBatchErr(err error) {
	t.m.Lock()
	t.lastAsyncBatchErr = err
	t.m.Unlock()
}

func runBatchWorker(requestCh chan *batchRequest, stopCh chan struct{},
	bindex bleve.Index, workerID int) {
	var targetBatch *bleve.Batch
	bdp := make([]*BleveDestPartition, 0, 50)
	bdpMaxSeqNums := make([]uint64, 0, 50)
	var ticker *time.Ticker
	batchFlushDuration := BleveBatchFlushDuration

	if batchFlushDuration > 0 {
		ticker = time.NewTicker(batchFlushDuration)
		defer ticker.Stop()
	}
	var tickerCh <-chan time.Time

	log.Printf("pindex_bleve: started runBatchWorker: %d for pindex: %s", workerID, bindex.Name())

	for {
		// trigger batch execution if we have enough items in batch
		if targetBatch != nil && targetBatch.Size() >= BleveMaxOpsPerBatch {
			executeBatch(bdp, bdpMaxSeqNums, bindex, targetBatch)
			targetBatch = nil
			atomic.AddUint64(&TotBatchesFlushedOnMaxOps, 1)
		}

		// wait for more mutations for a bigger target batch
		if targetBatch != nil && ticker != nil {
			tickerCh = ticker.C
		}

		select {
		case batchReq := <-requestCh:
			if batchReq == nil {
				log.Printf("pindex_bleve: batchWorker stopped, batchReq: nil")
				return
			}
			if batchReq.bdp == nil || batchReq.bindex == nil {
				break
			}

			// if batch merging is disabled then execute the batch
			if batchFlushDuration == 0 {
				bdp = bdp[:0]
				bdpMaxSeqNums = bdpMaxSeqNums[:0]
				batchReq.bdp.m.Lock()
				bdp = append(bdp, batchReq.bdp)
				bdpMaxSeqNums = append(bdpMaxSeqNums, batchReq.seqMax)
				batchReq.bdp.m.Unlock()
				executeBatch(bdp, bdpMaxSeqNums, batchReq.bindex, batchReq.batch)
				break
			}

			if targetBatch == nil {
				bdp = bdp[:0]
				bdpMaxSeqNums = bdpMaxSeqNums[:0]
				batchReq.bdp.m.Lock()
				bdp = append(bdp, batchReq.bdp)
				bdpMaxSeqNums = append(bdpMaxSeqNums, batchReq.seqMax)
				batchReq.bdp.m.Unlock()
				bindex = batchReq.bindex
				targetBatch = batchReq.batch
				atomic.AddUint64(&TotBatchesNew, 1)
				break
			}

			targetBatch.Merge(batchReq.batch)
			atomic.AddUint64(&TotBatchesMerged, 1)
			batchReq.bdp.m.Lock()
			bdp = append(bdp, batchReq.bdp)
			bdpMaxSeqNums = append(bdpMaxSeqNums, batchReq.seqMax)
			batchReq.bdp.m.Unlock()

		case <-tickerCh:
			if targetBatch != nil {
				executeBatch(bdp, bdpMaxSeqNums, bindex, targetBatch)
				targetBatch = nil
				atomic.AddUint64(&TotBatchesFlushedOnTimer, 1)
			}
			tickerCh = nil

		case <-stopCh:
			log.Printf("pindex_bleve: batchWorker %d stopped for `%v`", workerID, bindex.Name())
			return
		}

	}
}

func executeBatch(bdp []*BleveDestPartition, bdpMaxSeqNums []uint64,
	index bleve.Index, batch *bleve.Batch) {

	// perform a regulator check and then the execute() in case of
	// serverless mode. If that is successful, perform the corresponding
	// metering.
	regulateAndExecute(bdp, bdpMaxSeqNums, index, batch)
}

func regulateAndExecute(bdp []*BleveDestPartition, bdpMaxSeqNums []uint64,
	index bleve.Index, batch *bleve.Batch) {

	// The metering and checkQuota calls must happen on
	// indexes with valid sources, i.e. their dests should contain
	// valid sourceName. So, in scenarios like rebalance and hibernation
	// when there is a no-op bleve dest involved, those operations
	//  shouldn't happen.
	if !ServerlessMode || bdp[0].bdest.sourceName == "" {
		_, err := execute(bdp, bdpMaxSeqNums, index, batch)
		if err != nil {
			bdp[0].setLastAsyncBatchErr(err)
		}
		return
	}

	result, _, err := CheckQuotaWrite(bdp[0].bdest.stopCh,
		bdp[0].bdest.sourceName, "", true, index)

	// NOTE: At this point, it's guaranteed that the action is either
	// CheckResultError (which is the case when the bdp's bleve index is closed)
	// or CheckResultNormal, in which case we continue the execution
	if result == CheckResultError {
		log.Error(err)
		return
	}

	_, err = execute(bdp, bdpMaxSeqNums, index, batch)
	if err != nil {
		bdp[0].setLastAsyncBatchErr(err)
		return
	}

	// NOTE: each partition is going to fetch its bleve index's
	// bytes written to disk, and meter this to its local node
	// in the cluster.
	MeterWrites(bdp[0].bdest.stopCh, bdp[0].bdest.sourceName, index)
}

func execute(bdp []*BleveDestPartition, bdpMaxSeqNums []uint64,
	bindex bleve.Index, batch *bleve.Batch) (bool, error) {
	if batch == nil {
		return false, fmt.Errorf("pindex_bleve: executeBatch batch nil")
	}

	if bindex == nil {
		return false, fmt.Errorf("pindex_bleve: executeBatch bindex already closed")
	}

	batchTotalDocsSize := batch.TotalDocsSize()
	atomic.AddUint64(&BatchBytesAdded, batchTotalDocsSize)

	err := cbgt.Timer(func() error {
		atomic.AddUint64(&aggregateBDPStats.TotExecuteBatchBeg, 1)
		err := bindex.Batch(batch)
		atomic.AddUint64(&aggregateBDPStats.TotExecuteBatchEnd, 1)
		if err != nil && err != bleve.ErrorIndexClosed {
			log.Errorf("pindex_bleve: executeBatch over `%v`, err: %+v ",
				bindex.Name(), err)
		}
		return err
	}, bdp[0].bdest.stats.TimerBatchStore)

	if err != nil {
		return false, err
	}

	atomic.AddUint64(&NumBatchesIntroduced, 1)
	atomic.AddUint64(&BatchBytesRemoved, batchTotalDocsSize)

	for i, t := range bdp {
		t.m.Lock()
		if bdpMaxSeqNums[i] > atomic.LoadUint64(&t.seqMaxBatch) {
			atomic.StoreUint64(&t.seqMaxBatch, bdpMaxSeqNums[i])
		}
		for t.cwrQueue.Len() > 0 &&
			t.cwrQueue[0].ConsistencySeq <= atomic.LoadUint64(&t.seqMaxBatch) {
			cwr := heap.Pop(&t.cwrQueue).(*cbgt.ConsistencyWaitReq)
			if cwr != nil && cwr.DoneCh != nil {
				close(cwr.DoneCh)
			}
		}
		t.m.Unlock()
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

// Atomic counters that keep track of the number of times http and http-ssl
// were used for scatter gather over remote pindexes.
var totRemoteHttp uint64
var totRemoteHttpSsl uint64

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
	cancelCh <-chan bool, groupByNode bool, onlyPIndexes map[string]bool,
	partitionSelection string, rcAdder addRemoteClients) (
	bleve.IndexAlias, []RemoteClient, int, error) {

	alias := bleve.NewIndexAlias()

	remoteClients, numPIndexes, err := bleveIndexTargets(mgr, indexName, indexUUID,
		ensureCanRead, consistencyParams, cancelCh,
		groupByNode, onlyPIndexes, alias, partitionSelection, rcAdder)
	if err != nil {
		if _, ok := err.(*cbgt.ErrorLocalPIndexHealth); ok {
			return alias, remoteClients, numPIndexes, err
		}
		return nil, nil, 0, err
	}

	return alias, remoteClients, numPIndexes, nil
}

// BleveIndexCollector interface is a subset of the bleve.IndexAlias
// interface, with just the Add() method, allowing alternative
// implementations that need to collect "backend" bleve indexes based
// on a user defined index.
type BleveIndexCollector interface {
	Add(i ...bleve.Index)
	VisitIndexes(func(bleve.Index))
}

func addIndexClients(mgr *cbgt.Manager, indexName, indexUUID string,
	remotePlanPIndexes []*cbgt.RemotePlanPIndex, consistencyParams *cbgt.ConsistencyParams,
	onlyPIndexes map[string]bool, collector BleveIndexCollector,
	groupByNode bool) ([]RemoteClient, error) {
	prefix := mgr.GetOption("urlPrefix")
	remoteClients := make([]*IndexClient, 0, len(remotePlanPIndexes))
	rv := make([]RemoteClient, 0, len(remotePlanPIndexes))
	ss := cbgt.GetSecuritySetting()
	for _, remotePlanPIndex := range remotePlanPIndexes {
		if (onlyPIndexes != nil && !onlyPIndexes[remotePlanPIndex.PlanPIndex.Name]) ||
			remotePlanPIndex.NodeDef == nil {
			continue
		}

		hostPortUrl := "http://" + remotePlanPIndex.NodeDef.HostPort
		if ss.EncryptionEnabled {
			if u, err := remotePlanPIndex.NodeDef.HttpsURL(); err == nil {
				hostPortUrl = u
				atomic.AddUint64(&totRemoteHttpSsl, 1)
			} else {
				atomic.AddUint64(&totRemoteHttp, 1)
			}
		} else {
			atomic.AddUint64(&totRemoteHttp, 1)
		}

		baseURL := hostPortUrl + prefix +
			"/api/pindex/" + remotePlanPIndex.PlanPIndex.Name

		indexClient := &IndexClient{
			mgr:         mgr,
			name:        fmt.Sprintf("IndexClient - %s", baseURL),
			HostPort:    remotePlanPIndex.NodeDef.HostPort,
			IndexName:   indexName,
			IndexUUID:   indexUUID,
			PIndexNames: []string{remotePlanPIndex.PlanPIndex.Name},
			QueryURL:    baseURL + "/query",
			CountURL:    baseURL + "/count",
			Consistency: consistencyParams,
			httpClient:  cbgt.HttpClient(),
		}

		remoteClients = append(remoteClients, indexClient)
	}

	if groupByNode {
		remoteClients, _ = GroupIndexClientsByHostPort(remoteClients)
	}

	for _, remoteClient := range remoteClients {
		collector.Add(remoteClient)
		rv = append(rv, remoteClient)
	}

	return rv, nil
}

// PartitionSelectionStrategy lets clients specify any selection
// preferences for the query serving index partitions spread across
// the cluster.
// PartitionSelectionStrategy recognized options are,
//   - ""              : primary partitions are selected
//   - local           : local partitions are favored, pseudorandom selection from remote
//   - random          : pseudorandom selection from available local and remote
//   - random_balanced : pseudorandom selection from available local and remote nodes by
//     equally distributing the query load across all nodes.
type PartitionSelectionStrategy string

var FetchBleveTargets = func(mgr *cbgt.Manager, indexName, indexUUID string,
	planPIndexFilterName string, partitionSelection PartitionSelectionStrategy) (
	[]*cbgt.PIndex, []*cbgt.RemotePlanPIndex, []string, error) {
	if mgr == nil {
		return nil, nil, nil, fmt.Errorf("manager not defined")
	}

	return mgr.CoveringPIndexesEx(cbgt.CoveringPIndexesSpec{
		IndexName:            indexName,
		IndexUUID:            indexUUID,
		PlanPIndexFilterName: planPIndexFilterName,
	}, nil, false)
}

func bleveIndexTargets(mgr *cbgt.Manager, indexName, indexUUID string,
	ensureCanRead bool, consistencyParams *cbgt.ConsistencyParams,
	cancelCh <-chan bool, groupByNode bool, onlyPIndexes map[string]bool,
	collector BleveIndexCollector, partitionSelection string,
	rcAdder addRemoteClients) (
	[]RemoteClient, int, error) {
	planPIndexFilterName := "ok"
	if ensureCanRead {
		planPIndexFilterName = "canRead"
	}

	if onlyPIndexes != nil && partitionSelection != "" {
		// select all local pindexes
		partitionSelection = "local"
	}

	localPIndexesAll, remotePlanPIndexes, missingPIndexNames, err :=
		FetchBleveTargets(mgr, indexName, indexUUID,
			planPIndexFilterName, PartitionSelectionStrategy(partitionSelection))
	if err != nil {
		return nil, 0, fmt.Errorf("bleve: bleveIndexTargets, err: %v", err)
	}
	if consistencyParams != nil &&
		consistencyParams.Results == "complete" &&
		len(missingPIndexNames) > 0 {
		return nil, 0, fmt.Errorf("bleve: some index partitions aren't reachable,"+
			" missing: %v", len(missingPIndexNames))
	}

	numPIndexes := len(localPIndexesAll) + len(remotePlanPIndexes)

	localPIndexes := localPIndexesAll
	if onlyPIndexes != nil {
		localPIndexes = make([]*cbgt.PIndex, 0, len(localPIndexesAll))
		for _, localPIndex := range localPIndexesAll {
			if onlyPIndexes[localPIndex.Name] {
				localPIndexes = append(localPIndexes, localPIndex)
			}
		}
		if len(localPIndexes) == 0 {
			var wantedPIndexNames []string
			for pIndexName := range onlyPIndexes {
				wantedPIndexNames = append(wantedPIndexNames, pIndexName)
			}
			var hadPIndexNames []string
			for _, pIndex := range localPIndexesAll {
				hadPIndexNames = append(hadPIndexNames, pIndex.Name)
			}
			log.Warnf("pindex_bleve: bleveIndexTargets 0 localPIndexes, wanted: %v, had: %v for index: %s",
				wantedPIndexNames, hadPIndexNames, indexName)
		}
	}

	for _, missingPIndexName := range missingPIndexNames {
		if onlyPIndexes == nil || onlyPIndexes[missingPIndexName] {
			collector.Add(&MissingPIndex{
				name: missingPIndexName,
			})
		}
	}

	remoteClients, err := rcAdder(mgr, indexName, indexUUID,
		remotePlanPIndexes, consistencyParams, onlyPIndexes,
		collector, groupByNode)
	if err != nil {
		return nil, numPIndexes, err
	}

	// TODO: Should kickoff remote queries concurrently before we wait.

	return remoteClients, numPIndexes, cbgt.ConsistencyWaitGroup(indexName, consistencyParams,
		cancelCh, localPIndexes,
		func(localPIndex *cbgt.PIndex) error {
			bindex, _, rev, err := bleveIndex(localPIndex)
			if err != nil {
				return err
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

func bleveIndex(localPIndex *cbgt.PIndex) (bleve.Index, *BleveDest, uint64, error) {
	if !strings.HasPrefix(localPIndex.IndexType, "fulltext-index") {
		return nil, nil, 0, fmt.Errorf("bleve: bleveIndexTargets, wrong type,"+
			" localPIndex: %s, type: %s", localPIndex.Name, localPIndex.IndexType)
	}

	destFwd, ok := localPIndex.Dest.(*cbgt.DestForwarder)
	if !ok || destFwd == nil {
		return nil, nil, 0, fmt.Errorf("bleve: bleveIndexTargets, wrong destFwd type,"+
			" localPIndex: %s, destFwd type: %T", localPIndex.Name, localPIndex.Dest)
	}

	bdest, ok := destFwd.DestProvider.(*BleveDest)
	if !ok || bdest == nil {
		return nil, nil, 0, fmt.Errorf("bleve: bleveIndexTargets, wrong provider type,"+
			" localPIndex: %s, provider type: %T", localPIndex.Name, destFwd.DestProvider)
	}

	bdest.m.RLock()
	bindex := bdest.bindex
	rev := bdest.rev
	bdest.m.RUnlock()

	if bindex == nil {
		return nil, nil, 0, fmt.Errorf("bleve: bleveIndexTargets, nil bindex,"+
			" localPIndex: %s", localPIndex.Name)
	}

	return bindex, bdest, rev, nil
}

// ---------------------------------------------------------

var BleveRouteMethods map[string]string

func init() {
	BleveRouteMethods = make(map[string]string)
}

func BleveInitRouter(r *mux.Router, phase string,
	mgr *cbgt.Manager) {
	prefix := ""
	if mgr != nil {
		prefix = mgr.GetOption("urlPrefix")
	}

	if phase == "static.before" {
		staticBleveMapping := http.FileServer(bleveMappingUI.AssetFS())

		staticBleveMappingRoutes := AssetNames()

		for _, route := range staticBleveMappingRoutes {
			if strings.Contains(route, "static-bleve-mapping") {
				route = strings.TrimPrefix(route, "ns_server_static/fts")
				r.Handle(prefix+route, http.StripPrefix(prefix+"/static-bleve-mapping/",
					staticBleveMapping))
			}
		}
	}

	if phase == "manager.before" {
		r.Handle(prefix+"/api/index",
			NewFilteredListIndexHandler(mgr)).
			Methods("GET").Name(prefix + "/api/index")
		BleveRouteMethods[prefix+"/api/index"] = "GET"
		r.Handle(prefix+"/api/bucket/{bucketName}/scope/{scopeName}/index",
			NewFilteredListIndexHandler(mgr)).
			Methods("GET").Name(prefix +
			"/api/bucket/{bucketName}/scope/{scopeName}/index")
		BleveRouteMethods[prefix+
			"/api/bucket/{bucketName}/scope/{scopeName}/index"] = "GET"
	}

	if phase == "manager.after" {
		bleveMappingUI.RegisterHandlers(r, prefix+"/api")

		listIndexesHandler := ftsHttp.NewListIndexesHandler()
		r.Handle(prefix+"/api/pindex-bleve",
			listIndexesHandler).Methods("GET")
		BleveRouteMethods[prefix+"/api/pindex-bleve"] = "GET"

		getIndexHandler := ftsHttp.NewGetIndexHandler()
		getIndexHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}",
			getIndexHandler).Methods("GET")
		BleveRouteMethods[prefix+"/api/pindex-bleve/{pindexName}"] = "GET"

		docCountHandler := ftsHttp.NewDocCountHandler("")
		docCountHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/count",
			docCountHandler).Methods("GET")
		BleveRouteMethods[prefix+"/api/pindex-bleve/{pindexName}/count"] = "GET"

		searchHandler := ftsHttp.NewSearchHandler("")
		searchHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/query",
			searchHandler).Methods("POST")
		BleveRouteMethods[prefix+"/api/pindex-bleve/{pindexName}/query"] = "POST"

		docGetHandler := ftsHttp.NewDocGetHandler("")
		docGetHandler.IndexNameLookup = rest.PIndexNameLookup
		docGetHandler.DocIDLookup = rest.DocIDLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/doc/{docID}",
			docGetHandler).Methods("GET")
		BleveRouteMethods[prefix+"/api/pindex-bleve/{pindexName}/doc/{docID}"] = "GET"

		listFieldsHandler := ftsHttp.NewListFieldsHandler("")
		listFieldsHandler.IndexNameLookup = rest.PIndexNameLookup
		r.Handle(prefix+"/api/pindex-bleve/{pindexName}/fields",
			listFieldsHandler).Methods("GET")
		BleveRouteMethods[prefix+"/api/pindex-bleve/{pindexName}/fields"] = "GET"
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

func BleveQuerySamples() []cbgt.Documentation {
	return []cbgt.Documentation{
		{
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
		{
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
								"customerIndex": {
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

func reloadableIndexDefParamChange(paramPrev, paramCur string) bool {
	bpPrev := NewBleveParams()
	if len(paramPrev) == 0 {
		// make it a json unmarshal-able string
		paramPrev = "{}"
	}
	err := json.Unmarshal([]byte(paramPrev), bpPrev)
	if err != nil {
		return false
	}

	bpCur := NewBleveParams()
	if len(paramCur) == 0 {
		// make it a json unmarshal-able string
		paramCur = "{}"
	}
	err = json.Unmarshal([]byte(paramCur), bpCur)
	if err != nil {
		return false
	}

	// check for any changes in the spatial engines.
	if bpPrev.Store["spatialPlugin"] != bpCur.Store["spatialPlugin"] {
		return false
	}

	// check for non store parameter differences
	if !reflect.DeepEqual(bpCur.Mapping, bpPrev.Mapping) ||
		!reflect.DeepEqual(bpCur.DocConfig, bpPrev.DocConfig) {
		return false
	}
	// check for indexType updates
	prevType := bpPrev.Store["indexType"]
	curType := bpCur.Store["indexType"]
	if prevType != curType {
		return false
	}
	// always reboot partitions on scorch option changes
	if curType == "scorch" {
		log.Printf("bleve: reloadable scorch option change "+
			" detected, before: %s, after: %s", paramPrev, paramCur)
		return true
	}

	log.Warnf("bleve: unsupported index type: %s", curType)
	return true
}

func reloadableSourceParamsChange(paramPrev, paramCur string) bool {
	if paramPrev == paramCur {
		return true
	}

	if len(paramPrev) == 0 {
		// make it a json unmarshal-able string
		paramPrev = "{}"
	}

	var prevMap map[string]interface{}
	err := json.Unmarshal([]byte(paramPrev), &prevMap)
	if err != nil {
		log.Printf("pindex_bleve: reloadableSourceParamsChange"+
			" json parse paramPrev: %s, err: %v",
			paramPrev, err)
		return false
	}

	if len(paramCur) == 0 {
		// make it a json unmarshal-able string
		paramCur = "{}"
	}

	var curMap map[string]interface{}
	err = json.Unmarshal([]byte(paramCur), &curMap)
	if err != nil {
		log.Printf("pindex_bleve: reloadableSourceParamsChange"+
			" json parse paramCur: %s, err: %v",
			paramCur, err)
		return false
	}

	// any parsing err doesn't matter here.
	po, _ := cbgt.ParseFeedAllotmentOption(paramPrev)
	co, _ := cbgt.ParseFeedAllotmentOption(paramCur)
	if po != co {
		prevMap["feedAllotment"] = ""
		curMap["feedAllotment"] = ""
	}

	return reflect.DeepEqual(prevMap, curMap)
}

// RestartOnIndexDefChanges checks whether the changes in the indexDefns are
// quickly adoptable over a reboot of the pindex implementations.
// eg: kvstore configs updates like compaction percentage.
func RestartOnIndexDefChanges(
	configRequest *cbgt.ConfigAnalyzeRequest) cbgt.ResultCode {
	if configRequest == nil || configRequest.IndexDefnCur == nil ||
		configRequest.IndexDefnPrev == nil {
		return ""
	}
	if configRequest.IndexDefnPrev.Name != configRequest.IndexDefnCur.Name ||
		configRequest.IndexDefnPrev.SourceName !=
			configRequest.IndexDefnCur.SourceName ||
		configRequest.IndexDefnPrev.SourceUUID !=
			configRequest.IndexDefnCur.SourceUUID ||
		!reloadableSourceParamsChange(configRequest.IndexDefnPrev.SourceParams,
			configRequest.IndexDefnCur.SourceParams) ||
		configRequest.IndexDefnPrev.Type !=
			configRequest.IndexDefnCur.Type ||
		!reflect.DeepEqual(configRequest.SourcePartitionsCur,
			configRequest.SourcePartitionsPrev) ||
		!reloadableIndexDefParamChange(configRequest.IndexDefnPrev.Params,
			configRequest.IndexDefnCur.Params) {
		return ""
	}
	return cbgt.PINDEXES_RESTART
}

func mustEncode(w io.Writer, i interface{}) {
	if JSONImpl != nil && JSONImpl.GetManagerOption("jsonImpl") != "std" {
		MustEncodeWithParser(w, i)
	} else {
		rest.MustEncode(w, i)
	}
}

// MustEncodeWithParser encode with the registered parserType,
func MustEncodeWithParser(w io.Writer, i interface{}) {
	rw, rwOk := w.(http.ResponseWriter)
	if rwOk {
		h := rw.Header()
		if h != nil {
			h.Set("Cache-Control", "no-cache")
			if h.Get("Content-type") == "" {
				h.Set("Content-type", "application/json")
			}
		}
	}
	var err error
	if JSONImpl != nil {
		err = JSONImpl.Encode(w, i)
	} else {
		err = fmt.Errorf("bleve: MustEncodeWithParser fails as no custom parser found")
	}
	if err != nil {
		if rwOk {
			crw, ok := rw.(*rest.CountResponseWriter)
			if ok && crw.Wrote {
				return
			}
			rest.PropagateError(rw, nil,
				fmt.Sprintf("rest: custom JSON: %s encode, err: %v",
					JSONImpl.GetParserType(), err), http.StatusInternalServerError)
		}
	}
}

// -----------------------------------------------------------------------------

// This API determines if the sourceName provided within the index
// definition is compatible for FTS indexes by obtaining bucket
// information from the /pools/default/bucket/{bucketName} endpoint.
func checkSourceCompatability(mgr *cbgt.Manager, sourceName string) error {
	if mgr == nil {
		return nil
	}

	if len(sourceName) == 0 {
		return fmt.Errorf("source name not provided")
	}

	url := mgr.Server() + "/pools/default/b/" + url.QueryEscape(sourceName)
	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return err
	}

	resp, err := HttpGet(cbgt.HttpClient(), u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	rv := struct {
		BucketType     string `json:"bucketType"`
		StorageBackend string `json:"storageBackend"`
	}{}

	err = UnmarshalJSON(respBuf, &rv)
	if err != nil {
		return err
	}

	if rv.BucketType != "membase" && rv.BucketType != "ephemeral" {
		return fmt.Errorf("unsupported bucket type")
	}

	if rv.BucketType == "membase" {
		switch rv.StorageBackend {
		case "couchstore":
		case "magma":
		default:
			return fmt.Errorf("unsupported storage/backend for bucket: %v",
				rv.StorageBackend)
		}
	}

	return nil
}

func findGeoPoint(dm *mapping.DocumentMapping) bool {
	if dm != nil && dm.Enabled {
		for _, fm := range dm.Fields {
			if fm.Index && fm.Type == "geopoint" {
				return true
			}
		}
		for _, dmapping := range dm.Properties {
			found := findGeoPoint(dmapping)
			if found {
				return true
			}
		}
	}
	return false
}

func isGeoPointFieldInMapping(bp *BleveParams) bool {
	if bp != nil && bp.Mapping != nil {
		if im, ok := bp.Mapping.(*mapping.IndexMappingImpl); ok {
			// look for in the default mapping.
			res := findGeoPoint(im.DefaultMapping)
			if res {
				return true
			}

			// look for among the custom mappings.
			for _, dm := range im.TypeMapping {
				res := findGeoPoint(dm)
				if res {
					return true
				}
			}
		}
	}

	return false
}

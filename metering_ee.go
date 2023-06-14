//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build enterprise
// +build enterprise

package cbft

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/couchbase/regulator"
	"github.com/couchbase/regulator/config"
	"github.com/couchbase/regulator/factory"
	"github.com/couchbase/regulator/metering"
	"github.com/couchbase/regulator/utils"
	"github.com/couchbase/regulator/variants"
)

const (
	read uint = iota
	write
	compute
)

type message struct {
	user      string
	bucket    string
	index     string
	bytes     uint64
	operation uint
}

type regulatorStats struct {
	TotalRUsMetered                uint64  `json:"total_RUs_metered"`
	TotalWUsMetered                uint64  `json:"total_WUs_metered"`
	TotalMeteringErrs              uint64  `json:"total_metering_errs"`
	TotalReadOpsCapped             uint64  `json:"total_read_ops_capped"`
	TotalReadOpsRejected           uint64  `json:"total_read_ops_rejected"`
	TotalWriteOpsRejected          uint64  `json:"total_write_ops_rejected"`
	TotalWriteThrottleSeconds      float64 `json:"total_write_throttle_seconds"`
	TotalCheckQuotaReadErrs        uint64  `json:"total_read_ops_metering_errs"`
	TotalCheckQuotaWriteErrs       uint64  `json:"total_write_ops_metering_errs"`
	TotalOpsTimedOutWhileMetering  uint64  `json:"total_ops_timed_out_while_metering"`
	TotalBatchLimitingTimeOuts     uint64  `json:"total_batch_limiting_timeouts"`
	TotalBatchRejectionBackoffTime uint64  `json:"total_batch_rejection_backoff_time_ms"`
	TotCheckAccessOpsRejects       uint64  `json:"total_check_access_rejects"`
	TotCheckAccessErrs             uint64  `json:"total_check_access_errs"`
}

type serviceRegulator struct {
	mgr         *cbgt.Manager
	handler     regulator.StatsHttpHandler
	messageChan chan *message
	// pindex -> stats
	prevWBytes map[string]uint64

	m sync.RWMutex // Protects the fields that follow.
	// bucket -> stats
	stats map[string]*regulatorStats
}

var reg *serviceRegulator

func MeteringEndpointHandler(mgr *cbgt.Manager) (string,
	regulator.StatsHttpHandler) {
	return regulator.MeteringEndpoint, NewMeteringHandler(mgr)
}
func NewMeteringHandler(mgr *cbgt.Manager) regulator.StatsHttpHandler {
	regOps := regulator.InitSettings{
		NodeID:       service.NodeID(mgr.UUID()),
		Service:      regulator.Search,
		SettingsFile: mgr.Options()["regulatorSettingsFile"],
	}

	regHandler := factory.InitRegulator(regOps)
	reg = &serviceRegulator{
		handler:     regHandler,
		mgr:         mgr,
		messageChan: make(chan *message, 10),
		prevWBytes:  make(map[string]uint64),
		stats:       make(map[string]*regulatorStats),
	}
	log.Printf("regulator: Metering and limiting of FTS read/write" +
		" requests has started")
	go reg.startMetering()
	return regHandler
}

func (sr *serviceRegulator) disableLimitingThrottling() bool {
	if v, ok := sr.mgr.Options()["disableRegulatorControl"]; ok {
		if val, err := strconv.ParseBool(v); err == nil {
			return val
		}
	}
	return false
}

func (sr *serviceRegulator) startMetering() {
	for {
		select {
		case msg := <-sr.messageChan:
			var err error
			switch msg.operation {
			case write:
				err = sr.recordWrites(msg.bucket, msg.index, msg.user, msg.bytes)
			default:
				// invalid op
				return
			}

			if err != nil {
				sr.updateRegulatorStats(msg.bucket, "total_metering_errs", 1)
				log.Errorf("metering: error while metering the stats "+
					"with regulator %v", err)
			}
		}
	}
}

// A common utility to send out the metering messages onto the channel
func (sr *serviceRegulator) meteringUtil(bucket, index string,
	totalBytes uint64, op uint) {
	sr.initialiseRegulatorStats(bucket)
	msg := &message{
		user:      "",
		bucket:    bucket,
		index:     index,
		bytes:     totalBytes,
		operation: op,
	}

	select {
	case sr.messageChan <- msg:
	case <-time.After(5 * time.Second):
		sr.updateRegulatorStats(bucket, "total_ops_timed_out_while_metering", 1)
		log.Warnf("metering: message dropped, too much traffic on the "+
			"channel %v", msg)
	}
}

// Note: keeping the metering of read and write separate,
// so that further experiments or observations may lead to
// them behaving differently from each other
func MeterWrites(stopCh chan struct{}, bucket string, index bleve.Index) {
	if !ServerlessMode {
		// no metering for non-serverless versions
		return
	}
	if isClosed(stopCh) {
		log.Warnf("metering: pindex was closed, so ignoring its metering")
		return
	}
	scorchStats := index.StatsMap()
	indexStats, _ := scorchStats["index"].(map[string]interface{})
	if indexStats == nil {
		log.Warnf("metering: index stats missing")
		return
	}
	analysisBytes, _ := indexStats["num_bytes_written_at_index_time"].(uint64)

	reg.meteringUtil(bucket, index.Name(), analysisBytes, write)
}

func (sr *serviceRegulator) getWUs(pindexName string, bytes uint64) (regulator.Units, error) {

	// metering of write units is happening at a pindex level,
	// so to ensure the correct delta (of the bytes written on disk stat)
	// the prevWBytes is tracked per pindex
	sr.m.RLock()
	prevBytesMetered := sr.prevWBytes[pindexName]
	sr.m.RUnlock()
	if bytes <= prevBytesMetered {
		if bytes < prevBytesMetered {
			sr.m.Lock()
			sr.prevWBytes[pindexName] = 0
			sr.m.Unlock()
		}
		rv, _ := regulator.NewUnits(regulator.Search, regulator.Write, 0)
		return rv, nil
	}

	wus, err := metering.SearchWriteToWU(bytes - prevBytesMetered)
	if err != nil {
		rv, _ := regulator.NewUnits(regulator.Search, regulator.Write, 0)
		return rv, err
	}
	return wus, nil
}

func (sr *serviceRegulator) recordWrites(bucket, pindexName, user string,
	bytes uint64) error {

	// metering of write units is happening at a pindex level,
	// so to ensure the correct delta (of the bytes written on disk stat)
	// the prevWBytes is tracked per pindex
	wus, err := sr.getWUs(pindexName, bytes)
	if err != nil {
		return err
	}
	context := regulator.NewBucketCtx(bucket)
	sr.prevWBytes[pindexName] = bytes

	err = regulator.RecordUnits(context, wus)
	if err == nil {
		sr.updateRegulatorStats(bucket, "total_WUs_metered", float64(wus.Whole()))
	}
	return err
}

func RollbackRefund(pindex, sourceName string, bytesWrittenAtRollbackSeqno uint64) {
	bytesWrittenAtHighSeqno := reg.prevWBytes[pindex]
	if bytesWrittenAtHighSeqno > bytesWrittenAtRollbackSeqno {
		refundUnits, err := metering.SearchWriteToWU(bytesWrittenAtHighSeqno -
			bytesWrittenAtRollbackSeqno)
		if err != nil {
			log.Errorf("metering: failed to fetch refund units corresponding"+
				"to the rollback situation err: %v", err)
			return
		}
		bucketCtx := regulator.NewBucketCtx(sourceName)
		err = regulator.RefundUnits(bucketCtx, refundUnits)
		if err != nil {
			log.Errorf("metering: failed to refund the units corresponding "+
				"to the rollback situation err: %v", err)
		}
	}

	reg.prevWBytes[pindex] = bytesWrittenAtRollbackSeqno
}

func (sr *serviceRegulator) updateRegulatorStats(bucket, statName string,
	val float64) {
	sr.m.Lock()
	defer sr.m.Unlock()

	if sr.stats[bucket] != nil {
		switch statName {
		case "total_RUs_metered":
			sr.stats[bucket].TotalRUsMetered += uint64(val)
		case "total_WUs_metered":
			sr.stats[bucket].TotalWUsMetered += uint64(val)
		case "total_read_ops_capped":
			sr.stats[bucket].TotalReadOpsCapped += uint64(val)
		case "total_read_ops_rejected":
			sr.stats[bucket].TotalReadOpsRejected += uint64(val)
		case "total_write_ops_rejected":
			sr.stats[bucket].TotalWriteOpsRejected += uint64(val)
		case "total_write_throttle_seconds":
			sr.stats[bucket].TotalWriteThrottleSeconds += val
		case "total_read_ops_metering_errs":
			sr.stats[bucket].TotalCheckQuotaReadErrs += uint64(val)
		case "total_write_ops_metering_errs":
			sr.stats[bucket].TotalCheckQuotaWriteErrs += uint64(val)
		case "total_ops_timed_out_while_metering":
			sr.stats[bucket].TotalOpsTimedOutWhileMetering += uint64(val)
		case "total_batch_limiting_timeouts":
			sr.stats[bucket].TotalBatchLimitingTimeOuts += uint64(val)
		case "total_batch_rejection_backoff_time_ms":
			sr.stats[bucket].TotalBatchRejectionBackoffTime += uint64(val)
		case "total_check_access_rejects":
			sr.stats[bucket].TotCheckAccessOpsRejects += uint64(val)
		case "total_check_access_errs":
			sr.stats[bucket].TotCheckAccessErrs += uint64(val)
		case "total_metering_errs":
			sr.stats[bucket].TotalMeteringErrs += uint64(val)
		}
	}
}

func RefreshRegulatorStats() {
	if !ServerlessMode {
		return
	}

	existingBuckets := make(map[string]struct{})
	_, indexDefsByName, _ := reg.mgr.GetIndexDefs(false)
	for _, indexDef := range indexDefsByName {
		if _, ok := existingBuckets[indexDef.SourceName]; !ok {
			existingBuckets[indexDef.SourceName] = struct{}{}
		}
	}
	reg.m.Lock()
	for k := range reg.stats {
		if _, ok := existingBuckets[k]; !ok {
			delete(reg.stats, k)
		}
	}
	reg.m.Unlock()
}

func regulatorAggregateStats(in map[string]*regulatorStats) map[string]interface{} {
	rv := make(map[string]interface{}, len(in)+1)
	var totalUnitsMetered uint64
	for k, v := range in {
		totalUnitsMetered += (v.TotalRUsMetered + v.TotalWUsMetered)
		rv[k] = v
	}
	rv["total_units_metered"] = totalUnitsMetered
	return rv
}

func GetRegulatorStats() map[string]interface{} {
	if ServerlessMode {
		reg.m.RLock()
		rv := regulatorAggregateStats(reg.stats)
		reg.m.RUnlock()
		return rv
	}
	return nil
}

// Returns the maximum and minimum time, in milliseconds, for limiting requests.
func serverlessLimitingBounds() (int, int) {
	var err error
	minTime := defaultLimitingMinTime
	if min, exists := reg.mgr.Options()["minBackoffTimeForBatchLimitingMS"]; exists {
		minTime, err = strconv.Atoi(min)
		if err != nil {
			log.Errorf("limiting/throttling: error parsing minimum time for "+
				"exponential backoff, returning defaults: %#v", err)
			minTime = defaultLimitingMinTime
		}
	}

	maxTime := defaultLimitingMaxTime
	if max, exists := reg.mgr.Options()["maxBackoffTimeForBatchLimitingMS"]; exists {
		maxTime, err = strconv.Atoi(max)
		if err != nil {
			log.Errorf("limiting/throttling: error parsing maximum time for "+
				"exponential backoff, returning defaults: %#v", err)
			maxTime = defaultLimitingMaxTime
		}
	}

	return minTime, maxTime
}

func getThrottleLimit(ctx regulator.Ctx) utils.Limit {
	bCtx, _ := ctx.(regulator.BucketCtx)
	rCfg := config.GetConfig()
	searchHandle := config.ResolveSettingsHandle(regulator.Search, ctx)
	return rCfg.GetConfiguredLimitForBucket(bCtx.Bucket(), searchHandle)
}

// Note: keeping the throttle/limiting of read and write separate,
// so that further experiments or observations may lead to
// them behaving differently from each other based on the request passed
func CheckQuotaWrite(stopCh chan struct{}, bucket, user string, batchExec bool,
	req interface{}) (CheckResult, time.Duration, error) {
	if !ServerlessMode || reg.disableLimitingThrottling() {
		// no throttle/limiting checks for non-serverless versions
		// and when regulator's disabled.
		return CheckResultProceed, 0, nil
	}

	context := regulator.NewBucketCtx(bucket)
	checkQuotaOps := &regulator.CheckQuotaOpts{
		NoThrottle:          false,
		NoReject:            false,
		EstimatedDuration:   time.Duration(0),
		EstimatedUnitsMulti: []regulator.Units{},
	}

	reg.initialiseRegulatorStats(bucket)
	if batchExec {
		bindex := req.(bleve.Index)
		if bindex == nil {
			return CheckResultError, 0, fmt.Errorf("limiting/throttling: " +
				"index missing")
		}
		scorchStats := bindex.StatsMap()
		indexStats, _ := scorchStats["index"].(map[string]interface{})
		if indexStats == nil {
			return CheckResultError, 0, fmt.Errorf("limiting/throttling: " +
				"index stats missing")
		}
		bytes, _ := indexStats["num_bytes_written_at_index_time"].(uint64)
		wus, err := reg.getWUs(bindex.Name(), bytes)
		if err != nil {
			return CheckResultError, 0, err
		}
		checkQuotaOps.EstimatedUnits = wus
	}
	action, duration, err := regulator.CheckQuota(context, checkQuotaOps)
	if !batchExec {
		switch action {
		case regulator.CheckResultError:
			reg.updateRegulatorStats(bucket, "total_write_ops_metering_errs", 1)

		// index create and update requests at the request admission layer
		// are only limited, not throttled
		case regulator.CheckResultReject, regulator.CheckResultThrottleProceed,
			regulator.CheckResultThrottleRetry:
			reg.updateRegulatorStats(bucket, "total_write_ops_rejected", 1)
		case regulator.CheckResultProceed:
			if utilStatsTracker.isClusterIdle() {
				utilStatsTracker.spawnNodeUtilStatsTracker()
			}
		default:
			log.Warnf("limiting/metering: unindentified result from "+
				"regulator, result: %v\n", action)

			// allowing normal execution for unidentified actions for now
			return CheckResultProceed, 0, nil
		}
		return CheckResult(action), duration, err
	}

	for {
		if isClosed(stopCh) {
			return CheckResultError, 0, fmt.Errorf("limiting/throttling: pindex " +
				"was closed, ignoring its limting and throttling")
		}
		switch CheckResult(action) {
		case CheckResultProceed:
			if utilStatsTracker.isClusterIdle() {
				utilStatsTracker.spawnNodeUtilStatsTracker()
			}
			return CheckResult(action), duration, err
		case CheckResultThrottleProceed:
			time.Sleep(duration)
			reg.updateRegulatorStats(bucket, "total_write_throttle_seconds",
				float64(duration)/float64(time.Second))
			action = regulator.CheckResultProceed
		case CheckResultThrottleRetry:
			time.Sleep(duration)
			reg.updateRegulatorStats(bucket, "total_write_throttle_seconds",
				float64(duration)/float64(time.Second))
			action, duration, err = regulator.CheckQuota(context, checkQuotaOps)
			if err != nil {
				log.Errorf("limiting/throttling: error while checking quota "+
					"err: %v", err)
				return CheckResultError, 0, err
			}

		case CheckResultReject:
			minTime, maxTime := serverlessLimitingBounds()
			nextSleepMS := minTime
			backoffFactor := 2
			// Should return -1 when regulator returns CheckResultProceed/Throttle
			// since the system can definitely make progress.
			// For CheckResultReject, should return 0 since
			// no progress has been made and needs a retry.
			checkQuotaExponentialBackoff := func() int {
				if isClosed(stopCh) {
					action = regulator.CheckResultError
					return -1
				}
				var err error
				action, duration, err = regulator.CheckQuota(context, checkQuotaOps)
				if err != nil {
					return -1 // no progress can be made in case of regulator error.
				}

				if CheckResult(action) == CheckResultReject {
					reg.updateRegulatorStats(bucket, "total_write_ops_rejected",
						1)
					// A timeout logic to consider while performing the
					// exponential backoff. If the exponential backoff takes more
					// than maxTime to get a progress
					if nextSleepMS > maxTime {
						reg.updateRegulatorStats(bucket, "total_batch_limiting_timeouts", 1)
						return -1
					}
					reg.updateRegulatorStats(bucket, "total_batch_rejection_backoff_time_ms",
						float64(nextSleepMS))

					nextSleepMS = nextSleepMS * backoffFactor
					return 0 // backoff on reject
				}
				log.Debugf("limiting/throttling: ending exponential backoff on " +
					"rejection from regulator")
				return -1 // terminate backoff on any other action
			}

			// Blocking wait to execute the request till it returns.
			cbgt.ExponentialBackoffLoop("", checkQuotaExponentialBackoff,
				minTime, float32(backoffFactor), maxTime)
			if CheckResult(action) == CheckResultReject {
				// If the request is still limited after the exponential backoff,
				// accept it.
				action = regulator.CheckResultProceed
			}

		// as of now this case only corresponds to CheckResultError
		default:
			reg.updateRegulatorStats(bucket, "total_write_ops_metering_errs", 1)
			log.Errorf("limiting/throttling: error while limiting/throttling "+
				"the request %v\n", err)
			action = regulator.CheckResultProceed
		}
	}
}

func (sr *serviceRegulator) initialiseRegulatorStats(bucket string) {
	sr.m.RLock()
	if reg.stats[bucket] == nil {
		sr.m.RUnlock()
		sr.m.Lock()
		reg.stats[bucket] = &regulatorStats{}
		sr.m.Unlock()
		return
	}
	sr.m.RUnlock()
}

func CheckQuotaRead(bucket, user string,
	req interface{}) (CheckResult, time.Duration, error) {
	if !ServerlessMode || reg.disableLimitingThrottling() {
		// no throttle/limiting checks for non-serverless versions
		// and when regulator's disabled.
		return CheckResultProceed, 0, nil
	}

	context := regulator.NewBucketCtx(bucket)
	checkQuotaOps := &regulator.CheckQuotaOpts{
		NoThrottle:          true,
		NoReject:            false,
		EstimatedDuration:   time.Duration(0),
		EstimatedUnitsMulti: []regulator.Units{},
	}

	reg.initialiseRegulatorStats(bucket)
	result, duration, err := regulator.CheckQuota(context, checkQuotaOps)
	switch result {
	case regulator.CheckResultError:
		reg.updateRegulatorStats(bucket, "total_read_ops_metering_errs", 1)

	// query requests are not throttled, only limited at the
	// request admission layer
	case regulator.CheckResultReject, regulator.CheckResultThrottleProceed,
		regulator.CheckResultThrottleRetry:
		reg.updateRegulatorStats(bucket, "total_read_ops_rejected", 1)
	case regulator.CheckResultProceed:
		if utilStatsTracker.isClusterIdle() {
			utilStatsTracker.spawnNodeUtilStatsTracker()
		}
	default:
		log.Warnf("limiting/metering: unindentified result from "+
			"regulator, result: %v\n", result)
		return CheckResultProceed, 0, nil
	}
	return CheckResult(result), duration, err
}

func WriteRegulatorMetrics(w http.ResponseWriter, storageStats map[string]uint64) {
	if !ServerlessMode {
		// dont write to prom http.ResponseWriter for
		// non-serverless builds.
		return
	}
	reg.handler.WriteStats(w)

	statName := "num_disk_bytes_used_per_bucket"
	for bucket, storageBytes := range storageStats {
		b, err := json.Marshal(storageBytes)
		if err != nil {
			return
		}
		key := ` {bucket="` + bucket + `"}`
		w.Write([]byte(fmt.Sprintf("# TYPE fts_%s gauge\n", statName)))
		w.Write(append([]byte("fts_"+statName+key+" "), b...))
		w.Write(bline)
	}

}

func NewAggregateRecorder(bucket string) interface{} {
	ctx := regulator.NewBucketCtx(bucket)
	options := &regulator.AggregationOptions{
		Unbilled:         false,
		DeferredMetering: true,
	}
	return regulator.StartAggregateRecorder(ctx, regulator.Search, regulator.Read, options)
}

func getVariantType(queryType search.SearchQueryType) regulator.UnitType {
	switch queryType {
	case search.Geo:
		return variants.SearchGeoReadVariant
	case search.Numeric:
		return variants.SearchNumericReadVariant
	case search.Term:
		fallthrough
	default:
		return variants.SearchTextReadVariant

	}
}

func AggregateRecorderCallback(costRecorder interface{},
	msg search.SearchIncrementalCostCallbackMsg, queryType search.SearchQueryType, bytes uint64) {
	recorder := costRecorder.(regulator.AggregateRecorder)
	switch msg {
	case search.DoneM:
		recorder.Commit()
	case search.AbortM:
		recorder.Abort()
	default:
		variant := getVariantType(queryType)
		recorder.AddVariantBytes(bytes, variant)
	}
}

func CheckAccess(bucket, username string) (CheckResult, error) {
	if !ServerlessMode {
		return CheckAccessNormal, nil
	}

	context := regulator.NewBucketCtx(bucket)
	action, _, err := regulator.CheckAccess(context, false)
	switch action {
	case regulator.AccessNormal:
	case regulator.AccessNoIngress:
		reg.updateRegulatorStats(bucket, "total_check_access_rejects", 1)
		return CheckAccessNoIngress, fmt.Errorf("storage limit has been reached")
	case regulator.AccessError:
		reg.updateRegulatorStats(bucket, "total_check_access_errs", 1)
		return CheckAccessError, fmt.Errorf("unexpected check access error "+
			"with respect to storage limiting %v", err)
	}
	return CheckAccessNormal, nil
}

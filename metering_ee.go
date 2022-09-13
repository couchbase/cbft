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
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/couchbase/regulator"
	"github.com/couchbase/regulator/config"
	"github.com/couchbase/regulator/factory"
	"github.com/couchbase/regulator/metering"
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
	TotalReadOpsCapped             uint64  `json:"total_read_ops_capped"`
	TotalReadOpsRejected           uint64  `json:"total_read_ops_rejected"`
	TotalWriteOpsRejected          uint64  `json:"total_write_ops_rejected"`
	TotalWriteThrottleSeconds      float64 `json:"total_write_throttle_seconds"`
	TotalCheckQuotaReadErrs        uint64  `json:"total_read_ops_metering_errs"`
	TotalCheckQuotaWriteErrs       uint64  `json:"total_write_ops_metering_errs"`
	TotalOpsTimedOutWhileMetering  uint64  `json:"total_ops_timed_out_while_metering"`
	TotalBatchLimitingTimeOuts     uint64  `json:"total_batch_limiting_timeouts"`
	TotalBatchRejectionBackoffTime uint64  `json:"total_batch_rejection_backoff_time_ms"`
}

type serviceRegulator struct {
	mgr         *cbgt.Manager
	handler     regulator.StatsHttpHandler
	messageChan chan *message
	// pindex -> stats
	prevWBytes map[string]uint64
	prevRBytes map[string]uint64

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
		NodeID:  service.NodeID(mgr.UUID()),
		Service: regulator.Search,
	}

	regHandler := factory.InitRegulator(regOps)
	reg = &serviceRegulator{
		handler:     regHandler,
		mgr:         mgr,
		messageChan: make(chan *message, 10),
		prevWBytes:  make(map[string]uint64),
		prevRBytes:  make(map[string]uint64),
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
			case read:
				err = sr.recordReads(msg.bucket, msg.index, msg.user, msg.bytes)
			case write:
				err = sr.recordWrites(msg.bucket, msg.index, msg.user, msg.bytes)
			case compute:
				// TODO
			default:
				// invalid op
				return
			}

			if err != nil {
				log.Errorf("metering: error while metering the stats "+
					"with regulator %v", err)
			}
		}
	}
}

// A common utility to send out the metering messages onto the channel
func (sr *serviceRegulator) meteringUtil(bucket, index string,
	totalBytes uint64, op uint) {
	if sr.stats[bucket] == nil {
		sr.stats[bucket] = &regulatorStats{}
	}
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
		reg.updateRegulatorStats(bucket, "total_ops_timed_out_while_metering", 1)
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
	analysisBytes, _ := indexStats["num_bytes_written_at_index_time"].(uint64)

	reg.meteringUtil(bucket, index.Name(), analysisBytes, write)
}

func MeterReads(bucket string, index bleve.Index) {
	if !ServerlessMode {
		// no metering for non-serverless versions
		return
	}

	scorchStats := index.StatsMap()
	indexStats, _ := scorchStats["index"].(map[string]interface{})
	bytesReadStat, _ := indexStats["num_bytes_read_at_query_time"].(uint64)

	reg.meteringUtil(bucket, index.Name(), bytesReadStat, read)
}

func (sr *serviceRegulator) recordWrites(bucket, pindexName, user string,
	bytes uint64) error {

	// metering of write units is happening at a pindex level,
	// so to ensure the correct delta (of the bytes written on disk stat)
	// the prevWBytes is tracked per pindex
	prevBytesMetered := sr.prevWBytes[pindexName]

	if bytes <= prevBytesMetered {
		if bytes < prevBytesMetered {
			sr.prevWBytes[pindexName] = 0
		}
		return nil
	}
	wus, err := metering.SearchWriteToWU(bytes - prevBytesMetered)
	if err != nil {
		return err
	}
	context := regulator.NewBucketCtx(bucket)
	sr.prevWBytes[pindexName] = bytes

	sr.updateRegulatorStats(bucket, "total_WUs_metered", float64(wus.Whole()))
	return regulator.RecordUnits(context, wus)
}

func (sr *serviceRegulator) recordReads(bucket, pindexName, user string,
	bytes uint64) error {

	// metering of read units is happening at a pindex level,
	// each partition (can be either on coordinator or remote node)
	// meters whatever bytes is read from it on its local node.
	prevBytesMetered := sr.prevRBytes[pindexName]
	if bytes <= prevBytesMetered {
		if bytes < prevBytesMetered {
			sr.prevRBytes[pindexName] = 0
		}
		return nil
	}

	rus, err := metering.SearchReadToRU(bytes - prevBytesMetered)
	if err != nil {
		return err
	}

	// Capping the RUs according to the ftsThrottleLimit for that bucket,
	// fetched from regulator's GetConfiguredLimitForBucket API.
	// The reasoning here is that currently the queries incur a very high
	// first time cost in terms of the bytes read from the disk. In a scenario
	// where there are multiple first time queries, the per second units
	// metered would be extremely high, which can result in extremely
	// high wait times.
	// the formula for capping off ->
	// 			ftsThrottleLimit/(#indexesPerTenant * #queriesAllowed)
	context := regulator.NewBucketCtx(bucket)
	throttleLimit := uint64(getThrottleLimit(context))
	if rus.Whole() > throttleLimit {
		maxIndexCountPerSource := 20
		v, found := cbgt.ParseOptionsInt(sr.mgr.Options(), "maxIndexCountPerSource")
		if found {
			maxIndexCountPerSource = v
		}
		rus, err = regulator.NewUnits(regulator.Search, 0,
			throttleLimit/(uint64(maxIndexCountPerSource)*10))
		if err != nil {
			return fmt.Errorf("metering: failed to cap the RUs %v\n", err)
		}
		sr.updateRegulatorStats(bucket, "total_read_ops_capped", 1)
	}

	sr.prevRBytes[pindexName] = bytes

	sr.updateRegulatorStats(bucket, "total_RUs_metered", float64(rus.Whole()))
	return regulator.RecordUnits(context, rus)
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
		}
	}
}

func RefreshRegulatorStats() {
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

func getThrottleLimit(ctx regulator.Ctx) uint32 {
	bCtx, _ := ctx.(regulator.BucketCtx)
	rCfg := config.GetConfig()
	searchHandle := config.ResolveSettingsHandle(regulator.Search, ctx)
	limit := rCfg.GetConfiguredLimitForBucket(bCtx.Bucket(), searchHandle)
	return limit
}

// Note: keeping the throttle/limiting of read and write separate,
// so that further experiments or observations may lead to
// them behaving differently from each other based on the request passed
func CheckQuotaWrite(stopCh chan struct{}, bucket, user string, retry bool,
	req interface{}) (CheckResult, time.Duration, error) {
	if !ServerlessMode || reg.disableLimitingThrottling() {
		// no throttle/limiting checks for non-serverless versions
		// and when regulator's disabled.
		return CheckResultNormal, 0, nil
	}

	context := regulator.NewBucketCtx(bucket)
	estimatedUnits, err := regulator.NewUnits(regulator.Search,
		regulator.Write, uint64(0))
	if err != nil {
		return CheckResultError, 0, fmt.Errorf("limiting/throttling: failed to "+
			"create estimated units err: %v", err)
	}

	checkQuotaOps := &regulator.CheckQuotaOpts{
		NoThrottle:        false,
		NoReject:          false,
		EstimatedDuration: time.Duration(0),
		EstimatedUnits:    []regulator.Units{estimatedUnits},
	}
	if reg.stats[bucket] == nil {
		reg.stats[bucket] = &regulatorStats{}
	}
	action, duration, err := regulator.CheckQuota(context, checkQuotaOps)
	if !retry {
		switch action {
		case regulator.CheckResultError:
			reg.updateRegulatorStats(bucket, "total_write_ops_metering_errs", 1)

		// index create and update requests at the request admission layer
		// are only limited, not throttled
		case regulator.CheckResultReject, regulator.CheckResultThrottle:
			reg.updateRegulatorStats(bucket, "total_write_ops_rejected", 1)
		case regulator.CheckResultNormal:
		default:
			log.Warnf("limiting/metering: unindentified result from "+
				"regulator, result: %v\n", action)

			// allowing normal execution for unidentified actions for now
			return CheckResultNormal, 0, nil
		}
		return CheckResult(action), duration, err
	}

	for {
		if isClosed(stopCh) {
			return CheckResultError, 0, fmt.Errorf("limiting/throttling: pindex " +
				"was closed, ignoring its limting and throttling")
		}
		switch CheckResult(action) {
		case CheckResultNormal:
			return CheckResult(action), duration, err
		case CheckResultThrottle:
			time.Sleep(duration)
			reg.updateRegulatorStats(bucket, "total_write_throttle_seconds",
				float64(duration)/float64(time.Second))
			action = regulator.CheckResultNormal

		case CheckResultReject:
			minTime, maxTime := serverlessLimitingBounds()
			nextSleepMS := minTime
			backoffFactor := 2

			// Should return -1 when regulator returns CheckResultNormal/Throttle
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
				action = regulator.CheckResultNormal
			}

		// as of now this case only corresponds to CheckResultError
		default:
			reg.updateRegulatorStats(bucket, "total_write_ops_metering_errs", 1)
			log.Errorf("limiting/throttling: error while limiting/throttling "+
				"the request %v\n", err)
			action = regulator.CheckResultNormal
		}
	}
}

func CheckQuotaRead(bucket, user string,
	req interface{}) (CheckResult, time.Duration, error) {
	if !ServerlessMode || reg.disableLimitingThrottling() {
		// no throttle/limiting checks for non-serverless versions
		// and when regulator's disabled.
		return CheckResultNormal, 0, nil
	}

	context := regulator.NewBucketCtx(bucket)
	estimatedUnits, err := regulator.NewUnits(regulator.Search,
		regulator.Write, uint64(0))
	if err != nil {
		return CheckResultError, 0, fmt.Errorf("limiting/throttling: failed to "+
			"create estimated units err: %v", err)
	}
	checkQuotaOps := &regulator.CheckQuotaOpts{
		NoThrottle:        true,
		NoReject:          false,
		EstimatedDuration: time.Duration(0),
		EstimatedUnits:    []regulator.Units{estimatedUnits},
	}
	if reg.stats[bucket] == nil {
		reg.stats[bucket] = &regulatorStats{}
	}
	result, duration, err := regulator.CheckQuota(context, checkQuotaOps)
	switch result {
	case regulator.CheckResultError:
		reg.updateRegulatorStats(bucket, "total_read_ops_metering_errs", 1)

	// query requests are not throttled, only limited at the
	// request admission layer
	case regulator.CheckResultReject, regulator.CheckResultThrottle:
		reg.updateRegulatorStats(bucket, "total_read_ops_rejected", 1)
	case regulator.CheckResultNormal:
	default:
		log.Warnf("limiting/metering: unindentified result from "+
			"regulator, result: %v\n", result)
		return CheckResultNormal, 0, nil
	}
	return CheckResult(result), duration, err
}

func WriteRegulatorMetrics(w http.ResponseWriter, storageStats map[string]uint64) {
	if !ServerlessMode {
		// dont write to prom http.ResponseWriter for
		// non-serverless builds.
		return
	}
	reg.handler.WriteMetrics(w)

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

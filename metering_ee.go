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
	"fmt"
	"net/http"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"github.com/couchbase/regulator"
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

type serviceRegulator struct {
	mgr         *cbgt.Manager
	handler     regulator.StatsHttpHandler
	messageChan chan *message
	// pindex -> stats
	prevWBytes map[string]uint64
	prevRBytes map[string]uint64
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
	}
	log.Printf("metering: Metering and limiting of FTS read/write" +
		" requests has started")
	go reg.startMetering()
	return regHandler
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
		log.Warnf("metering: message dropped, too much traffic on the "+
			"channel %v", msg)
	}
}

// Note: keeping the metering of read and write separate,
// so that further experiments or observations may lead to
// them behaving differently from each other
func MeterWrites(bucket string, index bleve.Index) {
	if !ServerlessMode {
		// no metering for non-serverless versions.
		return
	}

	scorchStats := index.StatsMap()
	indexStats, _ := scorchStats["index"].(map[string]interface{})
	analysisBytes, _ := indexStats["num_bytes_written_at_index_time"].(uint64)

	reg.meteringUtil(bucket, index.Name(), analysisBytes, write)
}

func MeterReads(bucket string, index bleve.Index) {
	if !ServerlessMode {
		// no metering for non-serverless versions.
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

	// Capping the RUs according to the default ftsThrottleLimit = 5000.
	// The reasoning here is that currently the queries incur a very high
	// first time cost in terms of the bytes read from the disk. In a scenario
	// where there are multiple first time queries, the per second units
	// metered would be extremely high, which can result in extremely
	// high wait times.
	// the formula for capping off ->
	// 			ftsThrottleLimit/(#indexesPerTenant * #queriesAllowed)
	if rus.Whole() > 5000 {
		rus, err = regulator.NewUnits(regulator.Search, 0, 5000/(20*10))
		if err != nil {
			return fmt.Errorf("metering: failed to cap the RUs %v\n", err)
		}
	}

	context := regulator.NewBucketCtx(bucket)
	sr.prevRBytes[pindexName] = bytes
	return regulator.RecordUnits(context, rus)
}

// Note: keeping the throttle/limiting of read and write separate,
// so that further experiments or observations may lead to
// them behaving differently from each other based on the request passed
func CheckQuotaWrite(bucket, user string,
	req interface{}) (CheckResult, time.Duration, error) {
	if !ServerlessMode {
		// no throttle/limiting checks for non-serverless versions.
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
	result, duration, err := regulator.CheckQuota(context, checkQuotaOps)
	return CheckResult(result), duration, err
}

func CheckQuotaRead(bucket, user string,
	req interface{}) (CheckResult, time.Duration, error) {
	if !ServerlessMode {
		// no throttle/limiting checks for non-serverless versions.
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

	result, duration, err := regulator.CheckQuota(context, checkQuotaOps)
	return CheckResult(result), duration, err
}

func WriteRegulatorMetrics(w http.ResponseWriter) {
	if !ServerlessMode {
		// dont write to prom http.ResponseWriter for
		// non-serverless builds.
		return
	}
	reg.handler.WriteMetrics(w)
}

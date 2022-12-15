//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !enterprise
// +build !enterprise

package cbft

import (
	"net/http"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/couchbase/cbgt"
)

func MeteringEndpointHandler(mgr *cbgt.Manager) (string, http.Handler) {
	return "", nil
}

func GetRegulatorStats() map[string]interface{} {
	return nil
}

func MeterWrites(stopCh chan struct{}, bucket string, index bleve.Index) {
	return
}

func MeterReads(bucket string, pindexName string, bytes uint64) {
	return
}

func RefreshRegulatorStats() {
	return
}

func CheckQuotaWrite(stopCh chan struct{}, bucket, user string, retry bool,
	req interface{}) (CheckResult, time.Duration, error) {
	return CheckResultProceed, 0, nil
}

func CheckQuotaRead(bucket, user string,
	req interface{}) (CheckResult, time.Duration, error) {
	return CheckResultProceed, 0, nil
}

func WriteRegulatorMetrics(w http.ResponseWriter, storageStats map[string]uint64) {
	return
}

func CheckAccess(bucket, username string) (CheckResult, error) {
	return CheckAccessNormal, nil
}

func RollbackRefund(pindex, sourceName string, bytesWrittenAtRollbackSeqno uint64) {}

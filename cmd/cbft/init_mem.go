// Copyright 2018-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/cbft"
	log "github.com/couchbase/clog"
)

var (
	ftsHerder            *appHerder
	MemoryQuotaThreshold float32 = 0.75
)

func initMemOptions(options map[string]string) (err error) {
	if options == nil {
		return nil
	}

	var memQuota uint64

	v, exists := options["ftsMemoryQuota"] // In bytes.
	if exists {
		fmq, err2 := strconv.Atoi(v)
		if err2 != nil {
			return fmt.Errorf("init_mem:"+
				" parsing ftsMemoryQuota: %q, err: %v", v, err2)
		}
		memQuota = uint64(fmq)

		memoryLimit, err := cbft.GetMemoryLimit()
		if err != nil {
			return fmt.Errorf("init_mem: error getting memory limit: %v", err)
		}
		if memoryLimit < memQuota {
			memQuota = uint64(MemoryQuotaThreshold * float32(memoryLimit))
			// multiplying by the threshold avoids the OOM error by having
			// (1-threshold)*memoryLimit as a buffer.
		}
		options["ftsMemoryQuota"] = strconv.Itoa(int(memQuota))
		log.Printf("init_mem: the FTS memory quota is: %s bytes", options["ftsMemoryQuota"])
	}

	var memCheckInterval time.Duration
	v, exists = options["memCheckInterval"] // In Go duration format.
	if exists {
		var err2 error
		memCheckInterval, err2 = time.ParseDuration(v)
		if err2 != nil {
			return fmt.Errorf("init_mem:"+
				" parsing memCheckInterval: %q, err: %v", v, err2)
		}
	}

	var goverseerKickCh chan struct{}
	if memCheckInterval > 0 && memQuota > 0 {
		g := NewGoverseer(memCheckInterval, memQuota)
		go g.Run()
		goverseerKickCh = g.kickCh
	}

	ftsApplicationFraction, err := parseFTSMemApplicationFraction(options)
	if err != nil {
		return err
	}
	ftsIndexingFraction, err := parseFTSMemIndexingFraction(options)
	if err != nil {
		return err
	}
	ftsQueryingFraction, err := parseFTSMemQueryingFraction(options)
	if err != nil {
		return err
	}

	ftsHerder = newAppHerder(memQuota, ftsApplicationFraction,
		ftsIndexingFraction, ftsQueryingFraction, goverseerKickCh)

	cbft.RegistryQueryEventCallback = ftsHerder.queryHerderOnEvent()

	cbft.OnMemoryUsedDropped = func(curMemoryUsed, prevMemoryUsed uint64) {
		ftsHerder.onMemoryUsedDropped(curMemoryUsed, prevMemoryUsed)
	}

	return nil
}

// defaultFTSApplicationFraction is default ratio for the
// memApplicationFraction of the mem quota (default 100%)
var defaultFTSApplicationFraction = 1.0

func parseFTSMemApplicationFraction(options map[string]string) (float64,
	error) {
	return parseFraction("memApplicationFraction", defaultFTSApplicationFraction,
		options)
}

// defaultFTSMemIndexingFraction is the ratio of the application quota
// to use for indexing (default 75%)
var defaultFTSMemIndexingFraction = 0.75

func parseFTSMemIndexingFraction(options map[string]string) (float64, error) {
	return parseFraction("memIndexingFraction", defaultFTSMemIndexingFraction,
		options)
}

// defaultFTSMemQueryingFraction is the ratio of the application quota
// to use for querying (default 90%)
var defaultFTSMemQueryingFraction = 0.90

func parseFTSMemQueryingFraction(options map[string]string) (float64, error) {
	return parseFraction("memQueryingFraction", defaultFTSMemQueryingFraction,
		options)
}

func parseFraction(name string, defaultValue float64,
	options map[string]string) (float64, error) {
	v, exists := options[name]
	if exists {
		p, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("init_mem: %s, err: %v", name, err)
		}
		return p, nil
	}
	return defaultValue, nil
}

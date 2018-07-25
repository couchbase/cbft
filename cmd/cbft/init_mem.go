// Copyright (c) 2018 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You
// may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/couchbase/cbft"
)

var ftsHerder *appHerder

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

	if memCheckInterval > 0 {
		g := NewGoverseer(memCheckInterval, memQuota)
		go g.Run()
	}

	// Enable query herding by default.
	queryHerdingEnabled := true
	v, exists = options["enableFtsQueryHerding"] // Boolean.
	if exists {
		var err2 error
		queryHerdingEnabled, err2 = strconv.ParseBool(v)
		if err2 != nil {
			return fmt.Errorf("init_mem:"+
				" parsing enableFtsQueryHerding: %q, err: %v", v, err2)
		}
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
		ftsIndexingFraction, ftsQueryingFraction)

	ftsHerder.setQueryHerding(queryHerdingEnabled)

	cbft.RegistryQueryEventCallback = ftsHerder.queryHerderOnEvent()

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
// to use for indexing (default 65%)
var defaultFTSMemIndexingFraction = 0.65

func parseFTSMemIndexingFraction(options map[string]string) (float64, error) {
	return parseFraction("memIndexingFraction", defaultFTSMemIndexingFraction,
		options)
}

// defaultFTSMemQueryingFraction is the ratio of the application quota
// to use for querying (default 85%)
var defaultFTSMemQueryingFraction = 0.85

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

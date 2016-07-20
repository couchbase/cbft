// Copyright (c) 2016 Couchbase, Inc.
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

// +build forestdb

package main

import (
	"fmt"
	"strconv"

	log "github.com/couchbase/clog"

	"github.com/couchbase/goforestdb"
)

func InitOptions(options map[string]string) error {
	if options == nil {
		return nil
	}

	fmq, exists := options["ftsMemoryQuota"]
	if exists {
		frac, err := ParseFTSMemoryQuotaMossFraction(options)
		if err != nil {
			return err
		}

		if frac < 1.0 {
			if frac > 0.0 {
				fmqi, err := strconv.Atoi(fmq)
				if err != nil {
					return fmt.Errorf("init_forestdb:"+
						" parsing ftsMemoryQuota: %q, err: %v", fmq, err)
				}

				fmq = fmt.Sprintf("%d", uint64((1.0-frac)*float64(fmqi)))
			}

			_, existsFBCS := options["forestdbBufferCacheSize"]
			if !existsFBCS {
				options["forestdbBufferCacheSize"] = fmq
			}
		}
	}

	// ------------------------------------------------------

	var outerErr error

	config := forestdb.DefaultConfig()
	numConfig := 0

	configInt := func(optionName string, cb func(uint64)) {
		v, exists := options["forestdb"+optionName]
		if exists {
			i, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				outerErr = err
				return
			}
			log.Printf("init_forestdb: configInt, optionName: forestdb%s, i: %d",
				optionName, i)
			cb(i)
			numConfig += 1
		}
	}

	configBool := func(optionName string, cb func(bool)) {
		v, exists := options["forestdb"+optionName]
		if exists {
			b, err := strconv.ParseBool(v)
			if err != nil {
				outerErr = err
				return
			}
			log.Printf("init_forestdb: configBool, optionName: forestdb%s, b: %t",
				optionName, b)
			cb(b)
			numConfig += 1
		}
	}

	configInt("ChunkSize", func(i uint64) {
		config.SetChunkSize(uint16(i))
	})

	configInt("BlockSize", func(i uint64) {
		config.SetBlockSize(uint32(i))
	})

	configInt("BufferCacheSize", func(i uint64) {
		config.SetBufferCacheSize(uint64(i))
	})

	configInt("WalThreshold", func(i uint64) {
		config.SetWalThreshold(uint64(i))
	})

	configBool("WalFlushBeforeCommit", func(b bool) {
		config.SetWalFlushBeforeCommit(b)
	})

	configInt("BlockSize", func(i uint64) {
		config.SetPurgingInterval(uint32(i))
	})

	configInt("SeqTreeOpt", func(i uint64) {
		config.SetSeqTreeOpt(forestdb.SeqTreeOpt(uint8(i)))
	})

	configInt("DurabilityOpt", func(i uint64) {
		config.SetDurabilityOpt(forestdb.DurabilityOpt(uint8(i)))
	})

	configInt("OpenFlags", func(i uint64) {
		config.SetOpenFlags(forestdb.OpenFlags(uint32(i)))
	})

	configInt("CompactionBufferSizeMax", func(i uint64) {
		config.SetCompactionBufferSizeMax(uint32(i))
	})

	configBool("CleanupCacheOnClose", func(b bool) {
		config.SetCleanupCacheOnClose(b)
	})

	configBool("CompressDocumentBody", func(b bool) {
		config.SetCompressDocumentBody(b)
	})

	configInt("CompactionMode", func(i uint64) {
		config.SetCompactionMode(forestdb.CompactOpt(uint8(i)))
	})

	configInt("CompactionThreshold", func(i uint64) {
		config.SetCompactionThreshold(uint8(i))
	})

	configInt("CompactionMinimumFilesize", func(i uint64) {
		config.SetCompactionMinimumFilesize(i)
	})

	configInt("CompactorSleepDuration", func(i uint64) {
		config.SetCompactorSleepDuration(i)
	})

	configInt("PrefetchDuration", func(i uint64) {
		config.SetPrefetchDuration(i)
	})

	configInt("NumWalPartitions", func(i uint64) {
		config.SetNumWalPartitions(uint16(i))
	})

	configInt("NumBcachePartitions", func(i uint64) {
		config.SetNumBcachePartitions(uint16(i))
	})

	configInt("NumCompactorThreads", func(i uint64) {
		config.SetNumCompactorThreads(int(i))
	})

	configInt("NumBgflusherThreads", func(i uint64) {
		config.SetNumBgflusherThreads(int(i))
	})

	configInt("NumBlockReusingThreshold", func(i uint64) {
		config.SetNumBlockReusingThreshold(int(i))
	})

	configInt("NumKeepingHeaders", func(i uint64) {
		config.SetNumKeepingHeaders(int(i))
	})

	configInt("MaxWriterLockProb", func(i uint64) {
		config.SetMaxWriterLockProb(uint8(i))
	})

	configBool("MultiKVInstances", func(b bool) {
		config.SetMultiKVInstances(b)
	})

	configBool("TraceLog", func(b bool) {
		forestdb.Log = forestdb.NewLeveledLog(forestdb.LogTrace)
	})

	if outerErr != nil {
		return outerErr
	}

	if numConfig <= 0 {
		return nil
	}

	return forestdb.Init(config)

	// TODO: fdb config object leakage, needs a destroy API.
}

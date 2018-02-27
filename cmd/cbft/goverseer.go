//  Copyright (c) 2018 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"log"
	"runtime"
	"runtime/debug"
	"time"
)

type Goverseer struct {
	interval time.Duration
	quota    uint64
	maxRatio float64
}

func NewGoverseer(d time.Duration, q uint64) *Goverseer {
	return &Goverseer{
		interval: d,
		quota:    q,
		maxRatio: 1.0, // this caps our targetGOGC at 100
	}
}

func (g *Goverseer) Run() {
	log.Printf("goverseer: quota: %d, interval: %s", g.quota, g.interval)
	var memstats runtime.MemStats
	intervalTicker := time.NewTicker(g.interval)
	var last = 100 // Go's default value
	for {
		select {
		case <-intervalTicker.C:
			runtime.ReadMemStats(&memstats)
			var spaceRemaining uint64
			if g.quota > memstats.HeapAlloc {
				spaceRemaining = g.quota - memstats.HeapAlloc
			}
			ratio := float64(spaceRemaining) / float64(memstats.HeapAlloc)
			if ratio > g.maxRatio {
				ratio = g.maxRatio
			}
			targetGOGC := int(ratio * 100)
			if last != targetGOGC {
				log.Printf("goverseer: new target: %d, previous: %d, heap alloc: %d, remaining: %d, ratio: %f", targetGOGC, last, memstats.HeapAlloc, spaceRemaining, ratio)
				debug.SetGCPercent(targetGOGC)
				last = targetGOGC
			}
		}
	}
}

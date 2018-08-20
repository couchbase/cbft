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
	"runtime"
	"runtime/debug"
	"time"

	log "github.com/couchbase/clog"
)

type Goverseer struct {
	interval time.Duration
	kickCh   chan struct{}
	quota    uint64
	maxRatio float64
	minRatio float64
}

func NewGoverseer(d time.Duration, q uint64) *Goverseer {
	return &Goverseer{
		interval: d,
		kickCh:   make(chan struct{}, 1),
		quota:    q,
		maxRatio: 1.0, // this caps our targetGOGC at 100
		minRatio: 0.5, // this caps our targetGOGC at 50
	}
}

func (g *Goverseer) Run() {
	log.Printf("goverseer: quota: %d, interval: %s, maxRatio: %f, minRatio: %f",
		g.quota, g.interval, g.maxRatio, g.minRatio)

	var memstats runtime.MemStats

	intervalTicker := time.NewTicker(g.interval)

	var last = 100 // Go's default value

	// Counts # of kicks we received in-between interval firings.
	var kicks = 0

	adjustGC := func(ratio float64, msg string) {
		runtime.ReadMemStats(&memstats)
		var spaceRemaining uint64
		if g.quota > memstats.HeapAlloc {
			spaceRemaining = g.quota - memstats.HeapAlloc
		}
		if ratio == 0.0 {
			ratio = float64(spaceRemaining) / float64(memstats.HeapAlloc)
			if ratio > g.maxRatio {
				ratio = g.maxRatio
			} else if ratio < g.minRatio {
				ratio = g.minRatio
			}
		}
		targetGOGC := int(ratio * 100)
		if last != targetGOGC {
			log.Printf("goverseer: SetGCPercent on %s, targetGOGC: %d, last: %d, "+
				"heapAlloc: %d, spaceRemaining: %d, ratio: %f, kicks: %d", msg,
				targetGOGC, last, memstats.HeapAlloc, spaceRemaining, ratio, kicks)
			debug.SetGCPercent(targetGOGC)
			last = targetGOGC
		}
	}

	for {
		select {
		case <-g.kickCh:
			kicks++
			if kicks == 1 {
				// On the first kick in-between interval ticks, force
				// an aggressive SetGCPercent(), which will go back to
				// normal on the next tick.
				adjustGC(g.minRatio, "kick")
			} // Else swallow any more kicks in-between interval ticks.

		case <-intervalTicker.C:
			adjustGC(0.0, "interval")

			kicks = 0
		}
	}
}

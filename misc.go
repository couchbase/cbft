//  Copyright (c) 2014 Couchbase, Inc.
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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	log "github.com/couchbaselabs/clog"
)

// Compares two dotted versioning string and returns true when x >= y.
func VersionGTE(x, y string) bool {
	xa := strings.Split(x, ".")
	ya := strings.Split(y, ".")
	for i := range xa {
		if i >= len(ya) {
			return true
		}
		xv, err := strconv.Atoi(xa[i])
		if err != nil {
			return false
		}
		yv, err := strconv.Atoi(ya[i])
		if err != nil {
			return false
		}
		if xv < yv {
			return false
		}
	}
	return len(xa) >= len(ya)
}

func NewUUID() string {
	val1 := rand.Int63()
	val2 := rand.Int63()
	uuid := fmt.Sprintf("%x%x", val1, val2)
	return uuid[0:16]
}

// Calls f() in a loop, sleeping in an exponential backoff if needed.
// The provided f() function should return < 0 to stop the loop; >= 0
// to continue the loop, where > 0 means there was progress which
// allows an immediate retry of f() with no sleeping.  A return of < 0
// is useful when f() will never make any future progress.
func ExponentialBackoffLoop(name string,
	f func() int,
	startSleepMS int,
	backoffFactor float32,
	maxSleepMS int) {
	nextSleepMS := startSleepMS
	for {
		progress := f()
		if progress < 0 {
			return
		}
		if progress > 0 {
			// When there was some progress, we can reset nextSleepMS.
			log.Printf("backoff: %s, progress: %d", name, progress)
			nextSleepMS = startSleepMS
		} else {
			// If zero progress was made this cycle, then sleep.
			log.Printf("backoff: %s, sleep: %d (ms)", name, nextSleepMS)
			time.Sleep(time.Duration(nextSleepMS) * time.Millisecond)

			// Increase nextSleepMS in case next time also has 0 progress.
			nextSleepMS = int(float32(nextSleepMS) * backoffFactor)
			if nextSleepMS > maxSleepMS {
				nextSleepMS = maxSleepMS
			}
		}
	}
}

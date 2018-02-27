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
)

func initMemOptions(options map[string]string) (err error) {
	if options == nil {
		return nil
	}

	var memQuota uint64
	v, exists := options["ftsMemoryQuota"] // In bytes.
	if exists {
		fmq, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("init_mem:"+
				" parsing ftsMemoryQuota: %q, err: %v", v, err)
		}
		memQuota = uint64(fmq)
	}

	memCheckInterval := time.Second
	v, exists = options["memCheckInterval"] // In Go duration format.
	if exists {
		var err error
		memCheckInterval, err = time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("init_mem:"+
				" parsing numCheckInterval: %q, err: %v", v, err)
		}
	}

	if memCheckInterval > 0 {
		g := NewGoverseer(memCheckInterval, memQuota)
		go g.Run()
	}

	return nil
}

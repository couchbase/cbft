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
	"log"
	"sort"

	"github.com/couchbaselabs/cbft"

	cbftCmd "github.com/couchbaselabs/cbft/cmd/cbft"
)

// Emits markdown docs of cbft's REST API.
func main() {
	_, meta, err :=
		cbft.NewManagerRESTRouter(cbftCmd.VERSION, nil, "", "", nil)
	if err != nil {
		log.Panic(err)
	}

	paths := []string(nil)
	for path := range meta {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		m := meta[path]

		fmt.Printf("# %s\n\n", path)
		fmt.Printf("method: %s\n\n", m.Method)
		if m.Opts != nil && m.Opts[""] != "" {
			fmt.Printf("%s\n\n", m.Opts[""])
		}

		optNames := []string(nil)
		for optName := range m.Opts {
			if optName != "" {
				optNames = append(optNames, optName)
			}
		}
		sort.Strings(optNames)

		for _, optName := range optNames {
			fmt.Printf("**%s** - %s\n\n", optName, m.Opts[optName])
		}
	}
}

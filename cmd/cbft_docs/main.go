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
	"strings"

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

	categoriesMap := map[string]bool{}
	categories := []string(nil)

	paths := []string(nil)
	for path := range meta {
		paths = append(paths, path)

		m := meta[path]
		if m.Opts != nil {
			if !categoriesMap[m.Opts["_category"]] {
				categoriesMap[m.Opts["_category"]] = true
				categories = append(categories, m.Opts["_category"])
			}
		}
	}

	sort.Strings(categories)
	sort.Strings(paths)

	for _, category := range categories {
		if category != "" {
			ca := strings.Split(category, "/")
			fmt.Printf("# %s\n\n", ca[len(ca)-1])
		} else {
			fmt.Printf("# General\n\n")
		}

		for _, path := range paths {
			m := meta[path]
			if m.Opts != nil {
				if m.Opts["_category"] != category ||
					m.Opts["_status"] == "private" {
					continue
				}
			}

			fmt.Printf("### %s\n\n", path)
			fmt.Printf("**method**: %s\n\n", m.Method)
			if m.Opts != nil && m.Opts["_about"] != "" {
				fmt.Printf("%s\n\n", m.Opts["_about"])
			}

			optNames := []string(nil)
			for optName := range m.Opts {
				if optName != "" && !strings.HasPrefix(optName, "_") {
					optNames = append(optNames, optName)
				}
			}
			sort.Strings(optNames)

			for _, optName := range optNames {
				fmt.Printf("**%s**: %s\n\n", optName, m.Opts[optName])
			}
		}
	}
}

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

func categoryParse(categoryFull string) (string, string, string, string) {
	ma := strings.Split(categoryFull, "|")

	mainCategory := ma[0]
	mainCategoryParts := strings.Split(mainCategory, "/")
	mainCategoryVis := mainCategoryParts[len(mainCategoryParts)-1]

	subCategory := ma[1]
	subCategoryParts := strings.Split(subCategory, "/")
	subCategoryVis := subCategoryParts[len(subCategoryParts)-1]

	return mainCategory, mainCategoryVis, subCategory, subCategoryVis
}

// Emits markdown docs of cbft's REST API.
func main() {
	_, meta, err :=
		cbft.NewManagerRESTRouter(cbftCmd.VERSION, nil, "", "", nil)
	if err != nil {
		log.Panic(err)
	}

	mainCategoriesMap := map[string]bool{}
	mainCategories := []string(nil)

	subCategoriesMap := map[string]bool{}
	subCategories := []string(nil)

	paths := []string(nil)
	for path := range meta {
		paths = append(paths, path)

		m := meta[path]
		if m.Opts != nil {
			category := m.Opts["_category"]

			mainCategory, _, subCategory, _ := categoryParse(category)

			if !mainCategoriesMap[mainCategory] {
				mainCategoriesMap[mainCategory] = true
				mainCategories = append(mainCategories, mainCategory)
			}

			if !subCategoriesMap[subCategory] {
				subCategoriesMap[subCategory] = true
				subCategories = append(subCategories, subCategory)
			}
		}
	}

	sort.Strings(mainCategories)
	sort.Strings(subCategories)
	sort.Strings(paths)

	for _, mainCategory := range mainCategories {
		mainCategoryFirst := true

		for _, subCategory := range subCategories {
			subCategoryFirst := true

			for _, path := range paths {
				m := meta[path]

				category := ""
				if m.Opts != nil {
					category = m.Opts["_category"]
				}

				mc, mcVis, sc, scVis := categoryParse(category)
				if mc != mainCategory ||
					sc != subCategory {
					continue
				}

				if m.Opts != nil &&
					m.Opts["_status"] == "private" {
					continue
				}

				if mainCategoryFirst {
					fmt.Printf("# %s\n\n", mcVis)
				}
				mainCategoryFirst = false

				if subCategoryFirst {
					fmt.Printf("## %s\n\n", scVis)
				}
				subCategoryFirst = false

				fmt.Printf("---\n")
				fmt.Printf("**%s**\n\n", path)
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
}

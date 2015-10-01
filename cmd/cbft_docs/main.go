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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/gorilla/mux"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"

	cbftCmd "github.com/couchbase/cbft/cmd/cbft"
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

func sampleRequest(router *mux.Router,
	method string, urlPath string, body []byte) []byte {
	req := &http.Request{
		Method: method,
		URL:    &url.URL{Path: urlPath},
		Form:   url.Values(nil),
		Body:   ioutil.NopCloser(bytes.NewBuffer(body)),
	}
	record := httptest.NewRecorder()
	router.ServeHTTP(record, req)
	if record.Code != 200 {
		return nil
	}
	return record.Body.Bytes()
}

var skipSampleResponses = map[string]bool{
	"/api/managerMeta":      true,
	"/api/diag":             true,
	"/api/runtime/args":     true,
	"/api/runtime/stats":    true,
	"/api/runtime/statsMem": true,
}

// Emits markdown docs of cbft's REST API.
func main() {
	rand.Seed(0)

	dataDir, _ := ioutil.TempDir("./tmp", "data")
	defer os.RemoveAll(dataDir)

	cfg := cbgt.NewCfgMem()
	tags := []string(nil)
	container := ""
	weight := 1
	extras := ""
	bindHttp := "0.0.0.0:8095"

	mgr := cbgt.NewManager(cbgt.VERSION, cfg, cbgt.NewUUID(),
		tags, container, weight, extras, bindHttp,
		dataDir, "http://localhost:8091", nil)

	sourceType := "nil"
	sourceName := ""
	sourceUUID := ""
	sourceParams := ""
	indexType := "blackhole"
	indexName := "myFirstIndex"
	indexParams := ""
	prevIndexUUID := ""

	mgr.Start("wanted")

	err := mgr.CreateIndex(
		sourceType, sourceName, sourceUUID, sourceParams,
		indexType, indexName, indexParams,
		cbgt.PlanParams{}, prevIndexUUID)
	if err != nil {
		log.Fatalf("could not create myFirstIndex, err: %v", err)
	}

	staticDir := ""
	staticETag := ""

	mr, _ := cbgt.NewMsgRing(ioutil.Discard, 1)

	router, meta, err :=
		cbft.NewRESTRouter(cbftCmd.VERSION, mgr,
			staticDir, staticETag, mr)
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

	fmt.Printf("# API Reference\n\n")

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
					fmt.Printf("---\n\n# %s\n\n", mcVis)
				}
				mainCategoryFirst = false

				if subCategoryFirst {
					fmt.Printf("---\n\n## %s\n\n", scVis)
				}
				subCategoryFirst = false

				pathParts := strings.Split(path, " ")
				fmt.Printf("---\n\n")
				fmt.Printf("%s `%s`\n\n", m.Method, pathParts[0])

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

				if m.Opts != nil && m.Opts[""] != "" {
					fmt.Printf("%s\n\n", m.Opts[""])
				}

				if m.Method == "GET" && !skipSampleResponses[pathParts[0]] {
					url := strings.Replace(pathParts[0],
						"{indexName}", "myFirstIndex", 1)

					res := sampleRequest(router, m.Method, url, nil)
					if res != nil {
						var j map[string]interface{}

						err = json.Unmarshal(res, &j)
						if err == nil {
							s, err := json.MarshalIndent(j, "    ", "  ")
							if err == nil {
								fmt.Printf("Sample response:\n\n    %s\n\n", s)
							}
						}
					}
				}
			}
		}
	}

	fmt.Printf("---\n\nCopyright (c) 2015 Couchbase, Inc.\n")
}

//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	_ "github.com/blevesearch/bleve/index/store/metrics"
	"github.com/couchbase/moss"

	bleveMoss "github.com/blevesearch/bleve/index/store/moss"
)

var indexPath = flag.String("index", "", "index path")
var check = flag.Int("check", 100, "check this many terms (default 100)")
var field = flag.String("field", "", "check only this field")

func init() {
	bleveMoss.RegistryCollectionOptions["fts"] = moss.CollectionOptions{
		Log: log.Printf,
		OnError: func(err error) {
			log.Printf("moss OnError, err: %v", err)
		},
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, strings.TrimSpace(`
bleve_check performs various consistency checks on the
index specified by -index.
`)+"\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if len(flag.Args()) > 0 {
		log.Fatalf("unexpected argument '%s', use -help to see possible options",
			flag.Args()[0])
	}
	if *indexPath == "" {
		log.Fatal("specify index to check")
	}

	index, err := bleve.Open(*indexPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		cerr := index.Close()
		if cerr != nil {
			log.Fatalf("error closing index: %v", err)
		}
	}()

	var fieldNames []string
	if *field == "" {
		fieldNames, err = index.Fields()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("fields: %d\n", len(fieldNames))
	} else {
		fmt.Printf("only checking field: '%s'", *field)
		fieldNames = []string{*field}
	}

	totalProblems := 0
	for _, fieldName := range fieldNames {
		fmt.Printf("checking field: '%s'\n", fieldName)
		problems, err := checkField(index, fieldName)
		if err != nil {
			log.Fatal(err)
		}
		totalProblems += problems
	}

	fmt.Printf("found %d total problems\n", totalProblems)
	if totalProblems != 0 {
		os.Exit(1)
	}
}

func checkField(index bleve.Index, fieldName string) (int, error) {
	termDictionary, err := getDictionary(index, fieldName)
	if err != nil {
		return 0, err
	}
	fmt.Printf("field contains %d terms\n", len(termDictionary))

	numTested := 0
	numProblems := 0
	for term, count := range termDictionary {
		fmt.Printf("checked %d terms\r", numTested)
		if *check > 0 && numTested >= *check {
			break
		}

		tq := bleve.NewTermQuery(term)
		tq.SetField(fieldName)
		req := bleve.NewSearchRequest(tq)
		req.Size = 0
		res, err := index.Search(req)
		if err != nil {
			return 0, err
		}

		if res.Total != count {
			fmt.Printf("unexpected mismatch for term '%s', dictionary %d, search hits %d\n", term, count, res.Total)
			numProblems++
		}

		numTested++
	}
	fmt.Printf("done checking %d terms, found %d problems\n", numTested, numProblems)

	return numProblems, nil
}

func getDictionary(index bleve.Index, field string) (map[string]uint64, error) {
	rv := make(map[string]uint64)
	i, _, err := index.Advanced()
	if err != nil {
		log.Fatal(err)
	}
	r, err := i.Reader()
	if err != nil {
		log.Fatal(err)
	}
	d, err := r.FieldDict(field)
	if err != nil {
		log.Fatal(err)
	}

	de, err := d.Next()
	for err == nil && de != nil {
		rv[de.Term] = de.Count
		de, err = d.Next()
	}
	if err != nil {
		return nil, err
	}
	return rv, nil
}

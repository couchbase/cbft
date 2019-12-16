//  Copyright (c) 2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cbft

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve"
)

func TestDeleteByQuery(t *testing.T) {
	bleve.Config.DefaultIndexType = "scorch"
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := bleve.New("testidx", bleve.NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	batch := index.NewBatch()
	doca := map[string]interface{}{
		"name": "couchbase fts",
		"desc": "awesome search functionality",
	}
	err = batch.Index("a", doca)
	if err != nil {
		t.Error(err)
	}

	docy := map[string]interface{}{
		"name": "couchbase fts dev",
		"desc": "steve abhi sreekanth",
	}
	err = batch.Index("y", docy)
	if err != nil {
		t.Error(err)
	}

	docx := map[string]interface{}{
		"name": "couchbase fts dead",
		"desc": "keshav",
	}
	err = batch.Index("x", docx)
	if err != nil {
		t.Error(err)
	}

	docb := map[string]interface{}{
		"name": "couchbase fts test",
		"desc": "girish master",
	}
	err = batch.Index("b", docb)
	if err != nil {
		t.Error(err)
	}

	err = index.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	// close the index, open it again
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err = bleve.Open("testidx")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	count, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 4 {
		t.Errorf("expected doc count 4, got %d", count)
	}

	// delete all documents which has the text "couchbase"
	cleaner := newDelByQueryHandler(index)
	mq := bleve.NewMatchQuery("couchbase")
	mq.Analyzer = "keyword"
	pCount, err := cleaner.deleteByQuery(mq)
	if err != nil {
		t.Fatal(err)
	}
	if pCount != 4 {
		t.Errorf("expected deleted doc count is 4, got %d", pCount)
	}

	count, err = index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected doc count 0, got %d", count)
	}
	batch.Reset()

	// index some more data
	doca = map[string]interface{}{
		"name": "full text search",
		"desc": "work in progress",
	}
	err = batch.Index("a", doca)
	if err != nil {
		t.Error(err)
	}
	docb = map[string]interface{}{
		"name": "full text search",
		"desc": "work in progress",
	}
	err = batch.Index("b", docb)
	if err != nil {
		t.Error(err)
	}
	docd := map[string]interface{}{
		"name": "couchbase",
		"desc": "work in progress",
	}
	err = batch.Index("d", docd)
	if err != nil {
		t.Error(err)
	}
	err = index.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	// delete all documents which has the text "search"
	cleaner = newDelByQueryHandler(index)
	mq = bleve.NewMatchQuery("search")
	mq.Analyzer = "keyword"
	pCount, err = cleaner.deleteByQuery(mq)
	if err != nil {
		t.Fatal(err)
	}
	if pCount != 2 {
		t.Errorf("expected deleted doc count is 2, got %d", pCount)
	}

	count, err = index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected doc count 1, got %d", count)
	}
}

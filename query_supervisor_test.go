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

package cbft

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
)

func TestQuerySupervisor(t *testing.T) {
	// initial set
	for i := 0; i < 50; i++ {
		querySupervisor.AddEntry(nil)
	}

	done := make(chan struct{})

	// inserts' routine
	go func() {
		for i := 0; i < 100; i++ {
			querySupervisor.AddEntry(nil)
		}
		done <- struct{}{}
	}()

	// deletes' routine
	go func() {
		for i := uint64(1); i <= 50; i++ {
			querySupervisor.DeleteEntry(i)
		}
		done <- struct{}{}
	}()

	<-done
	<-done

	if querySupervisor.id != 150 {
		t.Errorf("ID: %v, not expected!", querySupervisor.id)
	}

	if querySupervisor.Count() != 100 {
		t.Errorf("Count: %v, not expected!", querySupervisor.Count())
	}

	if _, ok := querySupervisor.ExecutionTime(querySupervisor.id); !ok {
		t.Errorf("Expected id: %v to exist!", querySupervisor.id)
	}
}

func BenchmarkListLongerThan(b *testing.B) {

	for i := 0; i < 100; i++ {
		querySupervisor.AddEntry(&QuerySupervisorContext{
			Query:     bleve.NewMatchQuery("test"),
			Cancel:    nil,
			Size:      10,
			From:      0,
			Timeout:   10,
			IndexName: "dummy",
		})
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		done := make(chan struct{})

		// reads' routine
		go func() {
			for i := 0; i < 50; i++ {
				_ = querySupervisor.ListLongerThan(0, "")
			}
			done <- struct{}{}
		}()

		// inserts' routine
		go func() {
			for i := 0; i < 100; i++ {
				querySupervisor.AddEntry(&QuerySupervisorContext{
					Query:     bleve.NewMatchQuery("test"),
					Cancel:    nil,
					Size:      10,
					From:      0,
					Timeout:   10,
					IndexName: "dummy",
				})
			}
			done <- struct{}{}
		}()

		<-done
		<-done
	}
}

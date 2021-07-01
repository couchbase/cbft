//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
				_, _ = querySupervisor.ListLongerThanWithQueryCount(0, "")
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

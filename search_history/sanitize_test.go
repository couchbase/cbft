//  Copyright 2025-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package search_history

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSanitizeRequestBody(t *testing.T) {
	input := `{
				"query": "test",
				"knn": [{
					"field": "embedding",
			"vector": [0.1, 0.2, 0.3],
					"k": 10
				}]
	}`

	// Test vector exclusion (always enabled)
	result, err := sanitizeRequestBody([]byte(input))
	if err != nil {
		t.Fatalf("sanitize failed: %v", err)
	}

	// Verify valid JSON
	var js map[string]interface{}
	if err := json.Unmarshal([]byte(result), &js); err != nil {
		t.Errorf("result is not valid JSON: %v", err)
	}

	// Verify vectors excluded
	if strings.Contains(result, "[0.1") {
		t.Error("expected vectors to be excluded")
	}

	// Verify knn structure preserved
	if _, ok := js["knn"]; !ok {
		t.Error("knn structure should be preserved")
	}
}

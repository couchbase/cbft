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
	"fmt"
)

var VectorFieldNames = []string{
	"vector",
	"vector_base64",
}

// Removes vector embeddings from the request JSON
func sanitizeRequestBody(requestBody []byte) (string, error) {
	var requestMap map[string]interface{}
	if err := json.Unmarshal(requestBody, &requestMap); err != nil {
		return "", err
	}

	removeVectors(requestMap)

	sanitized, err := json.Marshal(requestMap)
	if err != nil {
		return "", fmt.Errorf("failed to marshal sanitized body: %v", err)
	}

	return string(sanitized), nil
}

func removeVectors(m map[string]interface{}) {
	for _, fieldName := range VectorFieldNames {
		if _, exists := m[fieldName]; exists {
			m[fieldName] = "..."
		}
	}

	for _, value := range m {
		switch v := value.(type) {
		case map[string]interface{}:
			removeVectors(v)
		case []interface{}:
			for _, item := range v {
				if itemMap, ok := item.(map[string]interface{}); ok {
					removeVectors(itemMap)
				}
			}
		}
	}
}

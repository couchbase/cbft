//  Copyright (c) 2016 Couchbase, Inc.
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
	"regexp"
	"testing"
)

func TestBleveDocConfigDetermineType(t *testing.T) {

	tests := []struct {
		key          []byte
		val          interface{}
		config       *BleveDocumentConfig
		expectedType string
	}{
		{
			key: []byte("anything"),
			val: map[string]interface{}{
				"type": "beer",
			},
			config: &BleveDocumentConfig{
				Mode:      "type_field",
				TypeField: "type",
			},
			expectedType: "beer",
		},
		{
			key: []byte("anything"),
			val: map[string]interface{}{},
			config: &BleveDocumentConfig{
				Mode:      "type_field",
				TypeField: "type",
			},
			expectedType: "_default",
		},
		{
			key: []byte("beer-123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:        "docid_regexp",
				DocIDRegexp: regexp.MustCompile(`^[^-]+`),
			},
			expectedType: "beer",
		},
		{
			key: []byte("-123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:        "docid_regexp",
				DocIDRegexp: regexp.MustCompile(`^[^-]+`),
			},
			expectedType: "_default",
		},
		{
			key: []byte("beer-123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:             "docid_prefix",
				DocIDPrefixDelim: "-",
			},
			expectedType: "beer",
		},
		{
			key: []byte("beer::123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:             "docid_prefix",
				DocIDPrefixDelim: "-",
			},
			expectedType: "_default",
		},
	}

	for _, test := range tests {
		actualType := test.config.determineType(test.key, test.val, "_default")
		if actualType != test.expectedType {
			t.Fatalf("expected type: '%s', got '%s'", test.expectedType, actualType)
		}
	}

}

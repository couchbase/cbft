//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
		{
			key: []byte("beer::123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:             "scope.collection.docid_prefix",
				DocIDPrefixDelim: "-",
			},
			expectedType: "_default",
		},
		{
			key: []byte("beer-123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:             "scope.collection.docid_prefix",
				DocIDPrefixDelim: "-",
			},
			expectedType: "beer",
		},
		{
			key: []byte("anything"),
			val: map[string]interface{}{
				"type": "beer",
			},
			config: &BleveDocumentConfig{
				Mode:      "scope.collection.type_field",
				TypeField: "type",
			},
			expectedType: "beer",
		},
		{
			key: []byte("beer-123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:        "scope.collection.docid_regexp",
				DocIDRegexp: regexp.MustCompile(`^[^-]+`),
			},
			expectedType: "beer",
		},
		{
			key: []byte("beer::123"),
			val: map[string]interface{}{
				"type": "notbeer",
			},
			config: &BleveDocumentConfig{
				Mode:             "scope.collection",
				DocIDPrefixDelim: "-",
			},
			expectedType: "_default",
		},
	}

	for _, test := range tests {
		actualType := test.config.DetermineType(test.key, test.val, "_default")
		if actualType != test.expectedType {
			t.Fatalf("expected type: '%s', got '%s'", test.expectedType, actualType)
		}
	}

}

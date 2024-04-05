//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
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

func TestBuildXattrs(t *testing.T) {

	tests := []struct {
		xattrs      map[string]interface{}
		errBytes    []byte
		excessInput []byte
		err         bool
	}{
		{
			xattrs: map[string]interface{}{
				"field": "value",
			},
			excessInput: nil,
			err:         false,
		},
		{
			xattrs: map[string]interface{}{
				"field": 1,
			},
			excessInput: nil,
			err:         false,
		},
		{
			xattrs: map[string]interface{}{
				"field": "value",
			},
			excessInput: []byte(`{"mainField":"mainVal"}`),
			err:         false,
		},
		{
			xattrs:      map[string]interface{}{},
			excessInput: []byte(`{"mainField":"mainVal"}`),
			err:         false,
		},
		{
			xattrs: map[string]interface{}{
				"field1": "value1",
				"field2": 2,
			},
			excessInput: []byte(`{"mainField":"mainVal"}`),
			err:         false,
		},
		{
			xattrs: map[string]interface{}{
				"prop": map[string]interface{}{
					"field1": "value1",
					"field2": 2,
				},
			},
			excessInput: []byte(`{"mainField":"mainVal"}`),
			err:         false,
		},
		{
			xattrs: map[string]interface{}{
				"field1": "value1",
				"field2": 2,
				"prop1": map[string]interface{}{
					"propField1": "propValue1",
					"propField2": 2,
				},
			},
			excessInput: []byte(`{"mainField":"mainVal"}`),
			err:         false,
		},
		{
			// val < 4 bytes
			xattrs: nil,
			errBytes: []byte{
				0, 0, 0,
			},
			excessInput: nil,
			err:         true,
		},
		{
			// length of mutation less than required
			xattrs: nil,
			errBytes: []byte{
				0, 0, 0, 8,
				0, 0, 0, 4,
			},
			excessInput: nil,
			err:         true,
		},
		{
			// wrong number of separators
			xattrs: nil,
			errBytes: []byte{
				0, 0, 0, 16,
				0, 0, 0, 12,
			},
			excessInput: []byte(`{"mainField":"mainVal"}`),
			err:         true,
		},
	}

	docConfig := BleveDocumentConfig{}
	for _, test := range tests {
		var totalXattrsBytes []byte

		if test.errBytes == nil {
			totalXattrsBytes = make([]byte, 4)
			for field, val := range test.xattrs {

				totalFieldBytes := make([]byte, 4)

				valBytes, err := json.Marshal(val)
				if err != nil {
					t.Fatal(err)
				}

				totalFieldBytes = append(totalFieldBytes, field...)
				totalFieldBytes = append(totalFieldBytes, []byte("\x00")...)
				totalFieldBytes = append(totalFieldBytes, valBytes...)
				totalFieldBytes = append(totalFieldBytes, []byte("\x00")...)

				binary.BigEndian.PutUint32(totalFieldBytes, uint32(len(totalFieldBytes)-4))

				totalXattrsBytes = append(totalXattrsBytes, totalFieldBytes...)
			}

			binary.BigEndian.PutUint32(totalXattrsBytes, uint32(len(totalXattrsBytes)))
		} else {
			totalXattrsBytes = test.errBytes
		}

		if test.excessInput != nil {
			totalXattrsBytes = append(totalXattrsBytes, test.excessInput...)
		}

		xattrs, val, err := docConfig.buildXAttrs(totalXattrsBytes)
		if err != nil {
			if test.err != true || xattrs != nil {
				t.Fatalf("Unexpected Results for Test - %v\n", test)
			}
		} else {
			if test.err != false {
				t.Fatalf("Unexpected Results for Test - %v\n", test)
			}

			testJSON, err := json.Marshal(test.xattrs)
			if err != nil {
				t.Fatal(err)
			}

			resJSON, err := json.Marshal(xattrs)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(testJSON, resJSON) {
				t.Fatalf("Unexpected Results for Test - %v\n", test)
			}

			if !bytes.Equal(test.excessInput, val) {
				t.Fatalf("Unexpected Results for Test - %v\n", test)
			}
		}
	}
}

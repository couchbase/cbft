//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import "testing"

func TestConvertStatName(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			"/x/y/z",
			"y_z",
		},
	}

	for _, test := range tests {
		actual := convertStatName(test.in)
		if actual != test.out {
			t.Errorf("expected '%s' got '%s' for '%s'", test.out, actual, test.in)
		}
	}
}

func TestCamelToUnderscore(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			"ThisIsCamelCase",
			"this_is_camel_case",
		},
	}

	for _, test := range tests {
		actual := camelToUnderscore(test.in)
		if actual != test.out {
			t.Errorf("expected '%s' got '%s' for '%s'", test.out, actual, test.in)
		}
	}
}

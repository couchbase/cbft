//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package main

import (
	"testing"
)

func TestEmptyPartitionFunc(t *testing.T) {
	stream := make(Stream)
	s, err := EmptyPartitionFunc(nil, "", map[string]Stream{"": stream})
	if err != nil || s != stream {
		t.Errorf("expected TestEmptyPartitionFunc to work")
	}
	s, err = EmptyPartitionFunc(nil, "", map[string]Stream{"foo": stream})
	if err == nil || s == stream {
		t.Errorf("expected TestEmptyPartitionFunc to not work")
	}
}

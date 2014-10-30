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
	"fmt"
)

func DataSourcePartitions(sourceType, sourceName,
	sourceUUID, sourceParams string) ([]string, error) {
	if sourceType == "couchbase" {
		// TODO: Should connect to couchbase source and get the number of vbuckets.
		return []string{}, nil
	}
	if sourceType == "simple" {
		// TODO: Should look at sourceParams for # of partitions.
		return []string{}, nil
	}
	return nil, fmt.Errorf("error: DataSourcePartitions got unknown sourceType: %s",
		sourceType)
}

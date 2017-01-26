//  Copyright (c) 2017 Couchbase, Inc.
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
	"encoding/json"
)

func ParseExtras(s string) (map[string]string, error) {
	rv := map[string]string{}

	err := json.Unmarshal([]byte(s), &rv)
	if err != nil {
		// Early versions of cbft-to-ns_server integration had a
		// simple, non-JSON "host:port" format for the nodeDef.Extras,
		// so fall back to that.
		rv["nsHostPort"] = s
	}

	return rv, nil
}

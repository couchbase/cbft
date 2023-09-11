//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

func ParseExtras(s string) (map[string]string, error) {
	rv := map[string]string{}

	err := UnmarshalJSON([]byte(s), &rv)
	if err != nil {
		// Early versions of cbft-to-ns_server integration had a
		// simple, non-JSON "host:port" format for the nodeDef.Extras,
		// so fall back to that.
		rv["nsHostPort"] = s
	}

	return rv, nil
}

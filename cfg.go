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

// Cfg is the interface that configuration providers must implement.
type Cfg interface {
	// Get retrieves an entry from the Cfg.  A zero cas means don't do
	// a CAS match on Get(), and a non-zero cas value means the Get()
	// will succeed only if the CAS matches.
	Get(key string, cas uint64) (val []byte, casSuccess uint64, err error)

	// Set creates or updates an entry in the Cfg.  A non-zero cas
	// that does not match will result in an error.  A zero cas means
	// the Set() operation must be an entry creation, where a zero cas
	// Set() will error if the entry already exists.
	Set(key string, val []byte, cas uint64) (casSuccess uint64, err error)

	// Del removes an entry from the Cfg.  A non-zero cas that does
	// not match will result in an error.  A zero cas means a CAS
	// match will be skipped, so that clients can perform a
	// "don't-care, out-of-the-blue" deletion.
	Del(key string, cas uint64) error

	// Subscribe allows clients to receive events on changes to a key.
	// During a deletion event, the CfgEvent.CAS field will be 0.
	Subscribe(key string, ch chan CfgEvent) error

	// Refresh forces the Cfg implementation to reload from its
	// backend-specific data source, clearing any locally cached data.
	// Any subscribers will receive events on a Refresh, where it's up
	// to subscribers to detect if there were actual changes or not.
	Refresh() error
}

// The error used on mismatches of CAS (compare and set/swap) values.
type CfgCASError struct{}

func (e *CfgCASError) Error() string { return "CAS mismatch" }

// See the Cfg.Subscribe() method.
type CfgEvent struct {
	Key string
	CAS uint64
}

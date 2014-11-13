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
	// A zero cas means don't do a CAS match on Get(), and a non-zero
	// cas value means the Get() will only succeed if the CAS matches.
	Get(key string, cas uint64) (val []byte, casSuccess uint64, err error)

	// A zero cas means the Set() operation must be an entry creation.
	// So, a zero cas Set() will error if the entry already exists.  A
	// non-zero, non-matching cas value will error.
	Set(key string, val []byte, cas uint64) (casSuccess uint64, err error)

	// A zero cas means don't match CAS on Del().  Otherwise, a
	// non-zero, non-matching cas value will error.
	Del(key string, cas uint64) error

	// Subscriptions to changes to a key.  During a deletion event,
	// the CfgEvent.CAS field will be 0.
	Subscribe(key string, ch chan CfgEvent) error
}

// The error used on mismatches of CAS (compare and set/swap) values.
type CfgCASError struct{}

func (e *CfgCASError) Error() string { return "CAS mismatch" }

// See the Cfg.Subscribe() method.
type CfgEvent struct {
	Key string
	CAS uint64
}

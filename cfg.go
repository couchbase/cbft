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

type Cfg interface {
	// A zero cas means don't do a CAS match on Get().
	Get(key string, cas uint64) (val []byte, casSuccess uint64, err error)

	// A zero cas means must be a create.
	Set(key string, val []byte, cas uint64) (casSuccess uint64, err error)

	// A zero cas means don't match CAS on Del().
	Del(key string, cas uint64) error

	// Ephemeral subscriptions to changes to a key.
	// During a deletion, the CfgEvent.CAS will be 0.
	Subscribe(key string, ch chan CfgEvent) error
}

type CfgCASError struct{}

func (e *CfgCASError) Error() string {
	return "CAS mismatch"
}

type CfgEvent struct {
	Key string
	CAS uint64
}

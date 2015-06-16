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

package cbft

const WORK_NOOP = ""
const WORK_KICK = "kick"

// A workReq represents an asynchronous request for work or a task,
// where results can be awaited upon via the resCh.
type workReq struct {
	op    string      // The operation.
	msg   string      // Some simple msg as part of the request.
	obj   interface{} // Any extra params for the request.
	resCh chan error  // Response/result channel.
}

// syncWorkReq makes a workReq request and synchronously awaits for a
// resCh response.
func syncWorkReq(ch chan *workReq, op, msg string, obj interface{}) error {
	resCh := make(chan error)
	ch <- &workReq{op: op, msg: msg, obj: obj, resCh: resCh}
	return <-resCh
}

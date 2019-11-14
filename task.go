//  Copyright (c) 2020 Couchbase, Inc.
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
	"context"
	"fmt"
	"sync"
)

// taskCache is a runtime cache for tracking the
// asynchronous tasks which are in progress.
type taskCache struct {
	m     sync.RWMutex
	cache map[string]context.CancelFunc // uuid to ctx
}

var liveTasks *taskCache

func init() {
	liveTasks = &taskCache{
		cache: make(map[string]context.CancelFunc),
	}
}

func (at *taskCache) tasks() []string {
	rv := make([]string, len(at.cache))
	at.m.RLock()
	for k := range at.cache {
		rv = append(rv, k)
	}
	at.m.RUnlock()
	return rv
}

func (at *taskCache) add(key string, cancel context.CancelFunc) {
	at.m.Lock()
	at.cache[key] = cancel
	at.m.Unlock()
}

func (at *taskCache) taskCompleted(key string) {
	at.m.Lock()
	delete(at.cache, key)
	at.m.Unlock()
}

func (at *taskCache) cancelTask(key string) error {
	at.m.Lock()
	defer at.m.Unlock()
	if cancelFunc, ok := at.cache[key]; ok {
		cancelFunc()
		delete(at.cache, key)
		return nil
	}
	return fmt.Errorf("task %s is no longer running", key)
}

func cancelTask(key string) error {
	return liveTasks.cancelTask(key)
}

func updateTaskStart(key string, cancel context.CancelFunc) {
	liveTasks.add(key, cancel)
}

func updateTaskFinish(key string) {
	liveTasks.taskCompleted(key)
}

func backgroundTasks() []string {
	return liveTasks.tasks()
}

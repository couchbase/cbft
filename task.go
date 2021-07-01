//  Copyright 2020-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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

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
	"io"
	"sync"
)

type MsgRing struct {
	m     sync.Mutex
	inner io.Writer
	Next  int      `json:"next"`
	Msgs  [][]byte `json:"msgs"`
}

func NewMsgRing(inner io.Writer, ringSize int) (*MsgRing, error) {
	if inner == nil {
		return nil, fmt.Errorf("need a non-nil inner io.Writer param")
	}
	if ringSize <= 0 {
		return nil, fmt.Errorf("need a positive ring size")
	}
	return &MsgRing{
		inner: inner,
		Next:  0,
		Msgs:  make([][]byte, ringSize),
	}, nil
}

// Implements the io.Writer interface.
func (m *MsgRing) Write(p []byte) (n int, err error) {
	m.m.Lock()

	m.Msgs[m.Next] = append([]byte(nil), p...) // Copy p.
	m.Next += 1
	if m.Next >= len(m.Msgs) {
		m.Next = 0
	}

	m.m.Unlock()

	return m.inner.Write(p)
}

func (m *MsgRing) Messages() [][]byte {
	rv := make([][]byte, 0, len(m.Msgs))

	m.m.Lock()
	defer m.m.Unlock()

	n := len(m.Msgs)
	i := 0
	idx := m.Next
	for i < n {
		if msg := m.Msgs[idx]; msg != nil {
			rv = append(rv, msg)
		}
		idx += 1
		if idx >= n {
			idx = 0
		}
		i += 1
	}
	return rv
}

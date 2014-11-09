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
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve"
)

func NewPIndexImpl(indexType, indexSchema, path string, restart func()) (
	PIndexImpl, Dest, error) {
	if indexType == "bleve" {
		bindexMapping := bleve.NewIndexMapping()
		if len(indexSchema) > 0 {
			if err := json.Unmarshal([]byte(indexSchema), &bindexMapping); err != nil {
				return nil, nil, fmt.Errorf("error: parse bleve index mapping: %v", err)
			}
		}

		bindex, err := bleve.New(path, bindexMapping)
		if err != nil {
			return nil, nil, fmt.Errorf("error: new bleve index, path: %s, err: %s",
				path, err)
		}

		return bindex, NewBleveDest(path, bindex, restart), err
	}

	return nil, nil, fmt.Errorf("error: NewPIndexImpl indexType: %s", indexType)
}

func OpenPIndexImpl(indexType, path string, restart func()) (PIndexImpl, Dest, error) {
	if indexType == "bleve" {
		bindex, err := bleve.Open(path)
		if err != nil {
			return nil, nil, err
		}
		return bindex, NewBleveDest(path, bindex, restart), err
	}

	return nil, nil, fmt.Errorf("error: OpenPIndexImpl indexType: %s", indexType)
}

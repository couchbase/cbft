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
	"io/ioutil"
	"os"
	"strings"

	"github.com/blevesearch/bleve"
)

// A PIndex represents a "physical" index or a index "partition".

const PINDEX_META_FILENAME string = "PINDEX_META"
const pindexPathSuffix string = ".pindex"

type PIndex struct {
	Name             string      `json:"name"`
	UUID             string      `json:"uuid"`
	IndexName        string      `json:"indexName"`
	IndexUUID        string      `json:"indexUUID"`
	IndexMapping     string      `json:"indexMapping"`
	SourceType       string      `json:"sourceType"`
	SourceName       string      `json:"sourceName"`
	SourceUUID       string      `json:"sourceUUID"`
	SourcePartitions string      `json:"sourcePartitions"`
	Path             string      `json:"-"` // Transient, not persisted.
	BIndex           bleve.Index `json:"-"` // Transient, not persisted.
	Stream           Stream      `json:"-"` // Transient, not persisted.
}

func NewPIndex(name, uuid,
	indexName, indexUUID, indexMapping,
	sourceType, sourceName, sourceUUID, sourcePartitions,
	path string) (*PIndex, error) {
	bindexMapping := bleve.NewIndexMapping()

	if len(indexMapping) > 0 {
		if err := json.Unmarshal([]byte(indexMapping), &bindexMapping); err != nil {
			return nil, fmt.Errorf("error: could not parse index mapping: %v", err)
		}
	}

	bindex, err := bleve.New(path, bindexMapping)
	if err != nil {
		return nil, fmt.Errorf("error: new bleve index, path: %s, err: %s",
			path, err)
	}

	pindex := &PIndex{
		Name:             name,
		UUID:             uuid,
		IndexName:        indexName,
		IndexUUID:        indexUUID,
		IndexMapping:     indexMapping,
		SourceType:       sourceType,
		SourceName:       sourceName,
		SourceUUID:       sourceUUID,
		SourcePartitions: sourcePartitions,
		Path:             path,
		BIndex:           bindex,
		Stream:           make(Stream),
	}
	buf, err := json.Marshal(pindex)
	if err != nil {
		return nil, err
	}

	// TODO: Save this in the bleve index instead of a separate file.
	err = ioutil.WriteFile(path+string(os.PathSeparator)+PINDEX_META_FILENAME,
		buf, 0600)
	if err != nil {
		bindex.Close()

		return nil, fmt.Errorf("error: could not save PINDEX_META_FILENAME,"+
			" path: %s, err: %v", path, err)
	}

	go pindex.Run()
	return pindex, nil
}

func OpenPIndex(path string) (*PIndex, error) {
	bindex, err := bleve.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error: could not open bleve index, path: %v, err: %v",
			path, err)
	}

	buf, err := ioutil.ReadFile(path + string(os.PathSeparator) + PINDEX_META_FILENAME)
	if err != nil {
		bindex.Close()

		return nil, fmt.Errorf("error: could not load PINDEX_META_FILENAME,"+
			" path: %s, err: %v", path, err)
	}

	pindex := &PIndex{}
	err = json.Unmarshal(buf, pindex)
	if err != nil {
		bindex.Close()

		return nil, fmt.Errorf("error: could not parse pindex json,"+
			" path: %s, err: %v", path, err)
	}

	pindex.Path = path
	pindex.BIndex = bindex
	pindex.Stream = make(Stream)

	go pindex.Run()
	return pindex, nil
}

func PIndexPath(dataDir, pindexName string) string {
	// TODO: path security checks / mapping here; ex: "../etc/pswd"
	return dataDir + string(os.PathSeparator) + pindexName + pindexPathSuffix
}

func ParsePIndexPath(dataDir, pindexPath string) (string, bool) {
	if !strings.HasSuffix(pindexPath, pindexPathSuffix) {
		return "", false
	}
	prefix := dataDir + string(os.PathSeparator)
	if !strings.HasPrefix(pindexPath, prefix) {
		return "", false
	}
	pindexName := pindexPath[len(prefix):]
	pindexName = pindexName[0 : len(pindexName)-len(pindexPathSuffix)]
	return pindexName, true
}

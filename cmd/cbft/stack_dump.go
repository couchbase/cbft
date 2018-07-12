//  Copyright (c) 2018 Couchbase, Inc.
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
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"time"
)

var maxDumps = 5

func DumpStack(dir, msg string) string {
	dumps_path := filepath.Join(dir, "dumps")
	if _, err := os.Stat(dumps_path); os.IsNotExist(err) {
		err = os.Mkdir(dumps_path, 0700)
		if err != nil {
			return ""
		}
	}

	files, err := ioutil.ReadDir(dumps_path)
	if err != nil {
		return ""
	}

	filenames := []string{}
	for _, f := range files {
		filenames = append(filenames, f.Name())
	}
	sort.Strings(filenames)

	for len(filenames) >= maxDumps {
		fileToRemove := filenames[0]
		filenames = filenames[1:]
		err = os.Remove(filepath.Join(dumps_path, fileToRemove))
		if err != nil {
			return ""
		}
	}

	newFile := strconv.FormatInt(time.Now().Unix(), 10) + ".fts.stack.dump.txt"
	file, err := os.Create(filepath.Join(dumps_path, newFile))
	if err != nil {
		return ""
	}

	fmt.Fprintf(file, msg+"\n\n")
	pprof.Lookup("goroutine").WriteTo(file, 2)
	file.Close()

	return file.Name()
}

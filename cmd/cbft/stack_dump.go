//  Copyright 2018-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"time"
)

var maxDumps = 5

func dumpStack(dir, msg string) string {
	dumps_path := filepath.Join(dir, "dumps")
	if _, err := os.Stat(dumps_path); os.IsNotExist(err) {
		err = os.Mkdir(dumps_path, 0700)
		if err != nil {
			return ""
		}
	}

	files, err := os.ReadDir(dumps_path)
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

	fmt.Fprintln(file, msg+"\n\n")
	pprof.Lookup("goroutine").WriteTo(file, 2)
	file.Close()

	return file.Name()
}

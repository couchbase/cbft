/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {

	modeline := ""
	blocks := map[string]int{}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "mode:") {
			lastSpace := strings.LastIndex(line, " ")
			prefix := line[0:lastSpace]
			suffix := line[lastSpace+1:]
			count, err := strconv.Atoi(suffix)
			if err != nil {
				fmt.Printf("error parsing count: %v", err)
				continue
			}
			existingCount, exists := blocks[prefix]
			if exists {
				blocks[prefix] = existingCount + count
			} else {
				blocks[prefix] = count
			}
		} else if modeline == "" {
			modeline = line
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}

	fmt.Println(modeline)
	for k, v := range blocks {
		fmt.Printf("%s %d\n", k, v)
	}
}

//  Copyright (c) 2016 Couchbase, Inc.
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
	"strconv"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

type ToolDefHandler func(cfg cbgt.Cfg, uuid string, tags []string,
	flags Flags, options map[string]string) (exitCode int)

type ToolDef struct {
	Name    string
	Handler ToolDefHandler
	Usage   string
}

var ToolDefs map[string]*ToolDef

func init() {
	ToolDefs = map[string]*ToolDef{
		"help": &ToolDef{
			Name:    "help",
			Handler: ToolHelp,
			Usage:   `prints out this help message.`,
		},

		"partitionSeqs": &ToolDef{
			Name:    "partitionSeqs",
			Handler: ToolMakeRepeat(ToolPartitionSeqs),
			Usage: `retrieves partition seqs from a source feed.
      sourceType=SOURCE_TYPE
      sourceName=SOURCE_NAME
      sourceUUID=SOURCE_UUID (optional)
      sourceParams=SOURCE_PARAMS (json encoded string)
      repeat=N (optional, negative number to repeat forever)
      repeatSleep=DURATION (optional, ex: "100ms", see go's time.ParseDuration())`,
		},
	}
}

func MainTool(cfg cbgt.Cfg, uuid string, tags []string, flags Flags,
	options map[string]string) int {
	tool, exists := options["tool"]
	if !exists {
		return -1 // Negative means caller should keep going with main server.
	}

	if tool == "" || tool == "h" || tool == "?" || tool == "usage" {
		tool = "help"
	}

	toolDef, exists := ToolDefs[tool]
	if !exists {
		log.Fatalf("tool: unknown tool: %s", tool)
		return 1
	}

	return toolDef.Handler(cfg, uuid, tags, flags, options)
}

func ToolHelp(cfg cbgt.Cfg, uuid string, tags []string,
	flags Flags, options map[string]string) (exitCode int) {
	fmt.Println("cbft tool usage:")
	fmt.Println("  ./cbft --options=tool=TOOL_NAME[,key0=val0[,keyN=valN]]\n")
	fmt.Println("Supported TOOL_NAME's include:")
	for tool, toolDef := range ToolDefs {
		fmt.Printf("  %s\n    %s\n\n", tool, toolDef.Usage)
	}
	return 2
}

func ToolPartitionSeqs(cfg cbgt.Cfg, uuid string, tags []string,
	flags Flags, options map[string]string) (exitCode int) {
	sourceType := options["sourceType"]

	feedType, exists := cbgt.FeedTypes[sourceType]
	if !exists || feedType == nil {
		fmt.Printf("tool: partitionSeqs: unknown sourceType: %s\n", sourceType)
		return 1
	}

	if feedType.PartitionSeqs == nil {
		return 0
	}

	partitionSeqs, err := feedType.PartitionSeqs(
		sourceType,
		options["sourceName"],
		options["sourceUUID"],
		options["sourceParams"],
		flags.Server,
		options)
	if err != nil {
		fmt.Printf("tool: partitionSeqs: err: %v\n", err)
		return 1
	}

	b, err := json.Marshal(partitionSeqs)
	if err != nil {
		fmt.Printf("tool: partitionSeqs: json marshal, err: %v\n", err)
	}

	fmt.Printf("%s\n", string(b))

	return 0
}

func ToolMakeRepeat(body ToolDefHandler) ToolDefHandler {
	f := func(cfg cbgt.Cfg, uuid string, tags []string,
		flags Flags, options map[string]string) (exitCode int) {
		var err error

		repeat := 0
		v, exists := options["repeat"]
		if exists {
			repeat, err = strconv.Atoi(v)
			if err != nil {
				fmt.Printf("tool: repeat: could not parse repeat option: %q", v)
			}
		}

		var repeatSleep time.Duration
		v, exists = options["repeatSleep"]
		if exists {
			repeatSleep, err = time.ParseDuration(v)
			if err != nil {
				fmt.Printf("tool: repeat: could not parse repeatSleep option: %q", v)
			}
		}

		i := 0

		for {
			rv := body(cfg, uuid, tags, flags, options)
			if rv != 0 {
				return rv
			}

			i++

			if repeat >= 0 && i >= repeat {
				return 0
			}

			if repeatSleep > time.Duration(0) {
				time.Sleep(repeatSleep)
			}
		}
	}

	return f
}

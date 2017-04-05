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
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	log "github.com/couchbase/clog"

	"github.com/couchbase/cbgt"
)

type toolDefHandler func(cfg cbgt.Cfg, uuid string, tags []string,
	flags cbftFlags, options map[string]string) (exitCode int)

type toolDef struct {
	Name    string
	Handler toolDefHandler
	Usage   string
}

var toolDefs map[string]*toolDef

func init() {
	toolDefs = map[string]*toolDef{
		"help": {
			Name:    "help",
			Handler: toolHelp,
			Usage:   `prints out this help message.`,
		},

		"partitionSeqs": {
			Name:    "partitionSeqs",
			Handler: toolRepeatable(toolPartitionSeqs),
			Usage: toolRepeatableUsage(`retrieves partition seqs from a source feed.
      sourceType=SOURCE_TYPE
      sourceName=SOURCE_NAME
      sourceUUID=SOURCE_UUID (optional)
      sourceParams=SOURCE_PARAMS (json encoded string)`),
		},

		"profile": {
			Name:    "profile",
			Handler: toolProfile,
			Usage: `asynchronous cpu or memory profiling.
      profileFile=FILE_PATH (example: "/tmp/cpu.pprof")
      profileType=PROFILE_TYPE (optional, default: "cpu", can also be "memory")
      profileWaitBefore=DURATION (optional, default: "0s", duration before profiling)
      profileWait=DURATION (optional, default: "10s", duration of the profile)`,
		},
	}
}

func mainTool(cfg cbgt.Cfg, uuid string, tags []string, flags cbftFlags, options map[string]string) int {
	tools, exists := options["tool"]
	if !exists {
		return -1 // Negative means caller should keep going with main server.
	}

	for _, tool := range strings.Split(tools, "|") {
		if tool == "" || tool == "h" || tool == "?" || tool == "usage" {
			tool = "help"
		}

		toolDef, exists := toolDefs[tool]
		if !exists {
			log.Fatalf("tool: unknown tool: %s", tool)
		}

		rv := toolDef.Handler(cfg, uuid, tags, flags, options)
		if rv >= 0 {
			return rv
		}
	}

	return -1
}

func toolHelp(cfg cbgt.Cfg, uuid string, tags []string,
	flags cbftFlags, options map[string]string) (exitCode int) {
	fmt.Println("\ncbft tool usage:")
	fmt.Println("  ./cbft [...] --options=tool=TOOL_NAME[,key0=val0[,keyN=valN]]")
	fmt.Println("")
	fmt.Println("Supported TOOL_NAME's include:")
	for tool, toolDef := range toolDefs {
		fmt.Printf("  %s\n    %s\n\n", tool, toolDef.Usage)
	}
	return 2
}

func toolPartitionSeqs(cfg cbgt.Cfg, uuid string, tags []string,
	flags cbftFlags, options map[string]string) (exitCode int) {
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

func toolRepeatable(body toolDefHandler) toolDefHandler {
	f := func(cfg cbgt.Cfg, uuid string, tags []string,
		flags cbftFlags, options map[string]string) (exitCode int) {
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

func toolRepeatableUsage(usage string) string {
	return usage + `
      repeat=N (optional, negative number to repeat forever)
      repeatSleep=DURATION (optional, ex: "100ms", see go's time.ParseDuration())`
}

func toolProfile(cfg cbgt.Cfg, uuid string, tags []string,
	flags cbftFlags, options map[string]string) (exitCode int) {
	var err error

	profileFile, exists := options["profileFile"]
	if !exists || profileFile == "" {
		fmt.Printf("tool: profile: profileFile option required\n")
		return 1
	}

	profileWaitBefore := time.Duration(0 * time.Second)
	v, exists := options["profileWaitBefore"]
	if exists {
		profileWaitBefore, err = time.ParseDuration(v)
		if err != nil {
			fmt.Printf("tool: profile: parse profileWaitBefore: %q,"+
				" err: %v\n", v, err)
			return 1
		}
	}

	profileWait := time.Duration(10 * time.Second)
	v, exists = options["profileWait"]
	if exists {
		profileWait, err = time.ParseDuration(v)
		if err != nil {
			fmt.Printf("tool: profile: parse profileWait: %q,"+
				" err: %v\n", v, err)
			return 1
		}
	}

	f, err := os.Create(profileFile)
	if err != nil {
		fmt.Printf("tool: profile: create profileFile: %q, err: %v",
			profileFile, err)
		return 1
	}

	go func() {
		time.Sleep(profileWaitBefore)

		profileType := options["profileType"]
		if profileType == "" || profileType == "cpu" {
			err = pprof.StartCPUProfile(f)
			if err != nil {
				log.Warnf("tool: profile: cpu, err: %v", err)
				return
			}
		}

		time.Sleep(profileWait)

		if profileType == "" || profileType == "cpu" {
			pprof.StopCPUProfile()
		} else if profileType == "memory" {
			pprof.WriteHeapProfile(f)
		} else {
			log.Warnf("tool: profile: unknown profileType: %s", profileType)
		}

		f.Close()

		log.Printf("tool: profile: done, profileFile: %s", profileFile)
	}()

	return -1
}

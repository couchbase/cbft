// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"github.com/blevesearch/bleve/v2/cmd/bleve/cmd"
	zapv11cmd "github.com/blevesearch/zapx/v11/cmd/zap/cmd"
	zapv12cmd "github.com/blevesearch/zapx/v12/cmd/zap/cmd"
	zapv13cmd "github.com/blevesearch/zapx/v13/cmd/zap/cmd"
	zapv14cmd "github.com/blevesearch/zapx/v14/cmd/zap/cmd"
	zapv15cmd "github.com/blevesearch/zapx/v15/cmd/zap/cmd"
	zapv16cmd "github.com/blevesearch/zapx/v16/cmd/zap/cmd"
	zapv17cmd "github.com/blevesearch/zapx/v17/cmd/zap/cmd"
	"github.com/spf13/cobra"

	// to support cbft's flavor of bleve build tags & flags
	_ "github.com/couchbase/cbft"
)

func init() {
	cmd.DefaultOpenReadOnly = true

	updateCommandAndAdd(zapv11cmd.RootCmd, "v11")
	updateCommandAndAdd(zapv12cmd.RootCmd, "v12")
	updateCommandAndAdd(zapv13cmd.RootCmd, "v13")
	updateCommandAndAdd(zapv14cmd.RootCmd, "v14")
	updateCommandAndAdd(zapv15cmd.RootCmd, "v15")
	updateCommandAndAdd(zapv16cmd.RootCmd, "v16")
	updateCommandAndAdd(zapv17cmd.RootCmd, "v17")
	cmd.RootCmd.AddCommand(zapCmd)
}

func main() {
	// remove commands that can modify the index
	for _, subCmd := range cmd.RootCmd.Commands() {
		if cmd.CanMutateBleveIndex(subCmd) {
			cmd.RootCmd.RemoveCommand(subCmd)
		}
	}

	cmd.Execute()
}

var zapCmd = &cobra.Command{
	Use:   "zap",
	Short: "zap lets you to interact with a zap file",
	Long:  `The zap command lets you interact with a zap file.`,
}

func updateCommandAndAdd(cmd *cobra.Command, ver string) {
	cmd.Use = ver
	cmd.Short = ver + " lets you to interact with a " + ver + " zap file"
	cmd.Long = "The " + ver + " command lets you interact with a " + ver + " zap file."
	zapCmd.AddCommand(cmd)
}

// Copyright © 2016 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"github.com/blevesearch/bleve/v2/cmd/bleve/cmd"
	zapv11cmd "github.com/blevesearch/zapx/v11/cmd/zap/cmd"
	zapv12cmd "github.com/blevesearch/zapx/v12/cmd/zap/cmd"
	zapv13cmd "github.com/blevesearch/zapx/v13/cmd/zap/cmd"
	zapv14cmd "github.com/blevesearch/zapx/v14/cmd/zap/cmd"
	zapv15cmd "github.com/blevesearch/zapx/v15/cmd/zap/cmd"
	"github.com/spf13/cobra"

	bleveMoss "github.com/blevesearch/bleve/v2/index/upsidedown/store/moss"

	// to support cbft's flavor of bleve build tags & flags
	_ "github.com/couchbase/cbft"

	"github.com/couchbase/moss"
)

func init() {
	cmd.DefaultOpenReadOnly = true

	updateCommandAndAdd(zapv11cmd.RootCmd, "v11")
	updateCommandAndAdd(zapv12cmd.RootCmd, "v12")
	updateCommandAndAdd(zapv13cmd.RootCmd, "v13")
	updateCommandAndAdd(zapv14cmd.RootCmd, "v14")
	updateCommandAndAdd(zapv15cmd.RootCmd, "v15")
	cmd.RootCmd.AddCommand(zapCmd)
}

func main() {
	bleveMoss.RegistryCollectionOptions["fts"] = moss.CollectionOptions{}

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

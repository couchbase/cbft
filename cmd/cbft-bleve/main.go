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
	"github.com/blevesearch/bleve/cmd/bleve/cmd"

	bleveMoss "github.com/blevesearch/bleve/index/store/moss"

	// to support cbft's flavor of bleve build tags & flags
	_ "github.com/couchbase/cbft"

	"github.com/couchbase/moss"
)

func init() {
	cmd.DefaultOpenReadOnly = true
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

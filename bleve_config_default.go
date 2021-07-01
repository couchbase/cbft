// Copyright 2018-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// +build server

package cbft

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/datetime/flexible"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/registry"
)

func init() {
	bleve.Config.DefaultIndexType = scorch.Name
	bleve.Config.DefaultKVStore = ""

	registry.RegisterDateTimeParser("disabled",
		func(config map[string]interface{}, cache *registry.Cache) (analysis.DateTimeParser, error) {
			return flexible.New(nil), nil // With no layouts, "disabled" always return error.
		})
}

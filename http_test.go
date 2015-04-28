//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestAssetFS(t *testing.T) {
	// Get code coverage for the assets embedded into
	// bindata_assetfs.go via the
	// github.com/elazarl/go-bindata-assetfs tool.
	if AssetFS() == nil {
		t.Errorf("expected an assetFS")
	}

	d, _ := ioutil.TempDir("./tmp", "test")
	defer os.RemoveAll(d)

	err := RestoreAssets(d, "static")
	if err != nil {
		t.Errorf("expected RestoreAssets to work, err: %v", err)
	}
}

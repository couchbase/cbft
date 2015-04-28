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

package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	log "github.com/couchbase/clog"
	"github.com/couchbaselabs/cbft"
)

func MainUUID(baseName, dataDir string) (string, error) {
	uuid := cbft.NewUUID()
	uuidPath := dataDir + string(os.PathSeparator) + baseName + ".uuid"
	uuidBuf, err := ioutil.ReadFile(uuidPath)
	if err == nil {
		uuid = strings.TrimSpace(string(uuidBuf))
		if uuid == "" {
			return "", fmt.Errorf("error: could not parse uuidPath: %s",
				uuidPath)
		}
		log.Printf("main: manager uuid: %s", uuid)
		log.Printf("main: manager uuid was reloaded")
	} else {
		log.Printf("main: manager uuid: %s", uuid)
		log.Printf("main: manager uuid was generated")
	}
	err = ioutil.WriteFile(uuidPath, []byte(uuid), 0600)
	if err != nil {
		return "", fmt.Errorf("error: could not write uuidPath: %s\n"+
			"  Please check that your -data/-dataDir parameter (%q)\n"+
			"  is to a writable directory where %s can store\n"+
			"  index data.",
			uuidPath, dataDir, baseName)
	}
	return uuid, nil
}

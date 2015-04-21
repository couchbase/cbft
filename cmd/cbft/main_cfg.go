//  Copyright (c) 2014 Couchbase, Inc.
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
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/couchbaselabs/cbft"
)

func MainCfg(connect, bindHttp, dataDir string) (cbft.Cfg, error) {
	// TODO: One day, the default cfg provider should not be simple
	if connect == "" || connect == "simple" {
		return MainCfgSimple(connect, bindHttp, dataDir)
	}
	if strings.HasPrefix(connect, "couchbase:") {
		return MainCfgCB(connect[len("couchbase:"):], bindHttp, dataDir)
	}
	return nil, fmt.Errorf("main_cfg: unsupported cfg connect: %s", connect)
}

func MainCfgSimple(connect, bindHttp, dataDir string) (cbft.Cfg, error) {
	cfgPath := dataDir + string(os.PathSeparator) + "cbft.cfg"
	cfgPathExists := false
	if _, err := os.Stat(cfgPath); err == nil {
		cfgPathExists = true
	}

	cfg := cbft.NewCfgSimple(cfgPath)
	if cfgPathExists {
		err := cfg.Load()
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func MainCfgCB(urlStr, bindHttp, dataDir string) (cbft.Cfg, error) {
	if bindHttp[0] == ':' ||
		strings.HasPrefix(bindHttp, "0.0.0.0:") ||
		strings.HasPrefix(bindHttp, "127.0.0.1:") ||
		strings.HasPrefix(bindHttp, "localhost:") {
		return nil, fmt.Errorf("main_cfg:"+
			" not a network/IP address, bindHttp: %s\n"+
			"  Please specify a -bindHttp parameter that's a\n"+
			"  real IP address (must be non-loopback, non-0.0.0.0)\n"+
			"  through which others nodes can connect to this node.",
			bindHttp)
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	bucket := "default"
	if u.User != nil && u.User.Username() != "" {
		bucket = u.User.Username()
	}

	cfg, err := cbft.NewCfgCB(urlStr, bucket)
	if err != nil {
		return nil, err
	}

	err = cfg.Load()
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

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
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
)

var httpMaxConnections = 100000

// high defaults to avoid any backward compatibility issues.
var httpReadTimeout = 20 * time.Second
var httpReadHeaderTimeout = 5 * time.Second
var httpWriteTimeout = 60 * time.Second
var httpIdleTimeout = 60 * time.Second

func initHTTPOptions(options map[string]string) error {
	s := options["httpTransportDialContextTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbgt.HttpTransportDialContextTimeout = v
	}

	s = options["httpTransportDialContextKeepAlive"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbgt.HttpTransportDialContextKeepAlive = v
	}

	s = options["httpTransportIdleConnTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbgt.HttpTransportIdleConnTimeout = v
	}

	s = options["httpTransportMaxIdleConns"]
	if s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		cbgt.HttpTransportMaxIdleConns = v
	}

	s = options["httpTransportMaxIdleConnsPerHost"]
	if s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		cbgt.HttpTransportMaxIdleConnsPerHost = v
	}

	s = options["httpTransportTLSHandshakeTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		cbgt.HttpTransportTLSHandshakeTimeout = v
	}

	mc, found := cbgt.ParseOptionsInt(options, "httpMaxConnections")
	if found {
		httpMaxConnections = mc
	}
	rt, found := cbgt.ParseOptionsInt(options, "httpReadTimeout")
	if found {
		httpReadTimeout = time.Duration(rt) * time.Second
	}
	wt, found := cbgt.ParseOptionsInt(options, "httpWriteTimeout")
	if found {
		httpWriteTimeout = time.Duration(wt) * time.Second
	}
	it, found := cbgt.ParseOptionsInt(options, "httpIdleTimeout")
	if found {
		httpIdleTimeout = time.Duration(it) * time.Second
	}
	ht, found := cbgt.ParseOptionsInt(options, "httpReadHeaderTimeout")
	if found {
		httpReadHeaderTimeout = time.Duration(ht) * time.Second
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cbgt.HttpTransportDialContextTimeout,
			KeepAlive: cbgt.HttpTransportDialContextKeepAlive,
		}).DialContext,
		MaxIdleConns:          cbgt.HttpTransportMaxIdleConns,
		MaxIdleConnsPerHost:   cbgt.HttpTransportMaxIdleConnsPerHost,
		IdleConnTimeout:       cbgt.HttpTransportIdleConnTimeout,
		TLSHandshakeTimeout:   cbgt.HttpTransportTLSHandshakeTimeout,
		ExpectContinueTimeout: cbgt.HttpTransportExpectContinueTimeout,
	}

	cbft.HttpClient = &http.Client{Transport: transport}

	return nil
}

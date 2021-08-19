//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
var httpHandlerTimeout = 300 * time.Minute

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
	ht, found = cbgt.ParseOptionsInt(options, "httpHandlerTimeout")
	if found {
		httpHandlerTimeout = time.Duration(ht) * time.Minute
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

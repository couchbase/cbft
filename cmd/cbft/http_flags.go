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
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/couchbase/cbft"
)

var httpTransportDialContextTimeout = 30 * time.Second   // Go's default is 30 secs.
var httpTransportDialContextKeepAlive = 30 * time.Second // Go's default is 30 secs.
var httpTransportMaxIdleConns = 300                      // Go's default is 100 (0 means no limit).
var httpTransportMaxIdleConnsPerHost = 100               // Go's default is 2.
var httpTransportIdleConnTimeout = 90 * time.Second      // Go's default is 90 secs.
var httpTransportTLSHandshakeTimeout = 10 * time.Second  // Go's default is 10 secs.
var httpTransportExpectContinueTimeout = 1 * time.Second // Go's default is 1 secs.

func InitHttpOptions(options map[string]string) error {
	s := options["httpTransportDialContextTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		httpTransportDialContextTimeout = v
	}

	s = options["httpTransportDialContextKeepAlive"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		httpTransportDialContextKeepAlive = v
	}

	s = options["httpTransportIdleConnTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		httpTransportIdleConnTimeout = v
	}

	s = options["httpTransportMaxIdleConns"]
	if s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		httpTransportMaxIdleConns = v
	}

	s = options["httpTransportMaxIdleConnsPerHost"]
	if s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		httpTransportMaxIdleConnsPerHost = v
	}

	s = options["httpTransportTLSHandshakeTimeout"]
	if s != "" {
		v, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		httpTransportTLSHandshakeTimeout = v
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   httpTransportDialContextTimeout,
			KeepAlive: httpTransportDialContextKeepAlive,
		}).DialContext,
		MaxIdleConns:          httpTransportMaxIdleConns,
		MaxIdleConnsPerHost:   httpTransportMaxIdleConnsPerHost,
		IdleConnTimeout:       httpTransportIdleConnTimeout,
		TLSHandshakeTimeout:   httpTransportTLSHandshakeTimeout,
		ExpectContinueTimeout: httpTransportExpectContinueTimeout,
	}

	httpClient := &http.Client{Transport: transport}

	cbft.HttpClient = httpClient

	cbft.HttpGet = func(url string) (resp *http.Response, err error) {
		return httpClient.Get(url)
	}

	cbft.HttpPost = func(url string, bodyType string, body io.Reader) (
		resp *http.Response, err error) {
		return httpClient.Post(url, bodyType, body)
	}

	return nil
}

//  Copyright (c) 2017 Couchbase, Inc.
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
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/couchbase/cbauth"

	"golang.org/x/net/netutil"
)

func setupHTTPListenersAndServ(routerInUse http.Handler, bindHTTPList []string, options map[string]string) {
	http.Handle("/", routerInUse)
	anyHostPorts := map[string]bool{}
	// Bind to 0.0.0.0's first for http listening.
	for _, bindHTTP := range bindHTTPList {
		if strings.HasPrefix(bindHTTP, "0.0.0.0:") {
			go mainServeHTTP("http", bindHTTP, nil, "", "")

			anyHostPorts[bindHTTP] = true
		}
	}

	for i := len(bindHTTPList) - 1; i >= 1; i-- {
		go mainServeHTTP("http", bindHTTPList[i], anyHostPorts, "", "")
	}

	if options["authType"] == "cbauth" {
		// Registering a certificate refresh callback with cbauth,
		// which will be responsible for updating https listeners,
		// whenever ssl certificates are changed.
		cbauth.RegisterCertRefreshCallback(setupHTTPSListeners)
	} else {
		setupHTTPSListeners()
	}

	mainServeHTTP("http", bindHTTPList[0], anyHostPorts, "", "")
}

// Add to HTTPS Server list serially
func addToHTTPSServerList(entry *http.Server) {
	httpsServersMutex.Lock()
	httpsServers = append(httpsServers, entry)
	httpsServersMutex.Unlock()
}

// Close all HTTPS Servers and clear HTTPS Server list
func closeAndClearHTTPSServerList() {
	httpsServersMutex.Lock()
	defer httpsServersMutex.Unlock()

	for _, server := range httpsServers {
		server.Close()
	}
	httpsServers = nil
}

func setupHTTPSListeners() error {
	// Close any previously open https servers
	closeAndClearHTTPSServerList()

	anyHostPorts := map[string]bool{}

	if flags.BindHTTPS != "" {
		bindHTTPSList := strings.Split(flags.BindHTTPS, ",")

		// Bind to 0.0.0.0's first for https listening.
		for _, bindHTTPS := range bindHTTPSList {
			if strings.HasPrefix(bindHTTPS, "0.0.0.0:") {
				go mainServeHTTP("https", bindHTTPS, nil,
					flags.TLSCertFile, flags.TLSKeyFile)

				anyHostPorts[bindHTTPS] = true
			}
		}

		for _, bindHTTPS := range bindHTTPSList {
			go mainServeHTTP("https", bindHTTPS, anyHostPorts,
				flags.TLSCertFile, flags.TLSKeyFile)
		}
	}

	return nil
}

// mainServeHTTP starts the http/https servers for cbft.
// The proto may be "http" or "https".
func mainServeHTTP(proto, bindHTTP string, anyHostPorts map[string]bool,
	certFile, keyFile string) {
	if bindHTTP[0] == ':' && proto == "http" {
		bindHTTP = "localhost" + bindHTTP
	}

	bar := "main: ------------------------------------------------------"

	if anyHostPorts != nil {
		// If we've already bound to 0.0.0.0 on the same port, then
		// skip this hostPort.
		hostPort := strings.Split(bindHTTP, ":")
		if len(hostPort) >= 2 {
			anyHostPort := "0.0.0.0:" + hostPort[1]
			if anyHostPorts[anyHostPort] {
				if anyHostPort != bindHTTP {
					log.Printf(bar)
					log.Printf("init_http: web UI / REST API is available"+
						" (via 0.0.0.0): %s://%s", proto, bindHTTP)
					log.Printf(bar)
				}
				return
			}
		}
	}

	log.Printf(bar)
	log.Printf("init_http: web UI / REST API is available: %s://%s", proto, bindHTTP)
	log.Printf(bar)

	listener, err := net.Listen("tcp", bindHTTP)
	if err != nil {
		log.Fatalf("init_http: listen, err: %v", err)
	}
	defer listener.Close()
	server := &http.Server{Addr: bindHTTP,
		Handler:      routerInUse,
		ReadTimeout:  httpReadTimeout,
		WriteTimeout: httpWriteTimeout}

	if proto == "http" {
		limitListener := netutil.LimitListener(listener, httpMaxConnections)
		err = server.Serve(limitListener)
		if err != nil {
			log.Fatalf("init_http: Serve, err: %v;\n"+
				"  Please check that your -bindHttp(s) parameter (%q)\n"+
				"  is correct and available.", err, bindHTTP)
		}
	} else {
		addToHTTPSServerList(server)
		// Initialize server.TLSConfig to the listener's TLS Config before calling
		// server for HTTP/2 support.
		// See: https://golang.org/pkg/net/http/#Server.Serve
		config := cloneTLSConfig(server.TLSConfig)
		if !strSliceContains(config.NextProtos, "http/1.1") {
			config.NextProtos = append(config.NextProtos, "http/1.1")
		}
		if !strSliceContains(config.NextProtos, "h2") {
			config.NextProtos = append(config.NextProtos, "h2")
		}

		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatalf("init_http: LoadX509KeyPair, err: %v", err)
		}
		tlsListener := tls.NewListener(tcpKeepAliveListener{listener.(*net.TCPListener)}, config)
		limitListener := netutil.LimitListener(tlsListener, httpMaxConnections)
		err = server.Serve(limitListener)
		if err != nil {
			log.Printf("init_http: Serve, err: %v;\n"+
				" HTTPS listeners closed, likely to be re-initialized, "+
				" -bindHttp(s) (%q)\n", err, bindHTTP)
		}
	}
}

// Helper function to determine if the provided string is already
// present in the provided array of strings.
func strSliceContains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// cloneTLSConfig returns a clone of the tls.Config.
func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil {
		return &tls.Config{}
	}
	return cfg.Clone()
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

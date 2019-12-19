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
	"crypto/x509"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbft"
	log "github.com/couchbase/clog"

	"golang.org/x/net/netutil"
)

type httpsServer struct {
	server   *http.Server
	listener net.Listener
}

// List of active https servers
var httpsServers []*httpsServer
var httpsServersMutex sync.Mutex

// AuthType used for HTTPS connections
var authType string

// Use IPv6
var ipv6 string

func setupHTTPListenersAndServ(routerInUse http.Handler, bindHTTPList []string,
	options map[string]string) {
	http.Handle("/", routerInUse)
	ipv6 = options["ipv6"]

	anyHostPorts := map[string]bool{}
	// Bind to 0.0.0.0's (IPv4) or [::]'s (IPv6) first for http listening.
	for _, bindHTTP := range bindHTTPList {
		if strings.HasPrefix(bindHTTP, "0.0.0.0:") ||
			strings.HasPrefix(bindHTTP, "[::]:") {
			go mainServeHTTP("http", bindHTTP, nil, "", "")

			anyHostPorts[bindHTTP] = true
		}
	}

	for i := len(bindHTTPList) - 1; i >= 1; i-- {
		go mainServeHTTP("http", bindHTTPList[i], anyHostPorts, "", "")
	}

	authType = options["authType"]

	if authType == "cbauth" {
		// Registering a TLS refresh callback with cbauth, which
		// will be responsible for updating https listeners,
		// whenever ssl certificates or the client cert auth settings
		// are changed.
		cbauth.RegisterTLSRefreshCallback(setupHTTPSListeners)
	} else {
		setupHTTPSListeners()
	}

	mainServeHTTP("http", bindHTTPList[0], anyHostPorts, "", "")
}

// Add to HTTPS Server list serially
func addToHTTPSServerList(server *http.Server, listener net.Listener) {
	httpsServersMutex.Lock()
	entry := &httpsServer{
		server:   server,
		listener: listener,
	}
	httpsServers = append(httpsServers, entry)
	httpsServersMutex.Unlock()
}

// Close all HTTPS Servers and clear HTTPS Server list
func closeAndClearHTTPSServerList() {
	httpsServersMutex.Lock()
	defer httpsServersMutex.Unlock()

	for _, entry := range httpsServers {
		// Close the listener associated with the server first.
		entry.listener.Close()
		// Then Close the server.
		entry.server.Close()
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
			if strings.HasPrefix(bindHTTPS, "0.0.0.0:") ||
				strings.HasPrefix(bindHTTPS, "[::]:") {
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

	if anyHostPorts != nil && len(bindHTTP) > 0 {
		// If we've already bound to 0.0.0.0 or [::] on the same port, then
		// skip this hostPort.
		portIndex := strings.LastIndex(bindHTTP, ":") + 1
		if portIndex > 0 && portIndex < len(bindHTTP) {
			// Possibly valid port available.
			port := bindHTTP[portIndex:]
			if _, err := strconv.Atoi(port); err == nil {
				// Valid port.
				host := "0.0.0.0"
				if net.ParseIP(bindHTTP[:portIndex-1]).To4() == nil && // Not an IPv4
					ipv6 == "true" {
					host = "[::]"
				}

				anyHostPort := host + ":" + port
				if anyHostPorts[anyHostPort] {
					if anyHostPort != bindHTTP {
						log.Printf(bar)
						log.Printf("init_http: web UI / REST API is available"+
							" (via %v): %s://%s", host, proto, bindHTTP)
						log.Printf(bar)
					}
					return
				}
			}
		} // Else port not found.
	}

	log.Printf(bar)
	log.Printf("init_http: web UI / REST API is available: %s://%s", proto, bindHTTP)
	log.Printf(bar)

	listener, err := net.Listen("tcp", bindHTTP)
	if err != nil {
		log.Fatalf("init_http: listen, err: %v", err)
	}
	server := &http.Server{Addr: bindHTTP,
		Handler:           routerInUse,
		ReadTimeout:       httpReadTimeout,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	if proto == "http" {
		limitListener := netutil.LimitListener(listener, httpMaxConnections)
		log.Printf("init_http: Setting up a http limit listener over %q", bindHTTP)
		atomic.AddUint64(&cbft.TotHTTPLimitListenersOpened, 1)
		err = server.Serve(limitListener)
		if err != nil {
			log.Fatalf("init_http: Serve, err: %v;\n"+
				"  Please check that your -bindHttp(s) parameter (%q)\n"+
				"  is correct and available.", err, bindHTTP)
		}
		atomic.AddUint64(&cbft.TotHTTPLimitListenersClosed, 1)
	} else {
		addToHTTPSServerList(server, listener)
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

		if authType == "cbauth" {
			// Set MinTLSVersion and CipherSuites to what is provided by
			// cbauth if authType were cbauth.
			config.MinVersion = cbauth.MinTLSVersion()
			config.CipherSuites = cbauth.CipherSuites()

			clientAuthType, er := cbauth.GetClientCertAuthType()
			if er != nil {
				log.Fatalf("init_http: GetClientCertAuthType, err: %v", err)
			}

			if clientAuthType != tls.NoClientCert {
				caCert, er := ioutil.ReadFile(certFile)
				if er != nil {
					log.Fatalf("init_http: ReadFile of cacert, err: %v", err)
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				config.ClientCAs = caCertPool
				config.ClientAuth = clientAuthType
			}
		}

		keepAliveListener := tcpKeepAliveListener{listener.(*net.TCPListener)}
		limitListener := netutil.LimitListener(keepAliveListener, httpMaxConnections)
		tlsListener := tls.NewListener(limitListener, config)
		log.Printf("init_http: Setting up a https limit listener over %q", bindHTTP)
		atomic.AddUint64(&cbft.TotHTTPSLimitListenersOpened, 1)
		err = server.Serve(tlsListener)
		if err != nil {
			log.Printf("init_http: Serve, err: %v;\n"+
				" HTTPS listeners closed, likely to be re-initialized, "+
				" -bindHttp(s) (%q)\n", err, bindHTTP)
		}
		atomic.AddUint64(&cbft.TotHTTPSLimitListenersClosed, 1)
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

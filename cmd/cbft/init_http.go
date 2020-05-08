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

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"

	"golang.org/x/net/netutil"
)

// List of active https servers
var httpsServersMutex sync.Mutex
var httpsServers []*http.Server

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
			mainServeHTTP("http", bindHTTP, nil)

			anyHostPorts[bindHTTP] = true
		}
	}

	for i := len(bindHTTPList) - 1; i >= 1; i-- {
		mainServeHTTP("http", bindHTTPList[i], anyHostPorts)
	}

	authType = options["authType"]

	if authType == "cbauth" {
		// Registering a TLS refresh callback with cbauth, which
		// will be responsible for updating https listeners,
		// whenever ssl certificates or the client cert auth settings
		// are changed.
		handleConfigChanges := func() error {
			// restart the servers in case of a refresh
			setupHTTPSListeners()
			return nil
		}
		cbgt.RegisterConfigRefreshCallback("fts/https", handleConfigChanges)
	}

	setupHTTPSListeners()
}

// Add to HTTPS Server list serially
func addToHTTPSServerList(server *http.Server) {
	httpsServersMutex.Lock()
	httpsServers = append(httpsServers, server)
	httpsServersMutex.Unlock()
}

// Close all HTTPS Servers and clear HTTPS Server list
func closeAndClearHTTPSServerList() {
	httpsServersMutex.Lock()
	defer httpsServersMutex.Unlock()

	for _, entry := range httpsServers {
		// Upon invoking Close() for a server, the blocking Serve(..) for
		// the server will return ErrServerClosed and is also responsible
		// for closing the listener that it's accepting incoming
		// connections on.
		entry.Close()
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
				mainServeHTTP("https", bindHTTPS, nil)

				anyHostPorts[bindHTTPS] = true
			}
		}

		for _, bindHTTPS := range bindHTTPSList {
			mainServeHTTP("https", bindHTTPS, anyHostPorts)
		}
	}

	return nil
}

// mainServeHTTP starts the http/https servers for cbft.
// The proto may be "http" or "https".
func mainServeHTTP(proto, bindHTTP string, anyHostPorts map[string]bool) {
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

	nwps := getNetworkProtocols()
	for p, nwp := range nwps {
		listener, err := getListener(bindHTTP, nwp)
		if listener == nil && err == nil {
			continue
		}
		if err != nil {
			if p == 0 {
				// Fail the service as the listen failed on the primary protocol.
				// If ipv6=true,
				//	-start listening to ipv6 - fail service if listen fails
				//	-try to listen to ipv4 - don't fail service even if listen fails.
				// If ipv6=false,
				//	-start listening to ipv4 - fail service if listen fails
				//	-try to listen to ipv6 - don't fail service even if listen fails.
				log.Fatalf("init_http: listen, err: %v", err)
			}
			log.Errorf("init_http: listen, err: %v", err)
			continue
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
			go func(nwp string, listener net.Listener, primary bool) {
				log.Printf("init_http: Setting up a http limit listener at %q,"+
					" proto: %q", bindHTTP, nwp)
				atomic.AddUint64(&cbft.TotHTTPLimitListenersOpened, 1)
				err = server.Serve(limitListener)
				if err != nil {
					if primary {
						log.Fatalf("init_http: Serve, err: %v;\n"+
							"  Please check that your -bindHttp(s) parameter (%q)\n"+
							"  is correct and available.", err, bindHTTP)
					}
					log.Printf("init_http: Serve, err: %v;\n"+
						"  Please check that your -bindHttp(s) parameter (%q)\n"+
						"  is correct and available.", err, bindHTTP)
				}
				atomic.AddUint64(&cbft.TotHTTPLimitListenersClosed, 1)
			}(nwp, limitListener, p == 0)
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
			config.Certificates[0], err = tls.LoadX509KeyPair(
				cbgt.TLSCertFile, cbgt.TLSKeyFile)
			if err != nil {
				log.Fatalf("init_http: LoadX509KeyPair, err: %v", err)
			}

			if authType == "cbauth" {
				ss := cbgt.GetSecuritySetting()
				if ss != nil && ss.TLSConfig != nil {
					// Set MinTLSVersion and CipherSuites to what is provided by
					// cbauth if authType were cbauth (cached locally).
					config.MinVersion = ss.TLSConfig.MinVersion
					config.CipherSuites = ss.TLSConfig.CipherSuites
					config.PreferServerCipherSuites = ss.TLSConfig.PreferServerCipherSuites

					if ss.ClientAuthType != nil && *ss.ClientAuthType != tls.NoClientCert {
						caCertPool := x509.NewCertPool()
						var certBytes []byte
						if len(ss.CertInBytes) != 0 {
							certBytes = ss.CertInBytes
						} else {
							// if no CertInBytes found in settings, then fallback
							// to reading directly from file. Upon any certs change
							// callbacks later, reboot of servers will ensure the
							// latest certificates in the servers.
							certBytes, err = ioutil.ReadFile(cbgt.TLSCertFile)
							if err != nil {
								log.Fatalf("init_http: ReadFile of cacert, err: %v", err)
							}
						}
						ok := caCertPool.AppendCertsFromPEM(certBytes)
						if !ok {
							log.Fatalf("init_http: error in appending certificates")
						}
						config.ClientCAs = caCertPool
						config.ClientAuth = *ss.ClientAuthType
					} else {
						config.ClientAuth = tls.NoClientCert
					}
				} else {
					config.ClientAuth = tls.NoClientCert
				}
			}

			keepAliveListener := tcpKeepAliveListener{listener.(*net.TCPListener)}
			limitListener := netutil.LimitListener(keepAliveListener, httpMaxConnections)
			tlsListener := tls.NewListener(limitListener, config)
			go func(nwp string, tlsListener net.Listener, primary bool) {
				log.Printf("init_http: Setting up a https limit listener at %q,"+
					" proto: %q", bindHTTP, nwp)
				atomic.AddUint64(&cbft.TotHTTPSLimitListenersOpened, 1)
				err = server.Serve(tlsListener)
				if err != nil {
					if primary {
						log.Fatalf("init_http: Serve, err: %v;"+
							" HTTPS listeners closed, likely to be re-initialized, "+
							" -bindHttp(s) (%q) on nwp: %s\n", err, bindHTTP, nwp)
					}
					log.Printf("init_http: Serve, err: %v;"+
						" HTTPS listeners closed, likely to be re-initialized, "+
						" -bindHttp(s) (%q) on nwp: %s\n", err, bindHTTP, nwp)
				}
				atomic.AddUint64(&cbft.TotHTTPSLimitListenersClosed, 1)
			}(nwp, tlsListener, p == 0)
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

func getNetworkProtocols() []string {
	if ipv6 == "true" {
		return []string{"tcp6", "tcp4"}
	}
	return []string{"tcp4", "tcp6"}
}

func getListener(bindAddr, nwp string) (net.Listener, error) {
	if (strings.HasPrefix(bindAddr, "0.0.0.0:") && nwp == "tcp6") ||
		(strings.HasPrefix(bindAddr, "[::1]:") && nwp == "tcp4") {
		return nil, nil
	}
	return net.Listen(nwp, bindAddr)
}

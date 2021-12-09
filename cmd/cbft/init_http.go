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
	"sync/atomic"
	"time"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"

	"golang.org/x/net/netutil"
)

func setupHTTPListenersAndServe(routerInUse http.Handler) {
	http.Handle("/", routerInUse)

	bindHTTPList := strings.Split(flags.BindHTTP, ",")
	bindHTTPSList := strings.Split(flags.BindHTTPS, ",")

	if authType == "cbauth" {
		// register a security refresh callback with cbauth, which is
		// responsible for updating the listeners on refresh
		handleConfigChanges := func(status int) error {
			// restart the servers in case of a refresh
			setupHTTPListeners(bindHTTPList, bindHTTPSList, status)
			return nil
		}
		cbgt.RegisterConfigRefreshCallback("fts/http,https", handleConfigChanges)
	}

	setupHTTPListeners(bindHTTPList, bindHTTPSList,
		cbgt.AuthChange_nonSSLPorts|cbgt.AuthChange_certificates)
}

func setupHTTPListeners(bindHTTPList, bindHTTPSList []string, status int) error {
	if status&cbgt.AuthChange_nonSSLPorts != 0 {
		// close any previously opened http servers
		serversCache.shutdownHttpServers(false)

		ss := cbgt.GetSecuritySetting()
		if ss == nil || !ss.DisableNonSSLPorts {
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
		} else if ss.DisableNonSSLPorts {
			// Only listen on 127.0.0.1:8094 or [::1]:8094 (based on address family)
			// Ref: MB-48142
			if len(bindHTTPList) > 0 {
				portIndex := strings.LastIndex(bindHTTPList[0], ":") + 1
				if portIndex > 0 && portIndex < len(bindHTTPList[0]) {
					port := bindHTTPList[0][portIndex:]
					mainServeHTTP("http", "127.0.0.1:"+port, nil)
					if ipv6 == "true" {
						mainServeHTTP("http", "[::1]:"+port, nil)
					}
				}
			}
		}
	}

	if status&cbgt.AuthChange_certificates != 0 {
		// close any previously opened https servers
		serversCache.shutdownHttpServers(true)

		anyHostPorts := map[string]bool{}
		// Bind to 0.0.0.0's (IPv4) or [::]'s (IPv6) first for http listening.
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
	if len(bindHTTP) == 0 {
		return
	}

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

	setupHTTPServer := func(listener net.Listener) {
		server := &http.Server{
			Addr:              bindHTTP,
			Handler:           routerInUse,
			ReadTimeout:       httpReadTimeout,
			ReadHeaderTimeout: httpReadHeaderTimeout,
			IdleTimeout:       httpIdleTimeout,
		}

		if proto == "http" {
			serversCache.registerHttpServer(server, false)
			limitListener := netutil.LimitListener(listener, httpMaxConnections)
			go func(listener net.Listener) {
				log.Printf("init_http: Setting up a http limit listener at %q", bindHTTP)
				atomic.AddUint64(&cbft.TotHTTPLimitListenersOpened, 1)
				err := server.Serve(limitListener)
				if err != nil {
					log.Warnf("init_http: Serve, err: %v;"+
						" closed HTTP listener on bindHTTP: %q", err, bindHTTP)
				}
				atomic.AddUint64(&cbft.TotHTTPLimitListenersClosed, 1)
			}(limitListener)
		} else {
			serversCache.registerHttpServer(server, true)
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

			var err error
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
						certInBytes := ss.CertInBytes
						if len(certInBytes) == 0 {
							// if no CertInBytes found in settings, then fallback
							// to reading directly from file. Upon any certs change
							// callbacks later, reboot of servers will ensure the
							// latest certificates in the servers.
							certInBytes, err = ioutil.ReadFile(cbgt.TLSCertFile)
							if err != nil {
								log.Fatalf("init_http: ReadFile of cacert, err: %v", err)
							}
						}
						ok := caCertPool.AppendCertsFromPEM(certInBytes)
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
			go func(tlsListener net.Listener) {
				log.Printf("init_http: Setting up a https limit listener at %q", bindHTTP)
				atomic.AddUint64(&cbft.TotHTTPSLimitListenersOpened, 1)
				err = server.Serve(tlsListener)
				if err != nil {
					log.Warnf("init_http: Serve, err: %v;"+
						" closed HTTPS listener on bindHTTPS: %q", err, bindHTTP)
				}
				atomic.AddUint64(&cbft.TotHTTPSLimitListenersClosed, 1)
			}(tlsListener)
		}
	}

	listener, err := net.Listen("tcp", bindHTTP)
	if err != nil {
		log.Fatalf("init_http: listen, err: %v", err)
	} else if listener != nil {
		setupHTTPServer(listener)
	} else {
		log.Warnf("init_http: listener not set up for %s", bindHTTP)
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

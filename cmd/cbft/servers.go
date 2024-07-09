//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"reflect"
	"sync"

	"github.com/couchbase/cbft"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"google.golang.org/grpc"
)

type serverCache struct {
	m sync.Mutex

	httpServers []*httpServer
	grpcServers []*grpcServer
}

// Make and return a new http server based on the parameters
func (s *serverCache) registerNewHTTPServer(https bool, bindHTTP, nwp string,
	routerInUse http.Handler) *httpServer {
	rv := &httpServer{
		server: &http.Server{
			Addr:              bindHTTP,
			Handler:           routerInUse,
			ReadTimeout:       httpReadTimeout,
			ReadHeaderTimeout: httpReadHeaderTimeout,
			IdleTimeout:       httpIdleTimeout,
			ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
				return context.WithValue(ctx, "conn", conn)
			},
		},
		nwp:   nwp,
		https: https,
	}

	if https {
		rv.tlsConfigLock = sync.RWMutex{}
		rv.tlsConfig = &tls.Config{
			GetConfigForClient: func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
				rv.tlsConfigLock.RLock()
				tls := rv.tlsConfig
				rv.tlsConfigLock.RUnlock()
				return tls, nil
			},
		}
		rv.server.TLSConfig = rv.tlsConfig
	}

	s.m.Lock()
	s.httpServers = append(s.httpServers, rv)
	s.m.Unlock()

	return rv
}

// Find and return the server that matches the parameters provided
func (s *serverCache) getHTTPServer(https bool, bindHTTP, nwp string,
	routerInUse http.Handler) *httpServer {
	if len(bindHTTP) == 0 {
		return nil
	}

	if bindHTTP[0] == ':' && !https {
		bindHTTP = "localhost" + bindHTTP
	}

	for _, server := range s.httpServers {
		if server.https == https && server.server.Addr == bindHTTP &&
			server.nwp == nwp &&
			reflect.ValueOf(server.server.Handler) == reflect.ValueOf(routerInUse) {
			return server
		}
	}
	return nil
}

// Shutdown the chosen http.Servers
func (s *serverCache) shutdownHttpServers(https bool) {
	s.m.Lock()
	// Upon invoking Close() for a server, the blocking Serve(..) for
	// the server will return ErrServerClosed and is also responsible
	// for closing the listener that it's accepting incoming
	// connections on.
	httpServers := make([]*httpServer, 0)
	for _, entry := range s.httpServers {
		// Stop the specified server type while tracking the rest
		if entry.https == https {
			entry.server.Close()
		} else {
			httpServers = append(httpServers, entry)
		}
	}
	s.httpServers = httpServers
	s.m.Unlock()
}

func (s *serverCache) registerNewGRPCServer(nwp, bindGRPC string, ssl bool,
	authType string) *grpcServer {

	rv := &grpcServer{
		nwp:      nwp,
		bindGRPC: bindGRPC,
		ssl:      ssl,
	}

	config := newTLSConfig(nil)

	opts := []grpc.ServerOption{
		cbft.AddServerInterceptor(),
		grpc.MaxConcurrentStreams(cbft.DefaultGrpcMaxConcurrentStreams),
		grpc.MaxSendMsgSize(cbft.DefaultGrpcMaxRecvMsgSize),
		grpc.MaxRecvMsgSize(cbft.DefaultGrpcMaxSendMsgSize),
		// TODO add more configurability
	}

	var creds *tlsCreds
	if ssl {
		creds = NewTransportCredentials(config)
		opts = append(opts, grpc.Creds(creds))
	}

	rv.server = grpc.NewServer(opts...)
	rv.creds = creds

	s.m.Lock()
	s.grpcServers = append(s.grpcServers, rv)
	s.m.Unlock()

	return rv
}

// Find and return the server that matches the parameters provided
func (s *serverCache) getGRPCServer(ssl bool, bindGRPC string,
	nwp string) *grpcServer {
	if len(bindGRPC) == 0 {
		return nil
	}

	if bindGRPC[0] == ':' {
		bindGRPC = "localhost" + bindGRPC
	}

	for _, server := range s.grpcServers {
		if server.bindGRPC == bindGRPC && server.ssl == ssl &&
			server.nwp == nwp {
			return server
		}
	}

	return nil
}

// Shutdown the chosen grpc.Servers
func (s *serverCache) shutdownGrpcServers(ssl bool) {
	s.m.Lock()
	grpcServers := make([]*grpcServer, 0)
	for _, server := range s.grpcServers {
		// Stop the specified server type while tracking the rest
		if server.ssl == ssl {
			server.server.Stop()
		} else {
			grpcServers = append(grpcServers, server)
		}
	}
	s.grpcServers = grpcServers
	s.m.Unlock()
}

type httpServer struct {
	server        *http.Server
	nwp           string
	https         bool
	tlsConfig     *tls.Config
	tlsConfigLock sync.RWMutex
}

func (entry *httpServer) setTLSConfig(config *tls.Config) {
	entry.tlsConfigLock.Lock()
	entry.tlsConfig = config
	entry.tlsConfigLock.Unlock()
}

func (entry *httpServer) getTLSConfig() *tls.Config {
	entry.tlsConfigLock.RLock()
	rv := cloneTLSConfig(entry.tlsConfig)
	entry.tlsConfigLock.RUnlock()
	return rv
}

type grpcServer struct {
	nwp      string
	bindGRPC string
	server   *grpc.Server
	ssl      bool
	creds    *tlsCreds
}

func (entry *grpcServer) setTLSConfig(config *tls.Config) {
	entry.creds.SetConfig(config)
}

func (entry *grpcServer) getTLSConfig() *tls.Config {
	return entry.creds.GetConfig()
}

func newTLSConfig(config *tls.Config) *tls.Config {
	rv := cloneTLSConfig(config)
	if !strSliceContains(rv.NextProtos, "http/1.1") {
		rv.NextProtos = append(rv.NextProtos, "http/1.1")
	}
	if !strSliceContains(rv.NextProtos, "h2") {
		rv.NextProtos = append(rv.NextProtos, "h2")
	}

	var err error
	if authType == "cbauth" {
		ss := cbgt.GetSecuritySetting()
		if ss != nil && ss.TLSConfig != nil {
			rv.Certificates = []tls.Certificate{ss.ServerCertificate}

			// Set MinTLSVersion and CipherSuites to what is provided by
			// cbauth if authType were cbauth (cached locally).
			rv.MinVersion = ss.TLSConfig.MinVersion
			rv.CipherSuites = ss.TLSConfig.CipherSuites
			rv.PreferServerCipherSuites = ss.TLSConfig.PreferServerCipherSuites

			if ss.ClientAuthType != nil && *ss.ClientAuthType != tls.NoClientCert {
				caCertPool := x509.NewCertPool()
				certInBytes := ss.CACertInBytes
				if len(certInBytes) == 0 {
					// if no CertInBytes found in settings, then fallback
					// to reading directly from file. Upon any certs change
					// callbacks later, reboot of servers will ensure the
					// latest certificates in the servers.
					caFile := cbgt.TLSCertFile
					if len(cbgt.TLSCAFile) > 0 {
						caFile = cbgt.TLSCAFile
					}

					certInBytes, err = os.ReadFile(caFile)
					if err != nil {
						log.Fatalf("init_http: ReadFile of cacert, path: %v, err: %v",
							caFile, err)
					}
				}
				ok := caCertPool.AppendCertsFromPEM(certInBytes)
				if !ok {
					log.Fatalf("init_http: error in appending certificates")
				}
				rv.ClientCAs = caCertPool
				rv.ClientAuth = *ss.ClientAuthType
			} else {
				rv.ClientAuth = tls.NoClientCert
			}
		} else {
			rv.ClientAuth = tls.NoClientCert
		}
	} else {
		rv.Certificates = make([]tls.Certificate, 1)
		rv.Certificates[0], err = tls.LoadX509KeyPair(
			cbgt.TLSCertFile, cbgt.TLSKeyFile)
		if err != nil {
			log.Fatalf("init_http: LoadX509KeyPair, err: %v", err)
		}
	}

	return rv
}

// -----------------------------------------------------------------------------

var serversCache = &serverCache{}

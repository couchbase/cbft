//  Copyright (c) 2019 Couchbase, Inc.
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

	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var grpcServers []*grpc.Server
var grpcServersMutex sync.Mutex

// Close all Grpc Servers
func closeAndClearGrpcServerList() {
	grpcServersMutex.Lock()

	for _, server := range grpcServers {
		// stop the grpc server.
		server.Stop()
	}
	grpcServers = nil
	grpcServersMutex.Unlock()
}

func setUpGrpcListenersAndServ(mgr *cbgt.Manager,
	options map[string]string) {

	if flags.BindGRPC != "" {
		setUpGrpcListenersAndServUtil(mgr, flags.BindGRPC, false, options, "")
	}

	if flags.BindGRPCSSL != "" {
		authType = options["authType"]

		if authType == "cbauth" {
			// Registering a TLS refresh callback with cbauth, which
			// will be responsible for updating https listeners,
			// whenever ssl certificates or the client cert auth settings
			// are changed.
			handleConfigChanges := func() error {
				// restart the servers in case of a refresh
				setUpGrpcListenersAndServUtil(mgr, flags.BindGRPCSSL, true, options,
					authType)
				return nil
			}

			cbgt.RegisterConfigRefreshCallback("fts/grpc-ssl", handleConfigChanges)
		}

		setUpGrpcListenersAndServUtil(mgr, flags.BindGRPCSSL, true, options, authType)
	}
}

func setUpGrpcListenersAndServUtil(mgr *cbgt.Manager, bindPORT string,
	secure bool, options map[string]string, authType string) {
	ipv6 = options["ipv6"]

	if secure {
		// close any previously open grpc servers
		closeAndClearGrpcServerList()
	}

	bindGRPCList := strings.Split(bindPORT, ",")
	anyHostPorts := map[string]bool{}
	// bind to 0.0.0.0's (IPv4) or [::]'s (IPv6) first for grpc listening.
	for _, bindGRPC := range bindGRPCList {
		if strings.HasPrefix(bindGRPC, "0.0.0.0:") ||
			strings.HasPrefix(bindGRPC, "[::]:") {
			go startGrpcServer(mgr, bindGRPC, secure, nil, authType)

			anyHostPorts[bindGRPC] = true
		}
	}

	for i := len(bindGRPCList) - 1; i >= 1; i-- {
		go startGrpcServer(mgr, bindGRPCList[i], secure, anyHostPorts, authType)
	}
}

func startGrpcServer(mgr *cbgt.Manager, bindGRPC string, secure bool,
	anyHostPorts map[string]bool, authType string) {
	if bindGRPC[0] == ':' {
		bindGRPC = "localhost" + bindGRPC
	}

	if anyHostPorts != nil && len(bindGRPC) > 0 {
		// if we've already bound to 0.0.0.0 or [::] on the same port, then
		// skip this hostPort.
		portIndex := strings.LastIndex(bindGRPC, ":") + 1
		if portIndex > 0 && portIndex < len(bindGRPC) {
			// possibly valid port available.
			port := bindGRPC[portIndex:]
			if _, err := strconv.Atoi(port); err == nil {
				// valid port.
				host := "0.0.0.0"
				if net.ParseIP(bindGRPC[:portIndex-1]).To4() == nil &&
					// not an IPv4
					ipv6 == "true" {
					host = "[::]"
				}

				anyHostPort := host + ":" + port
				if anyHostPorts[anyHostPort] {
					if anyHostPort != bindGRPC {
						log.Printf("init_grpc: GRPC is available"+
							" (via %v): %s", host, bindGRPC)
					}
					return
				}
			}
		} // else port not found.
	}

	listener, err := net.Listen("tcp", bindGRPC)
	if err != nil {
		log.Fatalf("init_grpc: mainGrpcServer, failed to listen: %v", err)
	}

	opts := getGrpcOpts(secure, authType)

	s := grpc.NewServer(opts...)

	searchSrv := &cbft.SearchService{}
	searchSrv.SetManager(mgr)
	pb.RegisterSearchServiceServer(s, searchSrv)

	reflection.Register(s)

	if secure {
		grpcServersMutex.Lock()
		grpcServers = append(grpcServers, s)
		grpcServersMutex.Unlock()
		atomic.AddUint64(&cbft.TotGRPCSListenersOpened, 1)
	} else {
		atomic.AddUint64(&cbft.TotGRPCListenersOpened, 1)
	}

	log.Printf("init_grpc: GrpcServer Started at %s", bindGRPC)
	if err := s.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	if secure {
		atomic.AddUint64(&cbft.TotGRPCSListenersClosed, 1)
		return
	}

	atomic.AddUint64(&cbft.TotGRPCListenersClosed, 1)
}

func getGrpcOpts(secure bool, authType string) []grpc.ServerOption {
	opts := []grpc.ServerOption{
		cbft.AddServerInterceptor(),
		grpc.MaxConcurrentStreams(cbft.DefaultGrpcMaxConcurrentStreams),
		grpc.MaxSendMsgSize(cbft.DefaultGrpcMaxRecvMsgSize),
		grpc.MaxRecvMsgSize(cbft.DefaultGrpcMaxSendMsgSize),
		// TODO add more configurability
	}

	if secure {
		var err error
		config := &tls.Config{}
		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(
			cbgt.TLSCertFile, cbgt.TLSKeyFile)
		if err != nil {
			log.Fatalf("init_grpc: LoadX509KeyPair, err: %v", err)
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
							log.Fatalf("init_grpc: ReadFile of cacert, err: %v", err)
						}
					}
					ok := caCertPool.AppendCertsFromPEM(certBytes)
					if !ok {
						log.Fatalf("init_grpc: error in appending certificates")
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

		creds := credentials.NewTLS(config)

		opts = append(opts, grpc.Creds(creds))
	}

	return opts
}

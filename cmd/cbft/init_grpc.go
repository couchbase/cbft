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
	"net"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

func setupGRPCListenersAndServe(mgr *cbgt.Manager) {
	bindGRPCList := strings.Split(flags.BindGRPC, ",")
	bindGRPCSList := strings.Split(flags.BindGRPCSSL, ",")

	if authType == "cbauth" {
		// register a security refresh callback with cbauth, which is
		// responsible for upating the listeners on refresh
		handleConfigChanges := func(status int) error {
			// restart the servers in case of a refresh
			setupGRPCListeners(mgr, bindGRPCList, bindGRPCSList, status)
			return nil
		}
		cbgt.RegisterConfigRefreshCallback("fts/grpc,grpc-ssl", handleConfigChanges)
	}

	setupGRPCListeners(mgr, bindGRPCList, bindGRPCSList,
		cbgt.AuthChange_nonSSLPorts|cbgt.AuthChange_certificates)
}

func setupGRPCListeners(mgr *cbgt.Manager,
	bindGRPCList, bindGRPCSList []string, status int) error {
	startGRPCListeners := func(ssl bool, servers []string) {
		anyHostPorts := map[string]bool{}
		// bind to 0.0.0.0's (IPv4) or [::]'s (IPv6) first for grpc listening.
		for _, server := range servers {
			if strings.HasPrefix(server, "0.0.0.0:") ||
				strings.HasPrefix(server, "[::]:") {
				startGrpcServer(mgr, server, ssl, nil)
				anyHostPorts[server] = true
			}
		}

		for i := len(servers) - 1; i >= 1; i-- {
			startGrpcServer(mgr, servers[i], ssl, anyHostPorts)
		}
	}

	if status&cbgt.AuthChange_nonSSLPorts != 0 {
		// close any previously opened grpc servers
		serversCache.shutdownGrpcServers(false)

		ss := cbgt.GetSecuritySetting()
		if ss == nil || !ss.DisableNonSSLPorts {
			startGRPCListeners(false, bindGRPCList)
		}
	}

	if status&cbgt.AuthChange_certificates != 0 {
		// close any previously opened grpc-ssl servers
		serversCache.shutdownGrpcServers(true)
		startGRPCListeners(true, bindGRPCSList)
	}

	return nil
}

func startGrpcServer(mgr *cbgt.Manager, bindGRPC string,
	ssl bool, anyHostPorts map[string]bool) {
	if len(bindGRPC) == 0 {
		return
	}

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

	setupGRPCServer := func(listener net.Listener) {
		go func(listener net.Listener) {
			opts := getGrpcOpts(ssl, authType)

			s := grpc.NewServer(opts...)

			searchSrv := &cbft.SearchService{}
			searchSrv.SetManager(mgr)
			pb.RegisterSearchServiceServer(s, searchSrv)

			reflection.Register(s)

			serversCache.registerGrpcServer(s, ssl)
			if ssl {
				atomic.AddUint64(&cbft.TotGRPCSListenersOpened, 1)
			} else {
				atomic.AddUint64(&cbft.TotGRPCListenersOpened, 1)
			}
			log.Printf("init_grpc: GrpcServer Started at %q", bindGRPC)
			if err := s.Serve(listener); err != nil {
				log.Warnf("init_grpc: Serve, err: %v;"+
					" closed gRPC listener on bindGRPC: %q", err, bindGRPC)
			}

			if ssl {
				atomic.AddUint64(&cbft.TotGRPCSListenersClosed, 1)
				return
			}
			atomic.AddUint64(&cbft.TotGRPCListenersClosed, 1)
		}(listener)
	}

	listener, err := net.Listen("tcp", bindGRPC)
	if err != nil {
		log.Fatalf("init_grpc: mainGrpcServer, failed to listen: %v", err)
	} else if listener != nil {
		setupGRPCServer(listener)
	} else {
		log.Warnf("init_grpc: listener not set up for %s", bindGRPC)
	}
}

func getGrpcOpts(ssl bool, authType string) []grpc.ServerOption {
	opts := []grpc.ServerOption{
		cbft.AddServerInterceptor(),
		grpc.MaxConcurrentStreams(cbft.DefaultGrpcMaxConcurrentStreams),
		grpc.MaxSendMsgSize(cbft.DefaultGrpcMaxRecvMsgSize),
		grpc.MaxRecvMsgSize(cbft.DefaultGrpcMaxSendMsgSize),
		// TODO add more configurability
	}

	if ssl {
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
					certInBytes := ss.CertInBytes
					if len(certInBytes) == 0 {
						// if no CertInBytes found in settings, then fallback
						// to reading directly from file. Upon any certs change
						// callbacks later, reboot of servers will ensure the
						// latest certificates in the servers.
						certInBytes, err = ioutil.ReadFile(cbgt.TLSCertFile)
						if err != nil {
							log.Fatalf("init_grpc: ReadFile of cacert, err: %v", err)
						}
					}
					ok := caCertPool.AppendCertsFromPEM(certInBytes)
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

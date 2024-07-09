//  Copyright 2019-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"net"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/couchbase/cbft"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
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
					ipv6 != ip_off {
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

	setupGRPCServer := func(listener net.Listener, nwp string, address string) {
		go func(nwp string, listener net.Listener) {
			s := serversCache.registerNewGRPCServer(nwp, address, ssl, authType)
			searchSrv := &cbft.SearchService{}
			searchSrv.SetManager(mgr)
			pb.RegisterSearchServiceServer(s.server, searchSrv)

			reflection.Register(s.server)
			if ssl {
				atomic.AddUint64(&cbft.TotGRPCSListenersOpened, 1)
			} else {
				atomic.AddUint64(&cbft.TotGRPCListenersOpened, 1)
			}
			log.Printf("init_grpc: GrpcServer Started at %q, proto: %q", bindGRPC, nwp)
			if err := s.server.Serve(listener); err != nil {
				log.Warnf("init_grpc: Serve, err: %v;"+
					" closed gRPC listener on bindGRPC: %q, proto: %q", err, bindGRPC, nwp)
			}

			if ssl {
				atomic.AddUint64(&cbft.TotGRPCSListenersClosed, 1)
				return
			}
			atomic.AddUint64(&cbft.TotGRPCListenersClosed, 1)
		}(nwp, listener)
	}

	if ipv6 != ip_off {
		// Check if server already exists before opening another
		s := serversCache.getGRPCServer(ssl, bindGRPC, "tcp6")
		if s != nil {
			config := s.getTLSConfig()
			config = newTLSConfig(config)
			s.setTLSConfig(config)
		} else {
			listener, err := getListener(bindGRPC, "tcp6")
			if err != nil {
				if ipv6 == ip_required {
					log.Fatalf("init_grpc: listen on ipv6, err: %v", err)
				} else { // ip_optional
					log.Errorf("init_grpc: listen on ipv6, err: %v", err)
				}
			} else if listener != nil {
				setupGRPCServer(listener, "tcp6", bindGRPC)
			} else {
				log.Warnf("init_grpc: ipv6 listener not set up for %s", bindGRPC)
			}
		}
	}

	if ipv4 != ip_off {
		// Check if server already exists before opening another
		s := serversCache.getGRPCServer(ssl, bindGRPC, "tcp4")
		if s != nil {
			config := s.getTLSConfig()
			config = newTLSConfig(config)
			s.setTLSConfig(config)
		} else {
			listener, err := getListener(bindGRPC, "tcp4")
			if err != nil {
				if ipv4 == ip_required {
					log.Fatalf("init_grpc: listen on ipv4, err: %v", err)
				} else { // ip_optional
					log.Errorf("init_grpc: listen on ipv4, err: %v", err)
				}
			} else if listener != nil {
				setupGRPCServer(listener, "tcp4", bindGRPC)
			} else {
				log.Warnf("init_grpc: ipv4 listener not set up for %s", bindGRPC)
			}
		}
	}
}

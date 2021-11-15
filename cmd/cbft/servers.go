//  Copyright 2021-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"net/http"
	"sync"

	"google.golang.org/grpc"
)

type serverCache struct {
	m sync.Mutex

	httpServers  []*http.Server
	httpsServers []*http.Server

	grpcServers    []*grpc.Server
	grpcSSLServers []*grpc.Server
}

// Add http.Server to cache
func (s *serverCache) registerHttpServer(server *http.Server, https bool) {
	s.m.Lock()
	if https {
		s.httpsServers = append(s.httpsServers, server)
	} else {
		s.httpServers = append(s.httpServers, server)
	}
	s.m.Unlock()
}

// Shutdown the chosen http.Servers
func (s *serverCache) shutdownHttpServers(https bool) {
	s.m.Lock()
	// Upon invoking Close() for a server, the blocking Serve(..) for
	// the server will return ErrServerClosed and is also responsible
	// for closing the listener that it's accepting incoming
	// connections on.
	if https {
		for _, entry := range s.httpsServers {
			entry.Close()
		}
		s.httpsServers = s.httpsServers[:0]
	} else {
		for _, entry := range s.httpServers {
			entry.Close()
		}
		s.httpServers = s.httpServers[:0]
	}
	s.m.Unlock()
}

// Add grpc.Server to cache
func (s *serverCache) registerGrpcServer(server *grpc.Server, ssl bool) {
	s.m.Lock()
	if ssl {
		s.grpcSSLServers = append(s.grpcSSLServers, server)
	} else {
		s.grpcServers = append(s.grpcServers, server)
	}
	s.m.Unlock()
}

// Shutdown the chosen grpc.Servers
func (s *serverCache) shutdownGrpcServers(ssl bool) {
	s.m.Lock()
	if ssl {
		for _, server := range s.grpcSSLServers {
			// stop the grpc server.
			server.Stop()
		}
		s.grpcSSLServers = s.grpcSSLServers[:0]
	} else {
		for _, server := range s.grpcServers {
			// stop the grpc server.
			server.Stop()
		}
		s.grpcServers = s.grpcServers[:0]
	}
	s.m.Unlock()
}

// -----------------------------------------------------------------------------

var serversCache = &serverCache{}

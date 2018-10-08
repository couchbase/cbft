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
)

var grpcServers []*grpc.Server
var grpcServersMutex sync.Mutex

// Close all Grpc Servers
func closeAndClearGrpcServerList() {
	grpcServersMutex.Lock()

	for _, server := range grpcServers {
		// gracefully stop the grpc server.
		server.GracefulStop()
	}
	grpcServers = nil
	grpcServersMutex.Unlock()
}

func startGrpcServer(mgr *cbgt.Manager, flags *cbftFlags) {
	closeAndClearGrpcServerList()

	hostPort := mgr.BindHttp()
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		log.Fatalf("init_grpc: mainGrpcServer, SplitHostPort: %v", err)
	}

	if len(flags.BindGRPC) > 0 {
		portIndex := strings.LastIndex(flags.BindGRPC, ":") + 1
		if portIndex > 0 && portIndex < len(flags.BindGRPC) {
			// possibly valid port available.
			p := flags.BindGRPC[portIndex:]
			if _, err = strconv.Atoi(p); err == nil {
				// valid port.
				cbft.GrpcPort = ":" + p
			}
		}
	}

	var creds credentials.TransportCredentials
	creds, err = credentials.NewServerTLSFromFile(flags.TLSCertFile,
		flags.TLSKeyFile)
	if err != nil {
		log.Fatalf("init_grpc: NewServerTLSFromFile, err: %v", err)
	}

	hostPort = host + cbft.GrpcPort
	lis, err := net.Listen("tcp", hostPort)
	if err != nil {
		log.Fatalf("init_grpc: mainGrpcServer, failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{
		grpc.Creds(creds),
		cbft.AddServerInterceptor(),
		grpc.MaxConcurrentStreams(cbft.DefaultGrpcMaxConcurrentStreams),
		grpc.MaxSendMsgSize(cbft.DefaultGrpcMaxRecvMsgSize),
		grpc.MaxRecvMsgSize(cbft.DefaultGrpcMaxSendMsgSize),
		// TODO add more configurability
	}

	s := grpc.NewServer(opts...)

	searchSrv := &cbft.SearchService{}
	searchSrv.SetManager(mgr)
	pb.RegisterSearchServiceServer(s, searchSrv)

	reflection.Register(s)

	grpcServers = append(grpcServers, s)

	log.Printf("init_grpc: GrpcServer Started at %s", hostPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

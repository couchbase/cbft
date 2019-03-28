//  Copyright (c) 2019 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cbft

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/search/query"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"

	"github.com/couchbase/cbauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// RPCClientConn represent the gRPC client connection cache.
var RPCClientConn map[string][]*grpc.ClientConn

var rpcConnMutex sync.Mutex

// rpc.ClientConn pool size/static connections per remote host
var connPoolSize = 5

// GrpcPort represents the port used with gRPC.
var GrpcPort = ":15000"

// default values same as that for http/rest connections
var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxBackOffDelay = time.Duration(10) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 50 // 50 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 50 // 50 MB

var DefaultGrpcMaxConcurrentStreams = uint32(5000)

var rsource rand.Source
var r1 *rand.Rand

func init() {
	RPCClientConn = make(map[string][]*grpc.ClientConn, 10)
	rsource = rand.NewSource(time.Now().UnixNano())
	r1 = rand.New(rsource)
}

// basicAuthCreds is an implementation of credentials.PerRPCCredentials
// that transforms the username and password into a base64 encoded value
// similar to HTTP Basic xxx
type basicAuthCreds struct {
	username, password string
}

// GetRequestMetadata sets the value for "authorization" key
func (b *basicAuthCreds) GetRequestMetadata(context.Context,
	...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + basicAuth(b.username, b.password),
	}, nil
}

// RequireTransportSecurity should return true only when the base64
// credentials have to be encrypted over the wire. (strictly tls)
func (b *basicAuthCreds) RequireTransportSecurity() bool {
	return false // to support non-tls mode
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func GetRpcClient(nodeUUID, hostPort string,
	certsPEM interface{}) (pb.SearchServiceClient, error) {
	var hostPool []*grpc.ClientConn
	var initialised bool

	key := nodeUUID + "-" + hostPort
	index := r1.Intn(connPoolSize)

	rpcConnMutex.Lock()
	if hostPool, initialised = RPCClientConn[key]; !initialised {
		opts, err := getGrpcOpts(hostPort, certsPEM)
		if err != nil {
			log.Printf("grpc_client: getGrpcOpts, host port: %s err: %v",
				hostPort, err)
			rpcConnMutex.Unlock()
			return nil, err
		}

		for i := 0; i < connPoolSize; i++ {
			conn, err := grpc.Dial(hostPort, opts...)
			if err != nil {
				log.Printf("grpc_client: grpc.Dial, err: %v", err)
				rpcConnMutex.Unlock()
				return nil, err
			}

			log.Printf("grpc_client: grpc ClientConn Created %d for host: %s", i, key)

			RPCClientConn[key] = append(RPCClientConn[key], conn)
		}
		hostPool = RPCClientConn[key]
	}

	rpcConnMutex.Unlock()

	// TODO connection mgmt
	// when to perform explicit conn.Close()?
	cli := pb.NewSearchServiceClient(hostPool[index])

	if certsPEM != nil {
		atomic.AddUint64(&totRemoteGrpcSecure, 1)
	} else {
		atomic.AddUint64(&totRemoteGrpc, 1)
	}

	return cli, nil
}

func getGrpcOpts(hostPort string, certsPEM interface{}) ([]grpc.DialOption, error) {
	cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
	if err != nil {
		return nil, fmt.Errorf("grpc_util: cbauth err: %v", err)
	}

	opts := []grpc.DialOption{
		grpc.WithBackoffMaxDelay(DefaultGrpcMaxBackOffDelay),

		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// send keepalive every 60 seconds to check the
			// connection livliness
			Time: DefaultGrpcConnectionHeartBeatInterval,
			// timeout value for an inactive connection
			Timeout: DefaultGrpcConnectionIdleTimeout,
		}),

		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(DefaultGrpcMaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(DefaultGrpcMaxSendMsgSize),
		),

		grpc.WithPerRPCCredentials(&basicAuthCreds{
			username: cbUser,
			password: cbPasswd,
		}),
	}

	if certsPEM != nil {
		// create a certificate pool from the CA
		certPool := x509.NewCertPool()
		// append the certificates from the CA
		ok := certPool.AppendCertsFromPEM([]byte(certsPEM.(string)))
		if !ok {
			return nil, fmt.Errorf("grpc_util: failed to append ca certs")
		}

		creds := credentials.NewClientTLSFromCert(certPool, "")

		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	return opts, nil
}

func parseStringTime(t string) (time.Time, error) {
	dateTimeParser, err := cache.DateTimeParserNamed(query.QueryDateTimeParser)
	if err != nil {
		return time.Time{}, err
	}
	var ti time.Time
	ti, err = dateTimeParser.ParseDateTime(t)
	if err != nil {
		return time.Time{}, err
	}
	return ti, nil
}

// GRPCPathStats represents the stats for a gRPC path spec.
type GRPCPathStats struct {
	m sync.Mutex

	focusStats map[string]*RPCFocusStats
}

// FocusStats returns the RPCFocusStats for a given focus value like
// an indexName
func (s *GRPCPathStats) FocusStats(focusVal string) *RPCFocusStats {
	s.m.Lock()
	if s.focusStats == nil {
		s.focusStats = map[string]*RPCFocusStats{}
	}
	rv, exists := s.focusStats[focusVal]
	if !exists {
		rv = &RPCFocusStats{}
		s.focusStats[focusVal] = rv
	}
	s.m.Unlock()
	return rv
}

// FocusValues returns the focus value strings
func (s *GRPCPathStats) FocusValues() (rv []string) {
	s.m.Lock()
	for focusVal := range s.focusStats {
		rv = append(rv, focusVal)
	}
	s.m.Unlock()
	return rv
}

// -------------------------------------------------------

// RPCFocusStats represents stats for a targeted or "focused" gRPC
// endpoint.
type RPCFocusStats struct {
	TotGrpcRequest             uint64
	TotGrpcRequestTimeNS       uint64
	TotGrpcRequestErr          uint64 `json:"TotGrpcRequestErr,omitempty"`
	TotGrpcRequestSlow         uint64 `json:"TotGrpcRequestSlow,omitempty"`
	TotGrpcRequestTimeout      uint64 `json:"TotGrpcRequestTimeout,omitempty"`
	TotGrpcResponseBytes       uint64 `json:"TotGrpcResponseBytes,omitempty"`
	TotGrpcClientRequest       uint64
	TotGrpcClientRequestTimeNS uint64
}

func updateRpcFocusStats(startTime time.Time, mgr *cbgt.Manager,
	req *pb.SearchRequest, ctx context.Context, err error) {
	focusStats := GrpcPathStats.FocusStats(req.IndexName)
	if focusStats != nil {
		slowQueryLogTimeoutV := mgr.Options()["slowQueryLogTimeout"]
		if slowQueryLogTimeoutV != "" {
			var slowQueryLogTimeout time.Duration
			slowQueryLogTimeout, err = time.ParseDuration(slowQueryLogTimeoutV)
			if err == nil {
				d := time.Since(startTime)
				if d > slowQueryLogTimeout {
					log.Printf("grpc_util: slow-query index: %s,"+
						" query: %s, duration: %v, err: %v",
						req.IndexName, string(req.Contents), d, err)

					atomic.AddUint64(&focusStats.TotGrpcRequestSlow, 1)
				}
			}
		}

		// check whether its a client request
		if _, err = extractMetaHeader(ctx, "rpcclusteractionkey"); err != nil {
			atomic.AddUint64(&focusStats.TotGrpcClientRequest, 1)
			atomic.AddUint64(&focusStats.TotGrpcClientRequestTimeNS,
				uint64(time.Now().Sub(startTime)))
		}

		if err != nil {
			atomic.AddUint64(&focusStats.TotGrpcRequestErr, 1)
			if err == context.DeadlineExceeded {
				atomic.AddUint64(&focusStats.TotGrpcRequestTimeout, 1)
			}
		}

		atomic.AddUint64(&focusStats.TotGrpcRequest, 1)
	}
}

//  Copyright 2019-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbft

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"

	"github.com/couchbase/cbauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// RPCClientConn represent the gRPC client connection cache.
var RPCClientConn map[string][]*grpc.ClientConn

var rpcConnMutex sync.Mutex

// rpc.ClientConn pool size/static connections per remote host
var connPoolSize = 5

var defaultRPCClientCacheSize = 10

// default values same as that for http/rest connections
var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxBackOffDelay = time.Duration(10) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 50 // 50 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 50 // 50 MB

// The number of concurrent streams/requests on a client connection can only
// be controlled by adjusting the server value. Set a very large value for the
// server config so that we have no fixed limit on the number of concurrent
// streams/requests on either the client or server.
var DefaultGrpcMaxConcurrentStreams = uint32(math.MaxInt32)

func init() {
	RPCClientConn = make(map[string][]*grpc.ClientConn, defaultRPCClientCacheSize)
	rand.Seed(time.Now().UnixNano())
}

// resetGrpcClients is used to reset the existing clients so that
// further getRpcClient calls will get newer clients with the fresh
// and the latest configs.
func resetGrpcClients() error {
	rpcConnMutex.Lock()
	RPCClientConn = make(map[string][]*grpc.ClientConn, defaultRPCClientCacheSize)
	rpcConnMutex.Unlock()
	return nil
}

// basicAuthCreds is an implementation of credentials.PerRPCCredentials
// that transforms the username and password into a base64 encoded value
// similar to HTTP Basic xxx
type basicAuthCreds struct {
	username                 string
	password                 string
	requireTransportSecurity bool
}

// GetRequestMetadata sets the value for "authorization" key
func (b *basicAuthCreds) GetRequestMetadata(context.Context,
	...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Basic " + basicAuth(b.username, b.password),
	}, nil
}

// RequireTransportSecurity indicates whether the credentials requires
// transport security.
func (b *basicAuthCreds) RequireTransportSecurity() bool {
	return b.requireTransportSecurity
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func getRpcClient(nodeUUID, hostPort string, certInBytes []byte,
	clientCert tls.Certificate, clientAuth tls.ClientAuthType) (
	pb.SearchServiceClient, error) {
	var hostPool []*grpc.ClientConn
	var initialised bool

	key := nodeUUID + "-" + hostPort
	index := rand.Intn(connPoolSize)

	rpcConnMutex.Lock()
	if hostPool, initialised = RPCClientConn[key]; !initialised {
		opts, err := getGrpcOpts(hostPort, certInBytes, clientCert, clientAuth)
		if err != nil {
			rpcConnMutex.Unlock()
			log.Errorf("grpc_client: getGrpcOpts, host port: %s, err: %v",
				hostPort, err)
			return nil, err
		}

		for i := 0; i < connPoolSize; i++ {
			conn, err := grpc.Dial(hostPort, opts...)
			if err != nil {
				rpcConnMutex.Unlock()
				log.Errorf("grpc_client: grpc.Dial, err: %v", err)
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

	if len(certInBytes) == 0 {
		atomic.AddUint64(&totRemoteGrpc, 1)
	} else {
		atomic.AddUint64(&totRemoteGrpcSsl, 1)
	}

	return cli, nil
}

func getGrpcOpts(hostPort string, certInBytes []byte, clientCert tls.Certificate,
	clientAuth tls.ClientAuthType) ([]grpc.DialOption, error) {
	cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
	if err != nil {
		return nil, fmt.Errorf("grpc_util: cbauth err: %v", err)
	}

	bac := &basicAuthCreds{
		username: cbUser,
		password: cbPasswd,
	}

	if len(certInBytes) != 0 {
		bac.requireTransportSecurity = true
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

		grpc.WithPerRPCCredentials(bac),
	}

	if len(certInBytes) != 0 {
		// create a certificate pool from the CA
		certPool := x509.NewCertPool()
		// append the certificates from the CA
		ok := certPool.AppendCertsFromPEM(certInBytes)
		if !ok {
			return nil, fmt.Errorf("grpc_util: failed to append ca certs")
		}
		var creds credentials.TransportCredentials
		if clientAuth != tls.NoClientCert {
			creds = credentials.NewTLS(&tls.Config{
				RootCAs:      certPool,
				Certificates: []tls.Certificate{clientCert},
				ClientAuth:   clientAuth,
			})
		} else {
			creds = credentials.NewClientTLSFromCert(certPool, "")
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	return opts, nil
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

func (s *GRPCPathStats) ResetFocusStats(focusVal string) {
	s.m.Lock()
	if s.focusStats != nil {
		if _, exists := s.focusStats[focusVal]; exists {
			s.focusStats[focusVal] = &RPCFocusStats{}
		}
	}
	s.m.Unlock()
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

// GrpcIndexQueryPath is keyed by path spec strings.
var GrpcPathStats = GRPCPathStats{
	focusStats: map[string]*RPCFocusStats{},
}

// -------------------------------------------------------

// RPCFocusStats represents stats for a targeted or "focused" gRPC
// endpoint.
type RPCFocusStats struct {
	TotGrpcRequest               uint64
	TotGrpcRequestTimeNS         uint64
	TotGrpcRequestErr            uint64 `json:"TotGrpcRequestErr,omitempty"`
	TotGrpcRequestSlow           uint64 `json:"TotGrpcRequestSlow,omitempty"`
	TotGrpcRequestTimeout        uint64 `json:"TotGrpcRequestTimeout,omitempty"`
	TotGrpcResponseBytes         uint64 `json:"TotGrpcResponseBytes,omitempty"`
	TotGrpcInternalRequest       uint64
	TotGrpcInternalRequestTimeNS uint64
}

func updateRpcFocusStats(startTime time.Time, mgr *cbgt.Manager,
	req *pb.SearchRequest, ctx context.Context, err error) {
	focusStats := GrpcPathStats.FocusStats(req.IndexName)
	if focusStats != nil {
		// check whether its a client request and track only in the coordinating node
		if _, er := extractMetaHeader(ctx, rpcClusterActionKey); er != nil {
			// co-ordinating node
			atomic.AddUint64(&focusStats.TotGrpcRequest, 1)
			atomic.AddUint64(&focusStats.TotGrpcRequestTimeNS,
				uint64(time.Now().Sub(startTime)))

			slowQueryLogTimeoutV := mgr.GetOption("slowQueryLogTimeout")
			if slowQueryLogTimeoutV != "" {
				var slowQueryLogTimeout time.Duration
				slowQueryLogTimeout, err = time.ParseDuration(slowQueryLogTimeoutV)
				if err == nil {
					d := time.Since(startTime)
					if d > slowQueryLogTimeout {
						log.Warnf("grpc_util: slow-query index: %s,"+
							" query: %s, duration: %v, err: %v",
							req.IndexName, string(req.Contents), d, err)

						atomic.AddUint64(&focusStats.TotGrpcRequestSlow, 1)
					}
				}
			}

			if err != nil {
				atomic.AddUint64(&focusStats.TotGrpcRequestErr, 1)
				if err == context.DeadlineExceeded {
					atomic.AddUint64(&focusStats.TotGrpcRequestTimeout, 1)
				}
			}
		} else {
			// not a co-ordinating node
			atomic.AddUint64(&focusStats.TotGrpcInternalRequest, 1)
			atomic.AddUint64(&focusStats.TotGrpcInternalRequestTimeNS,
				uint64(time.Now().Sub(startTime)))
		}
	}
}

// httpStatusCodes map the canonical gRPC codes to HTTP status codes
// as per https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
func httpStatusCodes(code codes.Code) int {
	switch code {
	case codes.OK:
		return 200
	case codes.Canceled:
		return 499
	case codes.Unknown:
		return 500
	case codes.InvalidArgument:
		return 400
	case codes.DeadlineExceeded:
		return 504
	case codes.NotFound:
		return 404
	case codes.AlreadyExists:
		return 409
	case codes.PermissionDenied:
		return 403
	case codes.ResourceExhausted:
		return 429
	case codes.FailedPrecondition:
		return 412 // custom override for fts
	case codes.Aborted:
		return 409
	case codes.OutOfRange:
		return 400
	case codes.Unimplemented:
		return 501
	case codes.Internal:
		return 500
	case codes.Unavailable:
		return 503
	case codes.DataLoss:
		return 500
	case codes.Unauthenticated:
		return 401
	default:
		return 500
	}
}

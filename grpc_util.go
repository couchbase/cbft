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
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/query"
	pb "github.com/couchbase/cbft/protobuf"
	log "github.com/couchbase/clog"
	"github.com/golang/protobuf/ptypes/duration"

	"github.com/couchbase/cbauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// RPCClientConn represent the gRPC client connection cache.
var RPCClientConn map[string][]*grpc.ClientConn

var rpcConnMutex sync.Mutex

// GrpcPort represents the port used with gRPC.
var GrpcPort = ":15000"

// default values same as that for http/rest connections
var DefaultGrpcConnectionIdleTimeout = time.Duration(60) * time.Second
var DefaultGrpcConnectionHeartBeatInterval = time.Duration(60) * time.Second

var DefaultGrpcMaxBackOffDelay = time.Duration(10) * time.Second

var DefaultGrpcMaxRecvMsgSize = 1024 * 1024 * 20 // 20 MB
var DefaultGrpcMaxSendMsgSize = 1024 * 1024 * 20 // 20 MB

var DefaultGrpcMaxConcurrentStreams = uint32(2000)

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

// RequireTransportSecurity should be true as even though the credentials
// are base64, we want to have it encrypted over the wire.
func (b *basicAuthCreds) RequireTransportSecurity() bool {
	return false // TODO - make it true
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func GetRpcClient(nodeUUID, hostPort string,
	certsPEM interface{}) (pb.SearchServiceClient, error) {
	// create a certificate pool from the CA
	certPool := x509.NewCertPool()
	// append the certificates from the CA
	ok := certPool.AppendCertsFromPEM([]byte(certsPEM.(string)))
	if !ok {
		return nil, fmt.Errorf("grpc_util: failed to append ca certs")
	}
	creds := credentials.NewClientTLSFromCert(certPool, "")

	cbUser, cbPasswd, err := cbauth.GetHTTPServiceAuth(hostPort)
	if err != nil {
		return nil, fmt.Errorf("grpc_util: cbauth err: %v", err)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),

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

		addClientInterceptor(),

		grpc.WithPerRPCCredentials(&basicAuthCreds{
			username: cbUser,
			password: cbPasswd,
		}),
	}

	var hostPool []*grpc.ClientConn
	var initialised bool
	if hostPool, initialised = RPCClientConn[nodeUUID]; !initialised {
		for i := 0; i < 10; i++ {
			conn, err := grpc.Dial(hostPort, opts...)
			if err != nil {
				log.Printf("grpc_client: grpc.Dial, err: %v", err)
				return nil, err
			}

			log.Printf("grpc_client: grpc ClientConn Created %d", i)
			rpcConnMutex.Lock()
			RPCClientConn[nodeUUID] = append(RPCClientConn[nodeUUID], conn)
			rpcConnMutex.Unlock()
		}
		hostPool = RPCClientConn[nodeUUID]
	}

	index := r1.Intn(10)
	// TODO connection mgmt
	// when to perform explicit conn.Close()?
	cli := pb.NewSearchServiceClient(hostPool[index])

	return cli, nil
}

func marshalProtoResults(searchResult *bleve.SearchResult) (*pb.StreamSearchResults, error) {
	result := &pb.StreamSearchResults_SearchResult{
		Status: &pb.SearchStatus{},
	}

	if searchResult.Status != nil {
		result.Status.Failed = int64(searchResult.Status.Failed)
		result.Status.Total = int64(searchResult.Status.Total)
		result.Status.Successful = int64(searchResult.Status.Successful)
	}
	result.MaxScore = searchResult.MaxScore
	result.Total = searchResult.Total

	var err error
	result.Hits, err = MarshalJSON(&searchResult.Hits)
	if err != nil {
		log.Printf("grpc_util, json err: %v", err)
		return nil, err
	}

	if searchResult.Facets != nil {
		result.Facets, err = MarshalJSON(&searchResult.Facets)
		if err != nil {
			log.Printf("grpc_util, json err: %v", err)
			return nil, err
		}
	}

	result.Took = &duration.Duration{Seconds: int64(searchResult.Took.Seconds()),
		Nanos: int32(searchResult.Took.Nanoseconds())}

	response := &pb.StreamSearchResults{
		PayLoad: &pb.StreamSearchResults_Results{
			Results: result,
		}}

	return response, nil
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

func makeSearchRequest(req *pb.SearchRequest) (*bleve.SearchRequest, error) {
	searchRequest := &bleve.SearchRequest{
		Sort: search.SortOrder{},
	}
	var err error
	searchRequest.Query, err = query.ParseQuery(req.Query)
	if err != nil {
		return nil, fmt.Errorf("parseQuery, err: %v", err)
	}

	searchRequest.Explain = req.Explain
	searchRequest.Fields = req.Fields
	searchRequest.From = int(req.From)
	searchRequest.Size = int(req.Size)
	searchRequest.IncludeLocations = req.IncludeLocations

	var temp struct {
		Sort []json.RawMessage `json:"sort"`
	}
	if req.Sort != nil {
		err = UnmarshalJSON(req.Sort, &temp.Sort)
		if err != nil {
			return nil, fmt.Errorf("parse Sort, json err: %v", err)
		}
	}

	if temp.Sort == nil {
		searchRequest.Sort = search.SortOrder{&search.SortScore{Desc: true}}
	} else {
		searchRequest.Sort, err = search.ParseSortOrderJSON(temp.Sort)
		if err != nil {
			return nil, fmt.Errorf("parseSortOrderJSON, err: %v", err)
		}
	}
	if req.Facets != nil {
		searchRequest.Facets = make(map[string]*bleve.FacetRequest,
			len(req.Facets.FacetsRequests))
		for k, v := range req.Facets.FacetsRequests {
			fr := bleve.NewFacetRequest(v.Field, int(v.Size))
			for _, dr := range v.DateTimeRanges {
				s, _ := parseStringTime(dr.Start)
				e, _ := parseStringTime(dr.End)
				fr.AddDateTimeRange(dr.Name, s, e)
			}

			for _, nr := range v.NumericRanges {
				fr.AddNumericRange(nr.Name, &nr.Min, &nr.Max)
			}

			searchRequest.Facets[k] = fr
		}
	}

	return searchRequest, nil
}

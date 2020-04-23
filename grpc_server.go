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
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var FeatureGRPC = "protocol:gRPC"

// atomic counter that keep track of the number of gRPC searches
var totRemoteGrpc uint64

var totRemoteGrpcSecure uint64

// totGrpcQueryRejectOnNotEnoughQuota tracks the number of rejected
// gRPC search requests on hitting the memory threshold for query
var totGrpcQueryRejectOnNotEnoughQuota uint64

// SearchService is an implementation for the SearchSrvServer
// gRPC search interface
type SearchService struct {
	mgr *cbgt.Manager
}

func (s *SearchService) SetManager(mgr *cbgt.Manager) {
	s.mgr = mgr
}

func (s *SearchService) Check(ctx context.Context,
	in *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	if in.Service == "" || in.Service == "Search" ||
		in.Service == "DocCount" {
		return &pb.HealthCheckResponse{
			Status: pb.HealthCheckResponse_SERVING,
		}, nil
	}
	return nil, status.Error(codes.NotFound, "unknown service")
}

func (s *SearchService) DocCount(ctx context.Context,
	req *pb.DocCountRequest) (*pb.DocCountResult, error) {
	pindex := s.mgr.GetPIndex(req.IndexName)
	if pindex == nil {
		return &pb.DocCountResult{DocCount: 0}, fmt.Errorf("grpc_server: "+
			"CountPIndex, no pindex, pindexName: %s", req.IndexName)
	}
	if pindex.Dest == nil {
		return &pb.DocCountResult{DocCount: 0}, fmt.Errorf("grpc_server: "+
			"CountPIndex, no pindex.Dest, pindexName: %s", req.IndexName)

	}

	if req.IndexUUID != "" && pindex.UUID != req.IndexUUID {
		return &pb.DocCountResult{DocCount: 0}, fmt.Errorf("grpc_server: "+
			"CountPIndex, wrong pindexUUID: %s, pindex.UUID: %s, pindexName: %s",
			req.IndexUUID, pindex.UUID, req.IndexName)
	}

	count, err := pindex.Dest.Count(pindex, nil)
	if err != nil {
		return &pb.DocCountResult{DocCount: 0}, fmt.Errorf("grpc_server: "+
			"CountPIndex, pindexName: %s, req: %#v, err: %v",
			req.IndexName, req, err)

	}

	return &pb.DocCountResult{DocCount: int64(count)}, nil
}

func (s *SearchService) Search(req *pb.SearchRequest,
	stream pb.SearchService_SearchServer) (err error) {
	startTime := time.Now()
	if req == nil {
		return status.Error(codes.FailedPrecondition,
			"grpc_server: Search empty search request")
	}

	defer func() {
		updateRpcFocusStats(startTime, s.mgr, req, stream.Context(), err)
	}()

	err = verifyRPCAuth(stream.Context(), req.IndexName, req)
	if err != nil {
		return status.Errorf(codes.PermissionDenied,
			"grpc_server: Search err: %v", err)
	}

	queryCtlParams := cbgt.QueryCtlParams{
		Ctl: cbgt.QueryCtl{
			Timeout: cbgt.QUERY_CTL_DEFAULT_TIMEOUT_MS,
		},
	}

	if req.QueryCtlParams != nil {
		err = UnmarshalJSON(req.QueryCtlParams, &queryCtlParams)
		if err != nil {
			return status.Errorf(codes.InvalidArgument,
				"grpc_server: Search parsing queryCtlParams, err: %v", err)
		}
	}

	queryPIndexes := QueryPIndexes{}
	if req.QueryPIndexes != nil {
		err = UnmarshalJSON(req.QueryPIndexes, &queryPIndexes)
		if err != nil {
			return status.Errorf(codes.InvalidArgument,
				"grpc_server: Search parsing queryPIndexes, err: %v", err)
		}
	}

	var sr *SearchRequest
	err = UnmarshalJSON(req.Contents, &sr)
	if err != nil {
		return status.Errorf(codes.InvalidArgument,
			"grpc_server: Search parsing searchRequest, err: %v", err)
	}
	var searchRequest *bleve.SearchRequest
	searchRequest, err = sr.ConvertToBleveSearchRequest()
	if err != nil {
		return status.Errorf(codes.InvalidArgument,
			"grpc_server: Search processing searchRequest, err: %v", err)
	}

	// pre process the query if applicable
	if strings.Compare(cbgt.CfgAppVersion, "7.0.0") >= 0 {
		hv, _ := extractMetaHeader(stream.Context(), rpcClusterActionKey)
		if hv != clusterActionScatterGather {
			searchRequest.Query = sr.decorateQuery(req.IndexName, searchRequest.Query, nil)
		}
	}

	if queryCtlParams.Ctl.Consistency != nil {
		err = ValidateConsistencyParams(queryCtlParams.Ctl.Consistency)
		if err != nil {
			return status.Errorf(codes.InvalidArgument,
				"grpc_server: Search validating consistency, err: %v", err)
		}
	}

	// always check for bleveMaxResultWindow, as there is a
	// third case of TopN and Streamed results.
	v, exists := s.mgr.Options()["bleveMaxResultWindow"]
	if exists {
		var bleveMaxResultWindow int
		bleveMaxResultWindow, err = strconv.Atoi(v)
		if err != nil {
			return status.Errorf(codes.InvalidArgument,
				"grpc_server: Search atoi: %v, err: %v", v, err)
		}

		if searchRequest.From+searchRequest.Size > bleveMaxResultWindow {
			err = status.Errorf(codes.InvalidArgument,
				"Validating request, err: %v",
				fmt.Errorf("grpc_server: Search bleveMaxResultWindow exceeded,"+
					" from: %d, size: %d, bleveMaxResultWindow: %d",
					searchRequest.From, searchRequest.Size,
					bleveMaxResultWindow))
			return err
		}
	}

	// phase 1 - set up timeouts, wait for local consistency reqiurements
	// to be satisfied, could return err 412

	// create a context with the appropriate timeout
	ctx, cancel, cancelCh := setupContextAndCancelCh(queryCtlParams, nil)
	// defer a call to cancel, this ensures that goroutine from
	// setupContextAndCancelCh always exits
	defer cancel()

	var onlyPIndexes map[string]bool
	if len(queryPIndexes.PIndexNames) > 0 {
		onlyPIndexes = cbgt.StringsToMap(queryPIndexes.PIndexNames)
	}

	alias, remoteClients, numPIndexes, er := bleveIndexAlias(s.mgr, req.IndexName,
		req.IndexUUID, true, queryCtlParams.Ctl.Consistency, cancelCh, true,
		onlyPIndexes, queryCtlParams.Ctl.PartitionSelection, addGrpcClients)
	if er != nil {
		if _, ok := er.(*cbgt.ErrorLocalPIndexHealth); !ok {
			err = status.Errorf(codes.Unavailable,
				"grpc_server: Search bleveIndexAlias, err: %v", er)
			return err
		}
	}

	var sh *streamer
	var handlerMaker search.MakeDocumentMatchHandler
	// check if the client requested streamed results/hits.
	if req.Stream {
		sh = newStreamHandler(searchRequest, stream)
		handlerMaker = sh.MakeDocumentMatchHandler
		ctx = context.WithValue(ctx, search.MakeDocumentMatchHandlerKey,
			handlerMaker)
		for _, rc := range remoteClients {
			if gc, ok := rc.(RemoteClient); ok {
				gc.SetStreamHandler(sh)
			}
		}
	}

	// estimate memory needed for merging search results from all
	// the pindexes
	mergeEstimate := uint64(numPIndexes) * bleve.MemoryNeededForSearchResult(searchRequest)
	err = fireQueryEvent(0, EventQueryStart, 0, mergeEstimate)
	if err != nil {
		atomic.AddUint64(&totGrpcQueryRejectOnNotEnoughQuota, 1)
		return status.Errorf(codes.ResourceExhausted,
			"grpc_server: Search query reject on not enough quota: %v", err)
	}

	defer fireQueryEvent(0, EventQueryEnd, 0, mergeEstimate)

	// set query start/end callbacks
	ctx = context.WithValue(ctx, bleve.SearchQueryStartCallbackKey,
		bleve.SearchQueryStartCallbackFn(bleveCtxQueryStartCallback))
	ctx = context.WithValue(ctx, bleve.SearchQueryEndCallbackKey,
		bleve.SearchQueryEndCallbackFn(bleveCtxQueryEndCallback))

	// register with the QuerySupervisor
	id := querySupervisor.AddEntry(&QuerySupervisorContext{
		Query:     searchRequest.Query,
		Cancel:    cancel,
		Size:      searchRequest.Size,
		From:      searchRequest.From,
		Timeout:   queryCtlParams.Ctl.Timeout,
		IndexName: req.IndexName,
	})
	defer querySupervisor.DeleteEntry(id)

	var searchResult *bleve.SearchResult
	searchResult, err = alias.SearchInContext(ctx, searchRequest)
	if searchResult != nil {
		err1 := processSearchResult(&queryCtlParams, req.IndexName, searchResult,
			remoteClients, err, er)
		if err1 != nil {
			err = status.Error(codes.DeadlineExceeded,
				fmt.Sprintf("grpc_server: Search searchInContext err: %v", err1))
			return err
		}

		if searchResult.Status != nil &&
			len(searchResult.Status.Errors) > 0 &&
			queryCtlParams.Ctl.Consistency != nil &&
			queryCtlParams.Ctl.Consistency.Results == "complete" {
			// complete results expected, do not propagate partial results
			return fmt.Errorf("grpc_server: results weren't retrieved from some"+
				" index partitions: %d", len(searchResult.Status.Errors))
		}

		response, er2 := MarshalJSON(searchResult)
		if er2 != nil {
			err = status.Errorf(codes.Internal,
				"grpc_server: Search response marshal err: %v", er2)
			return err
		}

		rv := &pb.StreamSearchResults{
			Contents: &pb.StreamSearchResults_SearchResult{
				SearchResult: response,
			}}

		if err = stream.Send(rv); err != nil {
			return status.Errorf(codes.Internal,
				"grpc_server: Search stream send, err: %v", err)
		}
	}

	return err
}

// TODO chaining of unary & stream interceptors can be done
// if neeeded for more stats/request tracking or debugging.
// eg: https://github.com/grpc-ecosystem/go-grpc-middleware

/*
	func serverInterceptor(ctx context.Context, req interface{},
	info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	response, err := handler(ctx, req)
	log.Printf("grpc_server: invoke server method: %s duration: %f sec err: %v",
		info.FullMethod, time.Since(start).Seconds(), err)
	return response, err
}*/

// wrappedServerStream is a thin wrapper around
// grpc.ServerStream that allows modifying context.
type wrappedServerStream struct {
	grpc.ServerStream
	// WrappedContext is the wrapper's own Context. You can assign it.
	wrappedContext context.Context
}

// Context returns the wrapper's wrappedContext,
// overwriting the nested grpc.ServerStream.Context()
func (w *wrappedServerStream) Context() context.Context {
	return w.wrappedContext
}

// wrapServerStream returns a ServerStream that has
// the ability to overwrite context.
func wrapServerStream(stream grpc.ServerStream) *wrappedServerStream {
	if existing, ok := stream.(*wrappedServerStream); ok {
		return existing
	}
	return &wrappedServerStream{ServerStream: stream,
		wrappedContext: stream.Context()}
}

func AddServerInterceptor() grpc.ServerOption {
	return grpc.StreamInterceptor(serverInterceptor)
}

func serverInterceptor(
	req interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) (err error) {
	// skip the authCallbacks wrapping/authentication for scatter gather calls,
	// as the user is already authenticated at the original node.
	if _, err = extractMetaHeader(ss.Context(), rpcClusterActionKey); err == nil {
		w := wrapServerStream(ss)
		w.wrappedContext = ss.Context()
		return handler(req, w)
	}

	nctx, err := wrapAuthCallbacks(req, ss.Context(), info.FullMethod)
	if err != nil {
		log.Printf("grpc_server: authenticate err: %+v", err)
		return err
	}

	w := wrapServerStream(ss)
	w.wrappedContext = nctx

	return handler(req, w)
}

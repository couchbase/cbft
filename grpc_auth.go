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
	"encoding/base64"
	"fmt"
	"github.com/couchbase/cbauth"
	pb "github.com/couchbase/cbft/protobuf"
	"github.com/couchbase/cbgt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strings"
)

type gRPCAuthKeyType string

var gRPCAuthHandlerKey = gRPCAuthKeyType("CheckRPCAuth")

type gRPCAuthHandler func(r requestParser) (bool, error)

// wrapAuthCallbacks embeds the right authentication callbacks
// into the context.
func wrapAuthCallbacks(req interface{},
	ctx context.Context, rpcPath string) (newCtx context.Context, err error) {
	newCtx, err = tryBasicAuth(req, ctx, rpcPath)
	if err == nil {
		return newCtx, nil
	}

	return ctx, err
}

func tryBasicAuth(req interface{}, ctx context.Context,
	rpcPath string) (context.Context, error) {
	srv := req.(*SearchService)
	if srv == nil {
		return nil, fmt.Errorf("invalid request type")
	}

	auth, err := extractMetaHeader(ctx, "authorization")
	if err != nil {
		return ctx, status.Errorf(codes.Unauthenticated,
			"err: %v", err)
	}

	const prefix = "Basic "
	if !strings.HasPrefix(auth, prefix) {
		return ctx, status.Error(codes.Unauthenticated,
			`missing "Basic " prefix in "Authorization" header`)
	}

	c, err := base64.StdEncoding.DecodeString(auth[len(prefix):])
	if err != nil {
		return ctx, status.Error(codes.Unauthenticated,
			`invalid base64 in header`)
	}

	cs := string(c)
	s := strings.IndexByte(cs, ':')
	if s < 0 {
		return ctx, status.Error(codes.Unauthenticated,
			`invalid basic auth format`)
	}
	user, passwd := cs[:s], cs[s+1:]
	creds, err := cbauth.Auth(user, passwd)
	if err != nil {
		return nil, err
	}

	var authFunc gRPCAuthHandler
	aw := &authWrapper{mgr: srv.mgr, creds: creds,
		path: rpcPath[strings.LastIndex(rpcPath, "/"):], method: "RPC"}
	authFunc = aw.authenticate

	nctx := context.WithValue(ctx, gRPCAuthHandlerKey, authFunc)
	return nctx, nil
}

func extractMetaHeader(ctx context.Context, header string) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", fmt.Errorf("no headers in request")
	}

	headerValue, ok := md[header]
	if !ok {
		return "", fmt.Errorf("no headers in request")
	}

	if len(headerValue) != 1 {
		return "", fmt.Errorf("more than 1 header in request")
	}

	return headerValue[0], nil
}

type authWrapper struct {
	mgr    *cbgt.Manager
	path   string
	method string
	creds  cbauth.Creds
}

func (a *authWrapper) authenticate(r requestParser) (bool, error) {
	var authType string
	if a.mgr != nil && a.mgr.Options() != nil {
		authType = a.mgr.Options()["authType"]
	}

	if authType == "" {
		return true, nil
	}

	if authType != "cbauth" {
		return false, nil
	}

	perms, err := preparePerms(a.mgr, r, a.method, a.path)
	if err != nil {
		return false, fmt.Errorf("grpc_auth: preparePerms err: %v", err)
	}

	if len(perms) <= 0 {
		return true, nil
	}

	for _, perm := range perms {
		allowed, err := CBAuthIsAllowed(a.creds, perm)
		if err != nil {
			return false, err
		}

		if !allowed {
			return false, err
		}
	}
	return true, nil
}

// rpcRequestParser implements the requestParser interface
type rpcRequestParser struct {
	indexName   string
	requestType string
	request     interface{}
}

func (rp *rpcRequestParser) GetIndexName() (string, error) {
	return rp.indexName, nil
}

func (rp *rpcRequestParser) GetIndexDef() (*cbgt.IndexDef, error) {
	return nil, nil // TODO when DDLs are supported over RPCs
}

func (rp *rpcRequestParser) GetCollectionNames() ([]string, error) {
	return nil, nil // placeholder implementation.
}

func (rp *rpcRequestParser) GetBucketName() (string, error) {
	return "", nil // placeholder implementation.
}

func (rp *rpcRequestParser) GetRequest() (interface{}, string) {
	return rp.request, "RPC"
}

func (rp *rpcRequestParser) GetPIndexName() (string, error) {
	// TODO - placeholder implementation, improve this as more
	// and more pindex based RPCs are introduced.
	if r, ok := rp.request.(*pb.SearchRequest); ok {
		if r.QueryPIndexes != nil {
			queryPIndexes := QueryPIndexes{}
			err := UnmarshalJSON(r.QueryPIndexes, &queryPIndexes)
			if err != nil {
				return "", fmt.Errorf("missing pindexName, err: %v", err)
			}

			if len(queryPIndexes.PIndexNames) > 0 {
				return queryPIndexes.PIndexNames[0], nil
			}
		}
	}
	return "", fmt.Errorf("missing pindexName")
}

func verifyRPCAuth(ctx context.Context, indexName string, req interface{}) error {
	if _, err := extractMetaHeader(ctx, rpcClusterActionKey); err == nil {
		return nil
	}

	var authHandler gRPCAuthHandler
	if aw := ctx.Value(gRPCAuthHandlerKey); aw != nil {
		authHandler = aw.(gRPCAuthHandler)
	}

	if authHandler == nil {
		return fmt.Errorf("grpc_auth: invalid authHandler")
	}

	v, err := authHandler(
		&rpcRequestParser{indexName: indexName,
			request: req})
	if err != nil {
		return fmt.Errorf("grpc_auth: auth err: %v", err)
	}
	if !v {
		return fmt.Errorf("grpc_auth: permission denied")
	}

	return nil
}

//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/gocb/v2"
)

// NOTE: these authenticator implementations are based on the gocb.Authenticator interface,
// and need a separate implementation since the gocbcore auth APIs in
// cbgt/gocbcore_utils.go are not compatible with the gocb.Authenticator interface.
type AuthParams struct {
	AuthUser     string `json:"authUser"`
	AuthPassword string `json:"authPassword"`

	AuthSaslUser     string `json:"authSaslUser"`
	AuthSaslPassword string `json:"authSaslPassword"`

	ClientCertPath string `json:"clientCertPath"`
	ClientKeyPath  string `json:"clientKeyPath"`
}

func (a *AuthParams) Credentials(req gocb.AuthCredsRequest) (
	[]gocb.UserPassPair, error) {
	return []gocb.UserPassPair{{
		Username: a.AuthUser,
		Password: a.AuthPassword,
	}}, nil
}

func (a *AuthParams) Certificate(req gocb.AuthCertRequest) (
	*tls.Certificate, error) {
	return nil, nil
}

func (a *AuthParams) SupportsTLS() bool {
	return true
}

func (a *AuthParams) SupportsNonTLS() bool {
	return true
}

func (a *AuthParams) JWT(req gocb.AuthCredsRequest) (gocb.JWT, error) {
	return "", nil
}

// ref: https://github.com/couchbase/gocb/blob/72079c78a8a34dd6a98cdc75a3e1c09796d2013b/auth.go#L92
func (a *AuthParams) DefaultSaslMechanisms(tlsEnabled bool) []gocb.SaslMechanism {
	if tlsEnabled {
		return []gocb.SaslMechanism{gocb.ScramSha512SaslMechanism, gocb.ScramSha256SaslMechanism, gocb.ScramSha1SaslMechanism}
	}
	return []gocb.SaslMechanism{gocb.PlainSaslMechanism}
}

type AuthParamsSasl struct {
	AuthParams
}

func (a *AuthParamsSasl) Credentials(req gocb.AuthCredsRequest) (
	[]gocb.UserPassPair, error) {
	return []gocb.UserPassPair{{
		Username: a.AuthSaslUser,
		Password: a.AuthSaslPassword,
	}}, nil
}

func (a *AuthParamsSasl) Certificate(req gocb.AuthCertRequest) (
	*tls.Certificate, error) {
	return nil, nil
}

func (a *AuthParamsSasl) SupportsTLS() bool {
	return true
}

func (a *AuthParamsSasl) SupportsNonTLS() bool {
	return true
}

func (a *AuthParamsSasl) JWT(req gocb.AuthCredsRequest) (gocb.JWT, error) {
	return "", nil
}

func (a *AuthParamsSasl) DefaultSaslMechanisms(tlsEnabled bool) []gocb.SaslMechanism {
	if tlsEnabled {
		return []gocb.SaslMechanism{gocb.ScramSha512SaslMechanism, gocb.ScramSha256SaslMechanism, gocb.ScramSha1SaslMechanism}
	}
	return []gocb.SaslMechanism{gocb.PlainSaslMechanism}
}

type AuthParamsCert struct {
	AuthParams
}

func (a *AuthParamsCert) Credentials(req gocb.AuthCredsRequest) (
	[]gocb.UserPassPair, error) {
	return []gocb.UserPassPair{{}}, nil
}

func (a *AuthParamsCert) Certificate(req gocb.AuthCertRequest) (
	*tls.Certificate, error) {
	if len(a.ClientCertPath) > 0 && len(a.ClientKeyPath) > 0 {
		cert, err := tls.LoadX509KeyPair(a.ClientCertPath, a.ClientKeyPath)
		if err != nil {
			return nil, err
		}

		return &cert, nil
	}

	return nil, fmt.Errorf("AuthParamsCert: certPath/keyPath unavailable")
}

func (a *AuthParamsCert) SupportsTLS() bool {
	return true
}

func (a *AuthParamsCert) SupportsNonTLS() bool {
	return true
}

func (a *AuthParamsCert) JWT(req gocb.AuthCredsRequest) (gocb.JWT, error) {
	return "", nil
}

// based on https://github.com/couchbase/gocb/blob/72079c78a8a34dd6a98cdc75a3e1c09796d2013b/auth.go#L101
func (a *AuthParamsCert) DefaultSaslMechanisms(tlsEnabled bool) []gocb.SaslMechanism {
	return []gocb.SaslMechanism{}
}

type CBAuthenticator struct{}

func (a *CBAuthenticator) Credentials(req gocb.AuthCredsRequest) (
	[]gocb.UserPassPair, error) {
	endpoint := req.Endpoint

	// get rid of the http:// or https:// prefix from the endpoint
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	username, password, err := cbauth.GetMemcachedServiceAuth(endpoint)
	if err != nil {
		return []gocb.UserPassPair{{}}, err
	}

	return []gocb.UserPassPair{{
		Username: username,
		Password: password,
	}}, nil
}

func (a *CBAuthenticator) Certificate(req gocb.AuthCertRequest) (
	*tls.Certificate, error) {
	ss := cbgt.GetSecuritySetting()
	if ss.ShouldClientsUseClientCert {
		return &ss.ClientCertificate, nil
	}

	return nil, nil
}

func (a *CBAuthenticator) SupportsTLS() bool {
	return true
}

func (a *CBAuthenticator) SupportsNonTLS() bool {
	return true
}

func (a *CBAuthenticator) JWT(req gocb.AuthCredsRequest) (gocb.JWT, error) {
	return "", nil
}

func (a *CBAuthenticator) DefaultSaslMechanisms(tlsEnabled bool) []gocb.SaslMechanism {
	if tlsEnabled {
		return []gocb.SaslMechanism{gocb.ScramSha512SaslMechanism, gocb.ScramSha256SaslMechanism, gocb.ScramSha1SaslMechanism}
	}
	return []gocb.SaslMechanism{gocb.PlainSaslMechanism}
}

func gocbAuth(sourceParams string, authType string) (
	auth gocb.Authenticator, err error) {
	params := &AuthParams{}

	if sourceParams != "" {
		err := UnmarshalJSON([]byte(sourceParams), params)
		if err != nil {
			return nil, fmt.Errorf("gocbcore_utils: gocbAuth" +
				" failed to parse sourceParams JSON to CBAuthParams")
		}
	}

	auth = params

	if params.AuthSaslUser != "" {
		auth = &AuthParamsSasl{*params}
	}

	if len(params.ClientCertPath) > 0 && len(params.ClientKeyPath) > 0 {
		auth = &AuthParamsCert{*params}
	}

	if authType == "cbauth" {
		auth = &CBAuthenticator{}
	}
	return auth, nil
}

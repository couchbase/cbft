//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"sync"
	"syscall"

	log "github.com/couchbase/clog"
	"google.golang.org/grpc/credentials"
)

const PrivacyAndIntegrity credentials.SecurityLevel = 3

// tlsCreds is the credentials required for authenticating a connection using TLS.
// This is a custom implementation of crendentials.TransportCredentials with the
// capability to switch the config and an additional lock used while switching
type tlsCreds struct {
	// TLS configuration
	config     *tls.Config
	configLock *sync.RWMutex
}

func (c tlsCreds) Info() credentials.ProtocolInfo {
	c.configLock.RLock()
	defer c.configLock.Unlock()
	return credentials.ProtocolInfo{
		SecurityProtocol: "tls",
		SecurityVersion:  "1.2",
		ServerName:       c.config.ServerName,
	}
}

func (c *tlsCreds) ClientHandshake(ctx context.Context, authority string,
	rawConn net.Conn) (_ net.Conn, _ credentials.AuthInfo, err error) {
	// use local cfg to avoid clobbering ServerName if using multiple endpoints
	cfg := c.GetConfig()
	if cfg.ServerName == "" {
		serverName, _, err := net.SplitHostPort(authority)
		if err != nil {
			// If the authority had no host port or if the authority cannot
			// be parsed, use it as-is.
			serverName = authority
		}
		cfg.ServerName = serverName
	}
	conn := tls.Client(rawConn, cfg)
	errChannel := make(chan error, 1)
	go func() {
		errChannel <- conn.Handshake()
		close(errChannel)
	}()
	select {
	case err := <-errChannel:
		if err != nil {
			conn.Close()
			return nil, nil, err
		}
	case <-ctx.Done():
		conn.Close()
		return nil, nil, ctx.Err()
	}
	tlsInfo := credentials.TLSInfo{
		State: conn.ConnectionState(),
		CommonAuthInfo: credentials.CommonAuthInfo{
			SecurityLevel: PrivacyAndIntegrity,
		},
	}
	id := sPIFFEIDFromState(conn.ConnectionState())
	if id != nil {
		tlsInfo.SPIFFEID = id
	}
	return wrapSyscallConn(rawConn, conn), tlsInfo, nil
}

func (c *tlsCreds) ServerHandshake(rawConn net.Conn) (
	net.Conn, credentials.AuthInfo, error) {
	conn := tls.Server(rawConn, c.GetConfig())
	if err := conn.Handshake(); err != nil {
		conn.Close()
		return nil, nil, err
	}
	tlsInfo := credentials.TLSInfo{
		State: conn.ConnectionState(),
		CommonAuthInfo: credentials.CommonAuthInfo{
			SecurityLevel: PrivacyAndIntegrity,
		},
	}
	id := sPIFFEIDFromState(conn.ConnectionState())
	if id != nil {
		tlsInfo.SPIFFEID = id
	}
	return wrapSyscallConn(rawConn, conn), tlsInfo, nil
}

func (c *tlsCreds) Clone() credentials.TransportCredentials {
	return credentials.NewTLS(c.GetConfig())
}

func (c *tlsCreds) OverrideServerName(serverNameOverride string) error {
	c.configLock.Lock()
	c.config.ServerName = serverNameOverride
	c.configLock.Unlock()
	return nil
}

func (c *tlsCreds) GetConfig() *tls.Config {
	c.configLock.RLock()
	rv := cloneTLSConfig(c.config)
	c.configLock.RUnlock()
	return rv
}

func (c *tlsCreds) SetConfig(config *tls.Config) {
	c.configLock.Lock()
	c.config = config
	c.configLock.Unlock()
}

// SPIFFEIDFromState parses the SPIFFE ID from State. If the SPIFFE ID format
// is invalid, return nil with warning.
func sPIFFEIDFromState(state tls.ConnectionState) *url.URL {
	if len(state.PeerCertificates) == 0 || len(state.PeerCertificates[0].URIs) == 0 {
		return nil
	}
	return sPIFFEIDFromCert(state.PeerCertificates[0])
}

// SPIFFEIDFromCert parses the SPIFFE ID from x509.Certificate. If the SPIFFE
// ID format is invalid, return nil with warning.
func sPIFFEIDFromCert(cert *x509.Certificate) *url.URL {
	if cert == nil || cert.URIs == nil {
		return nil
	}
	var spiffeID *url.URL
	for _, uri := range cert.URIs {
		if uri == nil || uri.Scheme != "spiffe" || uri.Opaque != "" ||
			(uri.User != nil && uri.User.Username() != "") {
			continue
		}
		// From this point, we assume the uri is intended for a SPIFFE ID.
		if len(uri.String()) > 2048 {
			log.Warnf("invalid SPIFFE ID: total ID length larger than 2048 bytes")
			return nil
		}
		if len(uri.Host) == 0 || len(uri.Path) == 0 {
			log.Warnf("invalid SPIFFE ID: domain or workload ID is empty")
			return nil
		}
		if len(uri.Host) > 255 {
			log.Warnf("invalid SPIFFE ID: domain length larger than 255 characters")
			return nil
		}
		// A valid SPIFFE certificate can only have exactly one URI SAN field.
		if len(cert.URIs) > 1 {
			log.Warnf("invalid SPIFFE ID: multiple URI SANs")
			return nil
		}
		spiffeID = uri
	}
	return spiffeID
}

type sysConn = syscall.Conn

// syscallConn keeps reference of rawConn to support syscall.Conn for channelz.
// SyscallConn() (the method in interface syscall.Conn) is explicitly
// implemented on this type,
//
// Interface syscall.Conn is implemented by most net.Conn implementations (e.g.
// TCPConn, UnixConn), but is not part of net.Conn interface. So wrapper conns
// that embed net.Conn don't implement syscall.Conn. (Side note: tls.Conn
// doesn't embed net.Conn, so even if syscall.Conn is part of net.Conn, it won't
// help here).
type syscallConn struct {
	net.Conn
	// sysConn is a type alias of syscall.Conn. It's necessary because the name
	// `Conn` collides with `net.Conn`.
	sysConn
}

// WrapSyscallConn tries to wrap rawConn and newConn into a net.Conn that
// implements syscall.Conn. rawConn will be used to support syscall, and newConn
// will be used for read/write.
//
// This function returns newConn if rawConn doesn't implement syscall.Conn.
func wrapSyscallConn(rawConn, newConn net.Conn) net.Conn {
	sysConn, ok := rawConn.(syscall.Conn)
	if !ok {
		return newConn
	}
	return &syscallConn{
		Conn:    newConn,
		sysConn: sysConn,
	}
}

const alpnProtoStrH2 = "h2"

// AppendH2ToNextProtos appends h2 to next protos.
func appendH2ToNextProtos(ps []string) []string {
	for _, p := range ps {
		if p == alpnProtoStrH2 {
			return ps
		}
	}
	ret := make([]string, 0, len(ps)+1)
	ret = append(ret, ps...)
	return append(ret, alpnProtoStrH2)
}

// NewTLS uses c to construct a TransportCredentials based on TLS.
func NewTransportCredentials(c *tls.Config) *tlsCreds {
	tc := &tlsCreds{
		config:     cloneTLSConfig(c),
		configLock: &sync.RWMutex{},
	}
	tc.config.NextProtos = appendH2ToNextProtos(tc.config.NextProtos)
	return tc
}

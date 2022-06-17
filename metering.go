//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build enterprise
// +build enterprise

package cbft

import (
	"github.com/couchbase/cbauth/service"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/regulator"
	"github.com/couchbase/regulator/factory"
	"net/http"
)

func MeteringEndpointHandler(mgr *cbgt.Manager) (string, http.Handler) {
	return regulator.MeteringEndpoint, NewMeteringHandler(mgr)
}

type message struct {
	user      string
	bucket    string
	bytes     uint64
	close     bool
	operation uint
}

type serviceRegulator struct {
	regInstance regulator.Regulator
	mgr         *cbgt.Manager
	messageChan chan *message
	prevWBytes  uint64
	prevRBytes  uint64
}

var srvReg *serviceRegulator

func NewMeteringHandler(mgr *cbgt.Manager) http.Handler {
	regOps := regulator.InitSettings{
		NodeID:    service.NodeID(mgr.UUID()),
		Service:   regulator.Search,
		TlsCAFile: cbgt.TLSCAFile,
	}

	regHandler := factory.InitRegulator(regOps)
	srvReg = &serviceRegulator{
		regInstance: regulator.Instance,
		mgr:         mgr,
		messageChan: make(chan *message, 10),
	}

	return regHandler
}

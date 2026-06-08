//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"net/http"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/rest"
)

// GetKeysInUseHandler is a REST handler that
// retrieves the keys in use for encryption.
type GetKeysInUseHandler struct {
	mgr *cbgt.Manager
}

func NewGetKeysInUseHandler(mgr *cbgt.Manager) *GetKeysInUseHandler {
	return &GetKeysInUseHandler{
		mgr: mgr,
	}
}

func (h *GetKeysInUseHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	keysInUse, err := encryptionManagerInstance.getAllKeysInUse()
	if err != nil {
		rest.ShowError(w, req, "getKeysInUse: failed to retrieve keys in use for encryption: "+err.Error(),
			http.StatusInternalServerError)
		return
	}

	rest.MustEncode(w, keysInUse)
}

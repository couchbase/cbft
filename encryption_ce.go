//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !enterprise
// +build !enterprise

package cbft

import (
	"fmt"
	"os"
	"sync"

	"github.com/couchbase/cbgt"
)

var encryptionManagerInstance *encryptionManager
var encryptionManagerOnce sync.Once

type encryptionManager struct {
}

func NewEncryptionManager(mgr *cbgt.Manager) (*encryptionManager, error) {
	encryptionManagerOnce.Do(func() {
		encryptionManagerInstance = &encryptionManager{}
	})

	return encryptionManagerInstance, nil
}

func (em *encryptionManager) encryptAndWriteFile(path string, data []byte, perm os.FileMode) (string, error) {
	err := os.WriteFile(path, data, perm)
	if err != nil {
		return "", fmt.Errorf(
			"encryptionManager: failed to write file with encryption: %w", err)
	}

	return "", nil
}

func (em *encryptionManager) decryptAndReadFile(path string) ([]byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf(
			"encryptionManager: failed to read file with encryption: %w", err)
	}

	return data, "", nil
}

func (em *encryptionManager) importEncryptionKeys(_ string) error {
	return nil
}

func (em *encryptionManager) prepEncryptionKeys(_, _ string) error {
	return nil
}

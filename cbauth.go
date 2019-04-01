//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cbft

import (
	"crypto/tls"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	log "github.com/couchbase/clog"
)

var TLSCertFile string
var TLSKeyFile string

type SecuritySetting struct {
	EncryptionEnabled  bool
	DisableNonSSLPorts bool // place holder for future, not used yet
	Certificate        *tls.Certificate
	CertInBytes        []byte
	TLSConfig          *cbauth.TLSConfig
	ClientAuthType     *tls.ClientAuthType
}

var currentSetting unsafe.Pointer = unsafe.Pointer(new(SecuritySetting))

func GetSecuritySetting() *SecuritySetting {
	return (*SecuritySetting)(atomic.LoadPointer(&currentSetting))
}

func updateSecuritySetting(c *SecuritySetting) {
	atomic.StorePointer(&currentSetting, unsafe.Pointer(c))
}

var securityCtx *SecurityContext

func init() {
	securityCtx = &SecurityContext{
		notifiers: make(map[string]configChangeNotifier),
	}

	cbauth.RegisterConfigRefreshCallback(securityCtx.refresh)
}

// SecurityContext let us register multiple tls config
// update callbacks and acts as a wrapper for handling
// config changes.
type SecurityContext struct {
	mutex     sync.RWMutex
	notifiers map[string]configChangeNotifier
}

// config change notifier
type configChangeNotifier func() error

func RegisterConfigRefreshCallback(key string, cb configChangeNotifier) {
	securityCtx.mutex.Lock()
	securityCtx.notifiers[key] = cb
	securityCtx.mutex.Unlock()
	log.Printf("cbauth: key: %s registered for tls config updates", key)
}

func (c *SecurityContext) refresh(code uint64) error {
	log.Printf("cbauth: received  security change notification, code: %v", code)

	newSetting := &SecuritySetting{}
	hasEnabled := false

	oldSetting := GetSecuritySetting()
	if oldSetting != nil {
		temp := *oldSetting
		newSetting = &temp
		hasEnabled = oldSetting.EncryptionEnabled
	}

	if code&cbauth.CFG_CHANGE_CERTS_TLSCONFIG != 0 {
		if err := c.refreshConfig(newSetting); err != nil {
			return err
		}

		if err := c.refreshCert(newSetting); err != nil {
			return err
		}
	}

	if code&cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION != 0 {
		if err := c.refreshEncryption(newSetting); err != nil {
			return err
		}
	}

	updateSecuritySetting(newSetting)

	c.mutex.RLock()
	if hasEnabled || hasEnabled != newSetting.EncryptionEnabled {
		for key, notifier := range c.notifiers {
			log.Printf("cbauth: notifying configs change for key: %v", key)
			if err := notifier(); err != nil {
				log.Printf("cbauth: notify failed, for key: %v: err: %v", key, err)
			}
		}
	} else {
		log.Printf("cbauth: encryption not enabled")
	}
	c.mutex.RUnlock()

	return nil
}

func (c *SecurityContext) refreshConfig(configs *SecuritySetting) error {
	TLSConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		log.Printf("cbauth: GetTLSConfig failed, err: %v", err)
		return err
	}

	ClientAuthType, err := cbauth.GetClientCertAuthType()
	if err != nil {
		log.Printf("cbauth: GetClientCertAuthType failed, err: %v", err)
		return err
	}

	configs.TLSConfig = &TLSConfig
	configs.ClientAuthType = &ClientAuthType

	return nil
}

func (c *SecurityContext) refreshCert(configs *SecuritySetting) error {
	if len(TLSCertFile) == 0 || len(TLSKeyFile) == 0 {
		return nil
	}

	cert, err := tls.LoadX509KeyPair(TLSCertFile, TLSKeyFile)
	if err != nil {
		log.Printf("cbauth: LoadX509KeyPair err : %v", err)
		return err
	}

	certInBytes, err := ioutil.ReadFile(TLSCertFile)
	if err != nil {
		log.Printf("cbauth: Certificates read err: %v", err)
		return err
	}

	configs.Certificate = &cert
	configs.CertInBytes = certInBytes

	// update http2client that with new certificate
	setupHttp2Client(certInBytes)

	return nil
}

func (c *SecurityContext) refreshEncryption(configs *SecuritySetting) error {
	cfg, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		log.Printf("cbauth: GetClusterEncryptionConfig err: %v", err)
		return err
	}

	configs.EncryptionEnabled = cfg.EncryptData
	configs.DisableNonSSLPorts = cfg.DisableNonSSLPorts

	return nil
}

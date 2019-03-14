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

package main

import (
	"crypto/tls"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/couchbase/cbauth"
	log "github.com/couchbase/clog"
)

type tlsConfigs struct {
	encryptionEnabled bool
	disableNonSSLPort bool
	certificate       *tls.Certificate
	certInBytes       []byte
	tlsPreference     *cbauth.TLSConfig
	refreshType       uint64
}

var tlsConfig unsafe.Pointer = unsafe.Pointer(new(tlsConfigs))

func getTLSConfigs() *tlsConfigs {
	return (*tlsConfigs)(atomic.LoadPointer(&tlsConfig))
}

func updateTLSConfigs(c *tlsConfigs) {
	atomic.StorePointer(&tlsConfig, unsafe.Pointer(c))
}

func encryptionEnabled() bool {
	configs := getTLSConfigs()
	if configs == nil {
		return false
	}
	return configs.encryptionEnabled
}

func disableNonSSLPort() bool {
	configs := getTLSConfigs()
	if configs == nil {
		return false
	}
	return configs.disableNonSSLPort
}

var tlsConfigHandler *cbauthCallbackWrapper

func init() {
	tlsConfigHandler = &cbauthCallbackWrapper{
		notifiers: make(map[string]configChangeNotifier),
	}

	cbauth.RegisterConfigRefreshCallback(tlsConfigHandler.refresh)
}

// cbauthCallbackWrapper let us register multiple tls config
// update callbacks and acts as a wrapper for handling
// config changes.
type cbauthCallbackWrapper struct {
	mutex     sync.RWMutex
	notifiers map[string]configChangeNotifier
}

// config change notifier
type configChangeNotifier func() error

func registerTLSRefreshCallback(key string, cb configChangeNotifier) {
	tlsConfigHandler.mutex.Lock()
	tlsConfigHandler.notifiers[key] = cb
	tlsConfigHandler.mutex.Unlock()
	log.Printf("init_cbauth: key: %s registered for tls config updates", key)
}

func (c *cbauthCallbackWrapper) refresh(value uint64) error {
	newConfigs := &tlsConfigs{}

	oldConfigs := getTLSConfigs()
	if oldConfigs != nil {
		temp := *oldConfigs
		newConfigs = &temp
	}

	if value == cbauth.CFG_CHANGE_CERTS_TLSCONFIG {
		if err := c.refreshConfig(newConfigs); err != nil {
			return err
		}

		if err := c.refreshCert(newConfigs); err != nil {
			return err
		}
	}

	if value == cbauth.CFG_CHANGE_CLUSTER_ENCRYPTION {
		if err := c.refreshEncryption(newConfigs); err != nil {
			return err
		}
	}

	newConfigs.refreshType = value
	updateTLSConfigs(newConfigs)

	c.mutex.RLock()
	for key, notifier := range c.notifiers {
		log.Printf("init_cbauth: notifying configs change for key: %v", key)
		if err := notifier(); err != nil {
			log.Printf("init_cbauth: notify failed, "+
				"for key: %v: err: %v", key, err)
		}
	}
	c.mutex.RUnlock()

	return nil
}

func (c *cbauthCallbackWrapper) refreshConfig(configs *tlsConfigs) error {
	newConfig, err := cbauth.GetTLSConfig()
	if err != nil {
		log.Printf("init_cbauth: refreshConfig failed, error: %v", err)
		return err
	}

	configs.tlsPreference = &newConfig

	return nil
}

func (c *cbauthCallbackWrapper) refreshCert(configs *tlsConfigs) error {
	cert, err := tls.LoadX509KeyPair(flags.TLSCertFile, flags.TLSKeyFile)
	if err != nil {
		log.Printf("init_cbauth: LoadX509KeyPair err : %v", err)
		return err
	}

	certInBytes, err := ioutil.ReadFile(flags.TLSCertFile)
	if err != nil {
		log.Printf("init_cbauth: certificates read err: %v", err)
		return err
	}

	configs.certificate = &cert
	configs.certInBytes = certInBytes

	return nil
}

func (c *cbauthCallbackWrapper) refreshEncryption(configs *tlsConfigs) error {
	cfg, err := cbauth.GetClusterEncryptionConfig()
	if err != nil {
		log.Printf("init_cbauth: GetClusterEncryptionConfig err: %v", err)
		return err
	}

	configs.encryptionEnabled = cfg.EncryptData
	configs.disableNonSSLPort = cfg.DisableNonSSLPorts

	return nil
}

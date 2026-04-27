//  Copyright 2026-Present Couchbase, Inc.
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
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/v2"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/couchbase/cbauth"
	"github.com/couchbase/cbauth/cbauthimpl"
	"github.com/couchbase/cbgt"
	log "github.com/couchbase/clog"
	gocbcrypto "github.com/couchbase/gocbcrypto"
)

// ------------------------- encryption manager implementation -----------------------

var encryptionManagerInstance *encryptionManager
var encryptionManagerOnce sync.Once

// This is responsible for all encryption related operations.
// Managing encryption, decryption, key management,
// bleve callbacks, cbauth callbacks and bucket updates related to encryption.
type encryptionManager struct {
	mgr *cbgt.Manager

	// key store is responsible for managing encryption keys
	// and caching keys in use for different buckets and key types
	keyStore *keyStore

	// bucket store is responsible for managing bucket name to UUID mappings
	// and caching the mappings for quick retrieval
	bucketStore *bucketStore
}

// unique context used for deriving encryption keys for search service
var searchEncryptionContext = []byte("search")

const (
	// key types tracked by search's encryption manager
	bucketKeyType = "service_bucket"
	otherKeyType  = "other"
	// cipher key string
	aes256gcmCipher = "AES-256-GCM"
	// nonce length for AES-256-GCM encryption
	noncelength = 12
	// timeout for getting encryption keys from cbauth
	getKeyTimeout = 2 * time.Minute
	// timeout for key import by cbauth during rebalance
	importKeyTimeout = int(2 * time.Minute)
	// number of parallel workers for drop keys operation
	numParallelDrops = 4
)

// NewEncryptionManager creates a new instance of encryption manager and registers
// the necessary callbacks with cbauth and cbgt manager. It returns an error if
// there is an issue with registering the callbacks or creating the manager.
func NewEncryptionManager(mgr *cbgt.Manager) (*encryptionManager, error) {
	if mgr == nil {
		return nil, fmt.Errorf("encryptionManager: manager cannot be nil")
	}

	encryptionManagerOnce.Do(func() {
		encryptionManagerInstance = &encryptionManager{
			keyStore:    newKeyStore(mgr),
			bucketStore: newBucketStore(mgr),
			mgr:         mgr,
		}
	})

	// register callbacks with cbauths for key functionality
	err := cbauth.RegisterEncryptionKeysCallbacks(
		encryptionManagerInstance.refreshKeys,
		encryptionManagerInstance.getInUseKeys,
		encryptionManagerInstance.dropKeys,
		encryptionManagerInstance.synchroniseKeys,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register encryption keys callbacks: %w",
			err)
	}

	// register callbacks with bleve for readers and writers
	index.WriterHook = encryptionManagerInstance.bleveWriterHook
	index.ReaderHook = encryptionManagerInstance.bleveReaderHook

	// register callbacks with cbgt manager for bucket updates and file encryption/decryption
	mgr.SetBucketUpdateCallback(
		encryptionManagerInstance.bucketUpdateCallback)
	mgr.SetEncryptAndWriteFileCallback(
		encryptionManagerInstance.encryptAndWriteFile)
	mgr.SetDecryptAndReadFileCallback(
		encryptionManagerInstance.decryptAndReadFile)
	log.Print("encryptionManager: encryption manager created and callbacks " +
		"registered successfully")

	return encryptionManagerInstance, nil
}

// ---------------------------- callbacks implementation --------------------------

// updates bucket cache when there is an index introduction/deletion,
// which is the only time we need to check bucket UUID changes
func (em *encryptionManager) bucketUpdateCallback() {
	em.bucketStore.refreshBucketUUIDMap()
}

// The callback is called in a separate go-routine (not protected by mutex)
// when there is a change in encryption keys. Returns nil if keys are ok, or
// "error" if any of the mandatory keys is missing and the service can't
// continue working. The callback must not take longer than a few seconds.
func (em *encryptionManager) refreshKeys(keyData cbauth.KeyDataType) error {
	log.Printf("encryptionManager: refreshKeys callback called for key data %v",
		keyData)

	keysInUse, err := em.keyStore.getKeysInUse(keyData.TypeName, keyData.BucketUUID)
	if err != nil {
		// log and continue operation when keys in use fails
		// failure is likely due to booting up
		log.Printf("encryptionManager: failed to get keys in use for key data "+
			"%v: %v", keyData, err)
	}

	newKeyInfo, err := em.getEncryptionKeys(keyData)
	if err != nil {
		return err
	}

	newKeysMap := make(map[string]struct{})
	for _, key := range newKeyInfo.Keys {
		newKeysMap[key.Id] = struct{}{}
	}

	// compare all of the keys in use with the new keys. If any of the keys in use is not
	// present in the new keys, return an error as we cannot guarantee continued
	// operation without the key
	for keyId := range keysInUse {
		if _, exists := newKeysMap[keyId]; !exists {
			return fmt.Errorf("encryptionManager: key with ID %s is still in "+
				"use but not present in new keys for key data %v", keyId, keyData)
		}
	}

	// update the key store with the new keys
	em.keyStore.updateMasterKey(keyData.BucketUUID, keyData.TypeName, newKeyInfo)

	log.Printf("encryptionManager: refreshKeys completed successfully for key data %v",
		keyData)
	return nil
}

// The callback is called in a separate go-routine (not protected by mutex) when
// Cluster Manager performs garbage collection for unused keys. The goal is to
// determine which keys are actually used by the service. Returns list of key
// ids that are being used. The callback must return quickly. The service
// should cache the list of keys in use if its calculation requires significant
// time.
// The service must include the empty key (empty key id) in the returned list if
// and only if there is unencrypted data on disk.
func (em *encryptionManager) getInUseKeys(keyData cbauth.KeyDataType) (
	[]string, error) {
	// get all of the keys in use from the cache
	keysInUse, err := em.keyStore.getKeysInUse(keyData.TypeName, keyData.BucketUUID)
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to get keys in "+
			"use for key data %v: %w", keyData, err)
	}

	// append the active key for the bucket if it's not already present in the
	// keys obtained
	keyInfo, err := em.keyStore.getKeyInfo(keyData.TypeName, keyData.BucketUUID)
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to get active key for "+
			"key data %v: %w", keyData, err)
	}

	if _, ok := keysInUse[keyInfo.ActiveKeyId]; !ok {
		keysInUse[keyInfo.ActiveKeyId] = struct{}{}
	}

	keys := make([]string, 0, len(keysInUse))
	for keyId := range keysInUse {
		keys = append(keys, keyId)
	}

	return keys, nil
}

// The callback is called in a separate go-routine (not protected by mutex) when
// Cluster Manager needs to get rid of one or more encryption keys. This means
// that the service should find the data that is encrypted by the specified keys,
// and re-encrypt that data with the current active key. The callback must not
// block. The service should initiate the process of re-encryption and return
// immediately. When re-encryption is finished, the service should call
// KeysDropComplete(DataType, Res).
// KeyIdsToDrop may include the empty key (empty key id), which should be treated
// as an ask to encrypt the data on disk that is not encrypted yet.
func (em *encryptionManager) dropKeys(keyData cbauth.KeyDataType,
	KeyIdsToDrop []string) {
	log.Printf("encryptionManager: dropKeys callback called for key data %v "+
		"and keys to drop %v", keyData, KeyIdsToDrop)

	if em.mgr != nil && em.mgr.BootingDone() {
		go em.dropKeysAsync(keyData, KeyIdsToDrop)
	} else {
		// should not be hit considering that encryption keys should be loaded
		// before cbauth issues a drop keys callback, but adding a safeguard
		// just in case to avoid potential issues.
		go func() {
			log.Printf("encryptionManager: manager is not ready for drop keys request, "+
				"waiting for boot to complete. Key data: %v, keys to drop: %v",
				keyData, KeyIdsToDrop)
			em.keyStore.waitPreLoad()
			log.Printf("encryptionManager: boot completed, proceeding with drop"+
				" keys for key data %v and keys to drop %v", keyData, KeyIdsToDrop)
			em.dropKeysAsync(keyData, KeyIdsToDrop)
		}()
	}
}

func (em *encryptionManager) dropKeysAsync(keyData cbauth.KeyDataType,
	KeyIdsToDrop []string) {
	_, pIndexes := em.mgr.CurrentMaps()
	keysToDropMap := make(map[string]struct{})
	for _, keyId := range KeyIdsToDrop {
		keysToDropMap[keyId] = struct{}{}
	}
	// initialize channels for jobs and errors
	jobs := make(chan *cbgt.PIndex, len(pIndexes))
	errs := make(chan error, len(pIndexes))
	wg := sync.WaitGroup{}

	// traverse through all planPIndexes and re-encrypt the ones encrypted with the keys to drop
	if keyData.TypeName == otherKeyType {
		wg.Add(1)
		go em.dropPlanPIndexes(keysToDropMap, &wg, errs)
	} else {
		for _ = range numParallelDrops {
			go em.dropWorker(jobs, keyData, keysToDropMap, &wg, errs)
		}

		// traverse through all pindexes and re-encrypt the ones
		// encrypted with the keys to drop.
		for _, pIndex := range pIndexes {
			wg.Add(1)
			jobs <- pIndex
		}
	}

	// blockingly wait for all re-encryption go-routines to finish and
	// aggregate errors if any before calling KeysDropComplete
	go func() {
		wg.Wait()
		// close channels so workers can exit
		close(jobs)
		close(errs)
		errMsg := ""
		for err := range errs {
			if err != nil {
				errMsg += err.Error() + "; "
			}
		}
		if errMsg != "" {
			log.Printf("encryptionManager: errors occurred during key drop: %s",
				errMsg)
			cbauth.KeysDropComplete(keyData,
				fmt.Errorf("encryptionManager: errors occurred during "+
					"key drop: %s", errMsg))
		} else {
			log.Printf("encryptionManager: keys drop completed successfully "+
				"for key data - %+v and key IDs to drop - %v",
				keyData, KeyIdsToDrop)
			cbauth.KeysDropComplete(keyData, nil)
		}
		em.bucketStore.refreshBucketUUIDMap()
	}()

	return
}

func (em *encryptionManager) dropPlanPIndexes(keysToDropMap map[string]struct{}, wg *sync.WaitGroup, errs chan error) {
	defer wg.Done()
	planPIndexesPath := filepath.Join(em.mgr.DataDir(), "planPIndexes")
	files, err := os.ReadDir(planPIndexesPath)
	if err != nil {
		log.Printf("encryptionManager: failed to read planPIndexes directory at "+
			"path %s: %v", planPIndexesPath, err)
		errs <- err
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		path := filepath.Join(planPIndexesPath, file.Name())
		err = em.reencryptFile(path, keysToDropMap)
		if err != nil {
			log.Printf("encryptionManager: failed to re-encrypt planPIndexes "+
				"file at path %s: %v", path, err)
			errs <- err
			return
		}
	}
}

func (em *encryptionManager) dropPIndex(pIndex *cbgt.PIndex, keysToDropMap map[string]struct{}) error {
	// obtain the bleve index for the pindex
	bIndex, _, _, err := bleveIndex(pIndex)
	if err != nil {
		log.Printf("encryptionManager: failed to get bleve index for "+
			"pindex %s: %v", pIndex.Name, err)
		return err
	}

	// drop the file writer ids used by the index
	if cIndex, ok := bIndex.(bleve.IndexWithCallbacks); ok {
		err := cIndex.DropFileWriterIDs(keysToDropMap)
		if err != nil {
			log.Printf("encryptionManager:failed to re-encrypt index for "+
				"pindex %s: %v", pIndex.Name, err)
			return err
		}
	}

	path := pIndex.Path
	if path == "" {
		log.Printf("encryptionManager: empty path for pindex %s, skipping "+
			"re-encryption", pIndex.Name)
		return fmt.Errorf("empty path for pindex %s", pIndex.Name)
	}

	// re-encrypt meta files associated with the pindex as well
	for _, metaFile := range []string{"PINDEX_BLEVE_META", "PINDEX_META"} {
		metaFilePath := filepath.Join(path, metaFile)
		err = em.reencryptFile(metaFilePath, keysToDropMap)
		if err != nil {
			log.Printf("encryptionManager: failed to re-encrypt "+
				"meta file %s for pindex %s: %v",
				metaFile, pIndex.Name, err)
			return err
		}
	}

	return nil
}

func (em *encryptionManager) dropWorker(jobs <-chan *cbgt.PIndex, keyData cbauth.KeyDataType, keysToDropMap map[string]struct{},
	wg *sync.WaitGroup, errs chan error) {
	for pIndex := range jobs {
		if pIndex.SourceUUID == keyData.BucketUUID {
			err := em.dropPIndex(pIndex, keysToDropMap)
			if err != nil {
				errs <- err
			}
		}
		wg.Done()
	}
}

// Not applicable to search service as we do not store keys outside rebalances
func (em *encryptionManager) synchroniseKeys(keyData cbauth.KeyDataType) error {
	return nil
}

// -------------------------- cbauth connectors for keys -----------------------

// obtain encryption keys from cbauth for the given key data
func (em *encryptionManager) getEncryptionKeys(keyData cbauth.KeyDataType) (
	*cbauth.EncrKeysInfo, error) {
	// try to get keys non blockingly if possible
	newKeyInfo, err := cbauth.GetEncryptionKeys(keyData)
	if err == cbauth.ErrKeysNotAvailable {
		// attempt to get keys blockingly with a timeout.
		ctx, cancel := context.WithTimeout(context.Background(), getKeyTimeout)
		defer cancel()
		newKeyInfo, err = cbauth.GetEncryptionKeysBlocking(ctx, keyData)
	}
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to get encryption keys "+
			"for key data %v: %w", keyData, err)
	}

	return newKeyInfo, nil
}

// obtain the active key for the given bucket and key type
func (em *encryptionManager) getKey(keyType, bucketUUID string) (*cbauthimpl.EaRKey, error) {
	keyInfo, err := em.keyStore.getKeyInfo(keyType, bucketUUID)
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to get key info "+
			"for bucket %s: %w", bucketUUID, err)
	}

	// if there is no active key for the bucket, return nil indicating that encryption
	// is not enabled for the bucket
	if keyInfo.ActiveKeyId == "" {
		return nil, nil
	}

	// Return the active key for the bucket
	for _, key := range keyInfo.Keys {
		if key.Id == keyInfo.ActiveKeyId {
			return &key, nil
		}
	}

	return nil, fmt.Errorf("encryptionManager: active key not found for "+
		"bucket: %s", bucketUUID)
}

// obtain the key with the given ID for the given bucket and key type
func (em *encryptionManager) getKeyById(keyType, bucketUUID string, keyId string) (*cbauthimpl.EaRKey, error) {
	// if keyId is empty, return nil indicating that encryption is not enabled for the bucket
	if keyId == "" {
		return nil, nil
	}

	keyInfo, err := em.keyStore.getKeyInfo(keyType, bucketUUID)
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to get key info for "+
			"bucket %s: %w", bucketUUID, err)
	}

	for _, key := range keyInfo.Keys {
		if key.Id == keyId {
			return &key, nil
		}
	}

	return nil, fmt.Errorf("encryptionManager: key with ID %s not found for "+
		"bucket %s", keyId, bucketUUID)
}

// -------------------------- bleve hooks -------------------------------

// hook used by bleve to obtain a writer callback for encrypting data.
// The callback bucket name and context from the path provided
func (em *encryptionManager) bleveWriterHook(path []byte) (
	string, func(data []byte) []byte, error) {
	if path == nil || len(path) == 0 {
		return "", emptyWriterCallback(), nil
	}

	// process the path to obtain the key type, bucket name and context for encryption
	keyType, bucketName, context, err := processPath(string(path))
	if err != nil {
		return "", nil, err
	}

	// obtain the bucket UUID for the bucket name
	bucketUUID, err := em.bucketStore.getUUIDforBucket(bucketName)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get bucket UUID for bucket %s: %w",
			bucketName, err)
	}

	// create and return the writer callback for encrypting data with the obtained key type, bucket UUID and context
	return em.newWriter(keyType, bucketUUID, context)
}

// hook used by bleve to obtain a reader callback for decrypting data. The callback
// obtains the bucket name and context from the path provided
func (em *encryptionManager) bleveReaderHook(keyId string, path []byte) (
	func(data []byte) ([]byte, error), error) {
	// if the path or keyId is empty, return a reader callback that returns the data as is without decryption
	if path == nil || len(path) == 0 || keyId == "" {
		return emptyReaderCallback(), nil
	}

	// process the path to obtain the key type, bucket name and context for decryption
	keyType, bucketName, context, err := processPath(string(path))
	if err != nil {
		return nil, err
	}

	// obtain the bucket UUID for the bucket name
	bucketUUID, err := em.bucketStore.getUUIDforBucket(bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket UUID for bucket %s: %w",
			bucketName, err)
	}

	// create and return the reader callback for decrypting data with the obtained key type, bucket UUID, key ID and context
	return em.newReader(keyType, bucketUUID, keyId, context)
}

// -------------------------- reader and writer callback creators -----------------------

// generate a cipher block for AES-256-GCM encryption using the provided key and context
func (em *encryptionManager) generateAES256GCMCipherBlock(key []byte,
	context string) (cipher.AEAD, error) {
	// derive a 256-bit key from the provided key and context using OpenSSL's KBKDF
	derivedKey := make([]byte, 32)
	derivedKey, err := gocbcrypto.OpenSSLKBKDFDeriveKey(key, searchEncryptionContext,
		[]byte(context), derivedKey, "SHA2-256", "")
	if err != nil {
		return nil, err
	}

	// create a new AES cipher block using the derived key
	block, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to create AES "+
			"cipher: %w", err)
	}

	// create a new GCM cipher mode instance using the AES block cipher
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encryptionManager: failed to create AES "+
			"GCM: %w", err)
	}

	return aesgcm, nil
}

// make a writer callback for the given cipher, key and context
func (em *encryptionManager) makeWriterCallback(cipher string, key []byte,
	context string) (func(data []byte) []byte, error) {

	switch cipher {
	case aes256gcmCipher:
		// generate a cipher block for AES-256-GCM encryption using the provided key and context
		aesgcm, err := em.generateAES256GCMCipherBlock(key, context)
		if err != nil {
			return nil, err
		}

		// generate a random nonce for encryption. The nonce will be
		// incremented for each encryption operation ensuring uniqueness
		nonce := make([]byte, noncelength)
		if _, err := rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("failed to generate random nonce: %w", err)
		}

		writerCallback := func(data []byte) []byte {
			// increment the nonce for each encryption operation to ensure uniqueness
			for i := len(nonce) - 1; i >= 0; i-- {
				if nonce[i] < 255 {
					nonce[i]++
					break
				}
				nonce[i] = 0
			}
			// encrypt the data using AES-256-GCM with the generated nonce
			ciphertext := aesgcm.Seal(nil, nonce, data, nil)
			// append the nonce to the end of the ciphertext for use in decryption
			result := append(ciphertext, nonce...)
			return result
		}

		return writerCallback, nil
	default:
		return func(data []byte) []byte {
			return data
		}, nil
	}
}

// make a reader callback for the given cipher, key and context
func (em *encryptionManager) makeReaderCallback(cipher string, key []byte,
	context string) (func(data []byte) ([]byte, error), error) {

	switch cipher {
	case aes256gcmCipher:
		// generate a cipher block for AES-256-GCM encryption using the provided key and context
		aesgcm, err := em.generateAES256GCMCipherBlock(key, context)
		if err != nil {
			return nil, err
		}

		readerCallback := func(data []byte) ([]byte, error) {
			if len(data) < noncelength {
				return nil, fmt.Errorf("ciphertext too short")
			}

			// extract the nonce from the end of the data
			nonce := data[len(data)-noncelength:]
			ciphertext := data[:len(data)-noncelength]

			// decrypt the data using AES-256-GCM with the extracted nonce
			plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to decrypt data: %w", err)
			}

			return plaintext, nil
		}

		return readerCallback, nil
	default:
		return func(data []byte) ([]byte, error) {
			return data, nil
		}, nil
	}
}

// ------------------------- key store implementation -----------------------

const keysInUseCacheFrequency = time.Hour

// keyStore is responsible for managing encryption keys.
// Includes caching master keys, obtaining current active keys,
// searching keys by ids, maintaining a cache for all keys
// currently in use and monitoring for changes in active keys.
type keyStore struct {
	mgr *cbgt.Manager

	// store of all master keys, keyed by keyType+bucketUUID
	keysLock sync.RWMutex
	keys     map[string]*cbauth.EncrKeysInfo

	// cache of keys currently in use, keyed by keyType+bucketUUID.
	// recompiled every keysInUseCacheFrequency duration by scanning
	// all disk files for keys in use
	keysInUseCache atomic.Pointer[map[string]map[string]struct{}]

	// cache of keys distributed since last keys in use cache refresh,
	// keyed by keyType+bucketUUID. This is needed to ensure that keys
	// distributed after the last cache refresh are included in the keys
	// in use until the next refresh happens.
	newKeysLock sync.RWMutex
	newKeys     map[string]map[string]struct{}
}

// Initializes the key store and starts the monitor for active keys.
func newKeyStore(mgr *cbgt.Manager) *keyStore {
	ks := &keyStore{
		keys:    make(map[string]*cbauth.EncrKeysInfo),
		newKeys: make(map[string]map[string]struct{}),
		mgr:     mgr,
	}
	ks.keysInUseCache.Store(&map[string]map[string]struct{}{})

	go ks.startMonitor()

	return ks
}

// obtain the key info for the given key type and bucket UUID.
// get key from cbauth on cache miss and update the cache.
func (ks *keyStore) getKeyInfo(keyType, bucketUUID string) (*cbauth.EncrKeysInfo,
	error) {
	var keyInfo *cbauth.EncrKeysInfo
	// check cache first for key information
	ks.keysLock.RLock()
	keyInfo = ks.keys[keyType+bucketUUID]
	ks.keysLock.RUnlock()

	// if not present, obtain key information from cbauth
	if keyInfo == nil {
		keyData := cbauth.KeyDataType{
			TypeName:   keyType,
			BucketUUID: bucketUUID,
		}

		// create a context with timeout to ensure a non infinite wait time
		ctx, cancel := context.WithTimeout(context.Background(), getKeyTimeout)
		defer cancel()

		// block and wait for cbauth to return the keys. Failure to obtain keys
		// is a critical error and may cause operations to fail
		var err error
		keyInfo, err = cbauth.GetEncryptionKeysBlocking(ctx, keyData)
		if err != nil {
			return nil, fmt.Errorf("keyStore: failed to get encryption keys for bucket %s: %w",
				bucketUUID, err)
		}

		// update the cache with the new key information
		ks.updateMasterKey(bucketUUID, keyType, keyInfo)
	}

	return keyInfo, nil
}

// set the new key in the newKeys cache to ensure it is included in the keys in use
func (ks *keyStore) setNewKey(keyType, bucketUUID string, keyId string) {
	ks.newKeysLock.Lock()
	if _, exists := ks.newKeys[keyType+bucketUUID]; !exists {
		ks.newKeys[keyType+bucketUUID] = make(map[string]struct{})
	}
	ks.newKeys[keyType+bucketUUID][keyId] = struct{}{}
	ks.newKeysLock.Unlock()
}

// starts a monitor that refreshes the keys in use cache at a regular interval.
func (ks *keyStore) startMonitor() {
	// monitor is paused until initial load is done to
	// avoid improper cache updates when keys are loaded
	// on demand during booting
	ks.waitPreLoad()
	ks.refreshKeysInUseCache()

	// refresh keys in use cache at a regular interval to
	// ensure correctness
	ticker := time.NewTicker(keysInUseCacheFrequency)
	defer ticker.Stop()

	// runs forever, regardless of whether encryption is enabled
	for {
		select {
		case <-ticker.C:
			ks.refreshKeysInUseCache()
		}
	}
}

// waits for manager to finish loading data dir before starting monitor
// ensuring all pindexes are registered
func (ks *keyStore) waitPreLoad() {
	// initialize a ticker with an arbitrary frequency
	// to check if loading is done
	preLoadTicker := time.NewTicker(5 * time.Second)
	defer preLoadTicker.Stop()

	// wait until loading is done before starting the monitor to avoid
	// improper cache updates when keys are loaded on demand during booting
	for {
		select {
		case <-preLoadTicker.C:
			if ks.mgr != nil && ks.mgr.BootingDone() {
				return
			}
		}
	}
}

// refreshes keys in use cache by scanning all disk files and then
// combining with any new keys distributed since last refresh to
// ensure an up to date list of keys in use.
func (ks *keyStore) refreshKeysInUseCache() {
	if ks.mgr == nil {
		log.Printf("keyStore: manager is not initialized, cannot refresh " +
			"keys in use cache")
		return
	}

	// if booting is not done, we still refresh the cache, but
	// we will be unable to accuratly judge if the keys in use
	// are present since pindexes are still being loaded and registered
	if !ks.mgr.BootingDone() {
		log.Printf("keyStore: Load in progress, keys in use cache " +
			"update skipped")
	}

	// get the current list of pindexes
	curKeyMap := make(map[string]map[string]struct{})
	_, pIndexes := ks.mgr.CurrentMaps()

	// obtain keys in use from all pindexes as well as their meta files
	// all errors are logged and skipped to ensure keys refresh still completes
	for _, pIndex := range pIndexes {
		if _, exists := curKeyMap[bucketKeyType+pIndex.SourceUUID]; !exists {
			curKeyMap[bucketKeyType+pIndex.SourceUUID] = make(map[string]struct{})
		}
		bIndex, _, _, err := bleveIndex(pIndex)
		if err != nil {
			log.Printf("keyStore: failed to get bleve index for pindex %s: %v",
				pIndex.Name, err)
			continue
		}

		// obtain keys in use from the bleve index
		if cIndex, ok := bIndex.(bleve.IndexWithCallbacks); ok {
			keysInUse, err := cIndex.FileWriterIDsInUse()
			if err != nil {
				log.Printf("keyStore: failed to get writer ids in use for pindex %s: %v",
					pIndex.Name, err)
				continue
			}

			for key := range keysInUse {
				curKeyMap[bucketKeyType+pIndex.SourceUUID][key] = struct{}{}
			}
		}

		path := pIndex.Path
		if path == "" {
			log.Printf("keyStore: empty path for pindex %s, skipping keys "+
				"in use cache update", pIndex.Name)
			continue
		}

		// obtain keys in use from the meta files not tracked by bleve
		for _, metaFile := range []string{"PINDEX_BLEVE_META", "PINDEX_META"} {
			pathMeta := filepath.Join(path, metaFile)
			_, metaId, err := encryptionManagerInstance.decryptAndReadFile(pathMeta)
			if err != nil {
				log.Printf("keyStore: failed to read meta file %s for pindex %s: %v",
					metaFile, pIndex.Name, err)
				continue
			}

			curKeyMap[bucketKeyType+pIndex.SourceUUID][metaId] = struct{}{}
		}
	}

	if curKeyMap[otherKeyType] == nil {
		curKeyMap[otherKeyType] = make(map[string]struct{})
	}

	// also obtain keys in use from other on disk files like recovery plans
	planPIndexesPath := filepath.Join(ks.mgr.DataDir(), "planPIndexes")
	files, err := os.ReadDir(planPIndexesPath)
	if err == nil {
		for _, file := range files {
			path := filepath.Join(planPIndexesPath, file.Name())
			_, metaId, err := encryptionManagerInstance.decryptAndReadFile(path)
			if err != nil {
				log.Printf("keyStore: failed to read planPIndexes meta file at path %s: %v",
					path, err)
				continue
			}

			curKeyMap[otherKeyType][metaId] = struct{}{}
		}
	}

	// combine keys obtained with all keys distributed since the last refresh
	// to ensure correctness of the cache
	ks.newKeysLock.Lock()
	for identifier, keys := range ks.newKeys {
		if _, exists := curKeyMap[identifier]; !exists {
			curKeyMap[identifier] = make(map[string]struct{})
		}
		for key := range keys {
			curKeyMap[identifier][key] = struct{}{}
		}
	}
	// reset the new keys cache after consuming to avoid unbounded growth
	ks.newKeys = make(map[string]map[string]struct{})
	ks.newKeysLock.Unlock()

	// update the keys in use cache with the new list of keys in use
	ks.keysInUseCache.Store(&curKeyMap)
}

// quick function to combine keys from newKeys cache and current keys in use cache
// used by cbauth callback for quick but accurate retrieval of keys in use
func (ks *keyStore) getKeysInUse(keyType, bucketUUID string) (map[string]struct{}, error) {
	if ks.mgr == nil {
		return nil, fmt.Errorf("keyStore: manager is not initialized, cannot " +
			"get keys in use")
	}

	// if booting is not done, we will be unable to accuratly judge if the keys in use
	// are present since pindexes are still being loaded and registered
	// user is expected to retry after some time if the information is still needed
	if !ks.mgr.BootingDone() {
		return nil, fmt.Errorf("keyStore: data dir load in progress, keys in " +
			"use retrieval might be inaccurate")
	}

	// combine keys from the new keys cache and the current keys in use cache to
	// ensure an up to date list of keys in use
	keysMap := make(map[string]struct{})
	ks.newKeysLock.RLock()
	newKeys := ks.newKeys[keyType+bucketUUID]
	if newKeys != nil {
		for keyId := range newKeys {
			keysMap[keyId] = struct{}{}
		}
	}
	ks.newKeysLock.RUnlock()

	keysInUse := (*ks.keysInUseCache.Load())[keyType+bucketUUID]
	if keysInUse != nil {
		for keyId := range keysInUse {
			keysMap[keyId] = struct{}{}
		}
	}

	return keysMap, nil
}

// updates the master key cache with the given key information for the specified bucket and key type.
func (ks *keyStore) updateMasterKey(bucketUUID, keyType string,
	keyInfo *cbauth.EncrKeysInfo) {
	ks.keysLock.Lock()
	ks.keys[keyType+bucketUUID] = keyInfo
	ks.keysLock.Unlock()
}

// ------------------------- bucket store implementation -----------------------

// This store can be used to map bucket names to UUIDs.
// updated only on demand when a pindex is created/deleted
type bucketStore struct {
	mgr *cbgt.Manager

	bucketLock    sync.RWMutex
	bucketUUIDMap map[string]string
}

func newBucketStore(mgr *cbgt.Manager) *bucketStore {
	return &bucketStore{
		bucketUUIDMap: make(map[string]string),
		mgr:           mgr,
	}
}

// clean disk path and obtain key type, bucket name and context for encryption
func processPath(p string) (string, string, string, error) {
	if p == "" {
		return "", "", "", fmt.Errorf("empty path provided")
	}

	// clean the path to ensure consistent processing and
	// split into components for analysis
	cleaned := path.Clean(p)
	parts := strings.Split(cleaned, "/")
	if len(parts) == 0 {
		return "", "", "", fmt.Errorf("invalid path provided: %s", p)
	}

	// process the components from the file name first to ensure we
	// do not capture system directories that are not transferrable across
	// multiple nodes in situations like rebalances
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] == "" {
			continue
		}

		// check if a pindex name can be obtained from the path component,
		// without triggering a full check of the cfg.
		pindexName, err := encryptionManagerInstance.mgr.GetPIndexName(
			parts[i], false)
		// if present, we assume this is a pindex path. key type will be
		// a bucket key and context is the filename and directories after
		// the pindex name
		if err == nil && pindexName != "" {
			// pindex names are in the format bucketName.indexType.indexUUID, so we can
			// obtain the bucket name just by splitting
			pindexParts := strings.Split(pindexName, ".")
			var bucketName string
			if len(pindexParts) < 3 {
				indexDef, _, err := encryptionManagerInstance.mgr.GetIndexDef(pindexName, false)
				if err != nil {
					return "", "", "", fmt.Errorf("invalid pindex "+
						"name extracted: %s from path: %s", pindexName, p)
				}
				bucketName = indexDef.SourceName
			} else {
				bucketName = pindexParts[0]
			}
			context := strings.Join(parts[i+1:], "/")
			// if the context ends with "temp", this is likely a temporary file created during re-encryption
			// so we trim the "temp" suffix to get the original context for encryption
			if strings.HasSuffix(context, "temp") {
				context = strings.TrimSuffix(context, "temp")
			}
			return bucketKeyType, bucketName, context, nil
		}

		// if the path component is a recover plan or a search history directory,
		// we will use the "other" key type and context is the filename and
		// the directories after the the current path component.
		if pathIsRecoveryPlan(parts[i]) || pathIsSearchHistory(parts[i]) {
			context := strings.Join(parts[i+1:], "/")
			return otherKeyType, "", context, nil
		}
	}

	return "", "", "", fmt.Errorf("no valid pindex, recovery plan, "+
		"or search history found in path: %s", p)
}

// check if the path component is the recovery plan directory
func pathIsRecoveryPlan(s string) bool {
	if s == "planPIndexes" {
		return true
	}
	return false
}

// check if the path component is the search history directory
func pathIsSearchHistory(s string) bool {
	if s == "search_history" {
		return true
	}
	return false
}

// get the uuid for bucket name
// hit pools/default endpoint on cache miss to get an updated bucket list
func (bs *bucketStore) getUUIDforBucket(bucketName string) (string, error) {
	if bucketName == "" {
		return "", nil
	}

	// check cache first for bucket UUID
	bs.bucketLock.RLock()
	uuid := bs.bucketUUIDMap[bucketName]
	bs.bucketLock.RUnlock()

	// if not present, hit pools/default to get an updated bucket list
	// and refresh the cache
	if uuid == "" {
		err := bs.refreshBucketUUIDMap()
		if err != nil {
			return "", err
		}

		bs.bucketLock.RLock()
		uuid = bs.bucketUUIDMap[bucketName]
		bs.bucketLock.RUnlock()

		if uuid == "" {
			return "", fmt.Errorf("bucketStore: bucket UUID not found "+
				"for bucket name: %s",
				bucketName)
		}
	}
	return uuid, nil
}

// refreshes bucket name to UUID map by hitting pools/default endpoint and parsing
// the bucket list. This is called on cache miss when trying to obtain UUID for
// a bucket name. It is also called when a pindex is created to ensure that the
// new bucket is included in the map.
func (bs *bucketStore) refreshBucketUUIDMap() error {
	url := bs.mgr.Server() + "/pools/default/buckets"
	u, err := cbgt.CBAuthURL(url)
	if err != nil {
		return fmt.Errorf("bucketStore: failed to parse URL %s: %w", url, err)
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return fmt.Errorf("bucketStore: failed to create request for "+
			"bucket info: %w", err)
	}

	resp, err := cbgt.HttpClient().Do(req)
	if err != nil {
		return fmt.Errorf("bucketStore: failed to get bucket info: %w", err)
	}
	defer resp.Body.Close()

	respBuf, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("bucketStore: failed to read bucket info "+
			"response body: %w", err)
	}
	if len(respBuf) == 0 {
		return fmt.Errorf("bucketStore: empty response body when " +
			"getting bucket info")
	}

	var buckets []struct {
		Name string `json:"name"`
		UUID string `json:"uuid"`
	}

	err = UnmarshalJSON(respBuf, &buckets)
	if err != nil {
		return fmt.Errorf("bucketStore: failed to unmarshal bucket "+
			"info response: %w", err)
	}

	bucketMap := make(map[string]string)
	for _, bucket := range buckets {
		bucketMap[bucket.Name] = bucket.UUID
	}

	bs.bucketLock.Lock()
	bs.bucketUUIDMap = bucketMap
	bs.bucketLock.Unlock()

	return nil
}

// ------------------------- file io implementations ---------------------------------

// creates a new writer callback for the given key type, bucket UUID and context.
func (em *encryptionManager) newWriter(keyType, bucketUUID string, context string) (
	string, func(data []byte) []byte, error) {

	key, err := em.getKey(keyType, bucketUUID)
	if err != nil {
		return "", nil, err
	}

	// if no key is present for the bucket and key type, we return an empty writer callback
	// that does not modify the data, and an empty key ID. This allows files to be written
	// without encryption if no key is configured, while still supporting encryption for buckets
	// that have keys configured.
	if key == nil {
		return "", emptyWriterCallback(), nil
	}

	// send a call to the key store for every key being used for creating callbacks
	em.keyStore.setNewKey(keyType, bucketUUID, key.Id)

	// create a writer callback with the key material and the context
	callback, err := em.makeWriterCallback(key.Cipher, []byte(key.Key), context)
	if err != nil {
		return "", nil, err
	}

	return key.Id, callback, nil
}

// returns a writer callback that does not modify the data
func emptyWriterCallback() func(data []byte) []byte {
	return func(data []byte) []byte {
		return data
	}
}

// writes the given data to the given path with encryption, and returns the key ID used for encryption
func (em *encryptionManager) encryptAndWriteFile(path string, data []byte, perm os.FileMode) (string, error) {
	// process the path to obtain the key type, bucket name and context for encryption
	keyType, bucket, context, err := processPath(path)
	if err != nil {
		return "", fmt.Errorf(
			"encryptionManager: failed to process path for encryption: %w", err)
	}

	// get the bucket UUID for the bucket name
	bucketUUID, err := encryptionManagerInstance.bucketStore.getUUIDforBucket(bucket)
	if err != nil {
		return "", fmt.Errorf(
			"encryptionManager: failed to get bucket UUID for bucket %s: %w",
			bucket, err)
	}

	// create a writer callback for the key type, bucket UUID and context
	id, writerCallback, err := encryptionManagerInstance.newWriter(
		keyType, bucketUUID, context)
	if err != nil {
		return "", fmt.Errorf(
			"encryptionManager: failed to create writer callback: %w", err)
	}

	// use the writer callback to encrypt the data, and append the key ID and its
	// length to the end of the file for decryption later
	data = writerCallback(data)

	// if the id is not empty, we append the key ID and its length to the end of the
	// file data to preserve json format
	if id != "" {
		data = append(data, []byte(id)...)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(id)))
		data = append(data, lenBuf[:]...)
	}

	// write the encrypted data to the file
	err = os.WriteFile(path, data, perm)
	if err != nil {
		return "", fmt.Errorf(
			"encryptionManager: failed to write file with encryption: %w", err)
	}

	return id, nil
}

// returns a writer callback for the given path by processing the path to obtain the key type, bucket name and context,
// and creating a writer callback with the corresponding key material and context.
func getWriterCallbackForPath(path string) (func(data []byte) []byte, error) {
	// process the path to obtain the key type, bucket name and context for encryption
	keyType, bucket, context, err := processPath(path)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to process path for writer callback: %w", err)
	}

	// get the bucket UUID for the bucket name
	bucketUUID, err := encryptionManagerInstance.bucketStore.getUUIDforBucket(bucket)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to get bucket UUID for bucket %s: %w",
			bucket, err)
	}

	// create a writer callback for the key type, bucket UUID and context
	_, writerCallback, err := encryptionManagerInstance.newWriter(
		keyType, bucketUUID, context)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to create writer callback: %w", err)
	}

	return writerCallback, nil
}

// returns a reader callback for the given key type, bucket UUID, key ID and context by retrieving the key material
// for the key ID and creating a reader callback with the key material and context. If the key ID is empty or no key is found
// for the key ID, an empty reader callback that does not modify the data is returned.
func (em *encryptionManager) newReader(keyType, bucketUUID, keyID, context string) (
	func(data []byte) ([]byte, error), error) {
	// get the key material for the key ID from the key store
	key, err := em.getKeyById(keyType, bucketUUID, keyID)
	if err != nil {
		return nil, err
	}

	// if no key is found for the key ID, we return an empty reader callback that does not modify the data
	if key == nil {
		return emptyReaderCallback(), nil
	}

	// create a reader callback with the key material and the context
	return em.makeReaderCallback(key.Cipher, []byte(key.Key), context)
}

// returns a reader callback that does not modify the data
func emptyReaderCallback() func(data []byte) ([]byte, error) {
	return func(data []byte) ([]byte, error) {
		return data, nil
	}
}

// reads the file at the given path which may be encrypted and returns the file data
// along with the key ID used for encryption if the file is encrypted.
// If the file is not encrypted, the key ID will be empty.
func (em *encryptionManager) decryptAndReadFile(path string) ([]byte, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf(
			"encryptionManager: failed to read file with encryption: %w", err)
	}

	// first attempt to unmarshal the file data as JSON. If it succeeds,
	// we assume the file is not encrypted and return the data as is.
	var tempJSON map[string]interface{}
	err = json.Unmarshal(data, &tempJSON)
	if err == nil {
		return data, "", nil
	}

	// if unmarshalling fails, we assume the file is encrypted and attempt to process the path
	// to obtain the key type, bucket name and context for decryption
	keyType, bucket, context, err := processPath(path)
	if err != nil {
		return nil, "", fmt.Errorf(
			"encryptionManager: failed to process path for decryption: %w", err)
	}

	// the last 4 bytes of the file contain the length of the key ID,
	// and the bytes before that contain the key ID itself.
	if len(data) < 4 {
		return nil, "", fmt.Errorf(
			"encryptionManager: file data too short to contain encryption metadata")
	}
	keyIDLen := binary.BigEndian.Uint32(data[len(data)-4:])
	if len(data) < int(4+keyIDLen) {
		return nil, "", fmt.Errorf(
			"encryptionManager: file data too short to contain key ID")
	}

	// extract the key ID from the file data using the length obtained from the last 4 bytes
	keyID := string(data[len(data)-4-int(keyIDLen) : len(data)-4])
	bucketUUID, err := encryptionManagerInstance.bucketStore.getUUIDforBucket(bucket)
	if err != nil {
		return nil, "", fmt.Errorf(
			"encryptionManager: failed to get bucket UUID for bucket %s: %w",
			bucket, err)
	}

	// create a reader callback for the key type, bucket UUID, key ID and context,
	// and use it to decrypt the file data
	readerCallback, err := encryptionManagerInstance.newReader(keyType,
		bucketUUID, keyID, context)
	if err != nil {
		return nil, "", fmt.Errorf(
			"encryptionManager: failed to create reader callback: %w", err)
	}
	data, err = readerCallback(data[:len(data)-4-int(keyIDLen)])
	if err != nil {
		return nil, "", fmt.Errorf(
			"encryptionManager: failed to read file with encryption: %w", err)
	}

	// remove the temp file if it exists from a previous failed re-encryption attempt,
	// to avoid leaving around stale temp files
	tempPath := path + "temp"
	if _, err := os.Stat(tempPath); err == nil {
		err = os.Remove(tempPath)
		if err != nil {
			return nil, "", fmt.Errorf(
				"encryptionManager: failed to remove existing temp file: %w", err)
		}
	}

	return data, keyID, nil
}

// returns a reader callback for the given path by processing the path to
// obtain the key type, bucket name and context
func getReaderCallbackForPath(path string) (func(data []byte) ([]byte, error), error) {
	// process the path to obtain the key type, bucket name and context for decryption
	keyType, bucket, context, err := processPath(path)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to process path for reader callback: %w", err)
	}

	// get the bucket UUID for the bucket name
	bucketUUID, err := encryptionManagerInstance.bucketStore.getUUIDforBucket(bucket)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to get bucket UUID for bucket %s: %w",
			bucket, err)
	}

	// read the file to obtain the key ID used for encryption
	_, keyID, err := encryptionManagerInstance.decryptAndReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to read file for reader callback: %w", err)
	}

	// create a reader callback for the key type, bucket UUID, key ID and context
	readerCallback, err := encryptionManagerInstance.newReader(keyType,
		bucketUUID, keyID, context)
	if err != nil {
		return nil, fmt.Errorf(
			"encryptionManager: failed to create reader callback: %w", err)
	}

	return readerCallback, nil
}

// re-encrypts the file at the given path with new encryption keys if the file is currently
// encrypted with any of the keys in the keysToDrop list.
func (em *encryptionManager) reencryptFile(path string, keysToDropMap map[string]struct{}) error {
	// read the file to obtain the file data and the key ID used for encryption
	data, keyID, err := em.decryptAndReadFile(path)
	if err != nil {
		return fmt.Errorf(
			"encryptionManager: failed to read file for re-encryption: %w", err)
	}

	// check if the key ID used for encryption is in the keysToDrop list
	if _, ok := keysToDropMap[keyID]; !ok {
		return nil
	}

	// create a temporary file with the same data to trigger encryption with new keys when writing,
	tempPath := path + "temp"
	_, err = em.encryptAndWriteFile(tempPath, data, 0600)
	if err != nil {
		return fmt.Errorf(
			"encryptionManager: failed to write file for re-encryption: %w", err)
	}

	// replace the original file with the temporary file that was written with new encryption keys
	err = os.Rename(tempPath, path)
	if err != nil {
		return fmt.Errorf(
			"encryptionManager: failed to replace original file with re-encrypted file: %w", err)
	}

	return nil
}

// ------------------------- rebalance implementation ---------------------------------

// Suffix and directory name for encryption keys during file transfer rebalance
const keyPath = "_KEY"

// copies all relavent key deks from cbauth to a temporary location during file
// transfer rebalance.
func (em *encryptionManager) prepEncryptionKeys(primaryPath, tempPath string) error {
	_, err := os.ReadDir(primaryPath)
	if err != nil {
		return fmt.Errorf("failed to read directory at path %s: %w", primaryPath, err)
	}

	// process the primary path to obtain the key type and bucket name for which we need to prep the keys
	keyType, bucketName, _, err := processPath(primaryPath)
	if err != nil {
		return fmt.Errorf("failed to process path %s: %w", primaryPath, err)
	}

	// obtain the bucket UUID for the bucket name
	bucketUUID, err := em.bucketStore.getUUIDforBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket UUID for bucket %s: %w",
			bucketName, err)
	}

	keyData := cbauth.KeyDataType{
		TypeName:   keyType,
		BucketUUID: bucketUUID,
	}

	// get the encryption keys for the bucket from cbauth. This is to obtain the latest
	// path where the keys are stored in cbauth
	keyInfo, err := cbauth.GetEncryptionKeysBlocking(context.Background(), keyData)
	if err != nil {
		return fmt.Errorf("failed to get encryption keys for bucket %s: %w",
			bucketUUID, err)
	}

	// get the keys that are currently in use for the bucket. This is needed to ensure
	// that we only copy the keys that are in use to the temporary location for rebalance
	keysInUse, err := em.getInUseKeys(keyData)
	if err != nil {
		return fmt.Errorf("failed to get keys in use for bucket %s: %w",
			bucketUUID, err)
	}

	// create a map of keys obtained from cbauth for the bucket for easy lookup when copying the keys to the temporary location
	keysMap := make(map[string]struct{})
	for _, key := range keyInfo.Keys {
		keysMap[key.Id] = struct{}{}
	}

	// ensure that all keys in use are present in the keys obtained from cbauth for the bucket
	for _, keyId := range keysInUse {
		if _, exists := keysMap[keyId]; !exists {
			return fmt.Errorf("key with ID %s is still in use but not present in keys for bucket %s",
				keyId, bucketName)
		}
	}

	keyPath := keyInfo.Path
	destPath := tempPath

	// create the destination path for the keys in the temporary location
	err = os.MkdirAll(destPath, 0700)
	if err != nil {
		return fmt.Errorf("failed to create keys directory at path %s: %w",
			destPath, err)
	}

	// Iterate through all keys in the key path and move only those keys
	// that are in use to the destination path. This ensures that we
	// only prep the keys that are actually needed for the pindex to function.
	dirChildren, err := os.ReadDir(keyPath)
	if err != nil {
		return fmt.Errorf("failed to read keys folder at path %s: %w", keyPath, err)
	}

	// keep track of the keys that were copied to the temporary location to ensure that all keys in use are copied
	copied := make(map[string]struct{})
	for _, child := range dirChildren {
		if !child.IsDir() {
			childName := child.Name()
			parts := strings.Split(childName, ".")
			if len(parts) == 0 {
				continue
			}

			keyId := parts[0]
			if _, exists := keysMap[keyId]; exists {
				srcChildPath := filepath.Join(keyPath, childName)
				destChildPath := filepath.Join(destPath, childName+"_KEY")

				err = copyFile(srcChildPath, destChildPath)
				if err != nil {
					return fmt.Errorf("failed to copy key file from path %s "+
						"to path %s: %w", srcChildPath, destChildPath, err)
				}
				copied[keyId] = struct{}{}
			}
		}
	}

	// ensure that all keys in use were copied to the temporary location.
	for _, keyId := range keysInUse {
		if _, exists := copied[keyId]; !exists {
			return fmt.Errorf("key with ID %s is in use but was not "+
				"copied to destination path %s", keyId, destPath)
		}
	}

	return nil
}

// after pindex transfer is complete, import the keys present in the temporary
// directory to cbauth and remove the temporary directory
func (em *encryptionManager) importEncryptionKeys(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("encryption: invalid path")
	}

	keysPath := filepath.Join(path, keyPath)
	if _, err := os.Stat(keysPath); os.IsNotExist(err) {
		return nil
	}

	dirChildren, err := os.ReadDir(keysPath)
	if err != nil {
		return fmt.Errorf("failed to read keys folder: %w", err)
	}

	// gather the paths of all the keys present in the temporary location to import them to cbauth
	keyPaths := make([]string, 0, len(dirChildren))
	for _, child := range dirChildren {
		if !child.IsDir() {
			keyPaths = append(keyPaths, filepath.Join(keysPath, child.Name()))
		}
	}

	// process the path to obtain the key type and bucket name for which we need to import the keys
	keyType, bucketName, _, err := processPath(path)
	if err != nil {
		return err
	}

	// obtain the bucket UUID for the bucket name
	bucketUUID, err := em.bucketStore.getUUIDforBucket(bucketName)
	if err != nil {
		return fmt.Errorf("failed to get bucket UUID for bucket %s: %w",
			bucketName, err)
	}

	dataType := cbauth.KeyDataType{
		TypeName:   keyType,
		BucketUUID: bucketUUID,
	}

	// import the keys present in the temporary location to cbauth for the bucket.
	// This will make the keys available for use by the pindex after rebalance is complete
	err = cbauth.ImportEncryptionKeys(keyPaths, dataType, importKeyTimeout)
	if err != nil {
		return fmt.Errorf("failed to import encryption keys: %w", err)
	}

	// remove the keys from the temporary location after importing to cbauth to clean up
	err = os.RemoveAll(keysPath)
	if err != nil {
		return fmt.Errorf("failed to remove keys directory: %w", err)
	}

	return nil
}

// helper function to copy a file from source path to destination path.
// This is used to copy the keys from cbauth to a temporary location during
// rebalance and also to copy the keys back to cbauth after rebalance is complete
func copyFile(src string, dest string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file at path %s: %w", src, err)
	}
	defer srcFile.Close()

	destFile, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("failed to create destination file at path %s: %w", dest, err)
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, srcFile)
	if err != nil {
		return fmt.Errorf("failed to copy data from path %s to path %s: %w", src, dest, err)
	}

	return nil
}

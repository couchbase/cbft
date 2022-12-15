//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package cbft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/hibernate"
	log "github.com/couchbase/clog"
	"github.com/couchbase/tools-common/fsutil"
	"github.com/couchbase/tools-common/objstore/objcli"
	"github.com/couchbase/tools-common/objstore/objcli/objaws"
	"github.com/couchbase/tools-common/objstore/objutil"
	"github.com/couchbase/tools-common/objstore/objval"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func GetS3Client() (objcli.Client, error) {
	session, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, fmt.Errorf("s3_utils: error creating session: %v", err)
	}

	client := objaws.NewClient(objaws.ClientOptions{ServiceAPI: s3.New(session)})
	return client, nil
}

func CheckIfRemotePathIsValid(remotePath string) bool {
	return strings.HasPrefix(remotePath, "s3://")
}

// TODO USE tools' lib std function -
// https://github.com/couchbase/tools-common/blob/master/objstore/objutil/url.go#L114
// A sample remote path is of the form:
// s3://<s3-bucket-name>/fts/<path to file>
// This function aims to extract the S3 bucket name and path after the bucket name.
func GetRemoteBucketAndPathHook(remotePath string) (string, string, error) {
	split := strings.SplitN(remotePath, ":", 2)
	if len(split) < 2 {
		return "", "", fmt.Errorf("s3_utils: malformed path")
	}
	s3Path := split[1]
	split = strings.SplitAfterN(s3Path, "/", 4)
	if len(split) < 3 {
		return "", "", fmt.Errorf("s3_utils: malformed path")
	}
	// returning the bucket name
	return split[2][0 : len(split[2])-1], split[3], nil
}

func downloadFromBucket(c objcli.Client, bucket, prefix, pindexPath string,
	copyStats *CopyPartitionStats, ctx context.Context) error {

	atomic.AddInt32(&copyStats.TotCopyPartitionStart, 1)

	fn := func(attrs *objval.ObjectAttrs) error {
		if strings.HasPrefix(attrs.Key, prefix) {
			startTime := time.Now()

			split := strings.Split(attrs.Key, "/")

			dirKey := strings.Join(split[2:len(split)-1], "/")
			folderToBeCreated := pindexPath + string(os.PathSeparator) + dirKey
			err := fsutil.Mkdir(folderToBeCreated, os.ModePerm, true, true)
			if err != nil {
				return fmt.Errorf("s3_utils: failed to create folder '%s': %w", folderToBeCreated, err)
			}

			fileKey := strings.Join(split[2:], "/") // skip the pindex folder name.
			filename := pindexPath + "/" + fileKey
			log.Printf("s3_utils: downloading file: %s", filename)
			file, err := os.Create(filename)
			if err != nil {
				return err
			}
			defer file.Close()

			options := objutil.DownloadOptions{
				Client:  c,
				Bucket:  bucket,
				Key:     attrs.Key,
				Writer:  file,
				Options: objutil.Options{Context: ctx},
			}
			err = objutil.Download(options)
			if err != nil {
				var awsErr awserr.Error
				if errors.As(err, &awsErr) {
					return fmt.Errorf("s3_utils: download error(AWS error) is :%s", awsErr.Message())
				}
				return err
			}

			downloadDuration := time.Since(startTime)
			atomic.AddInt32(&copyStats.TotCopyPartitionTimeInMs,
				int32(downloadDuration.Milliseconds()))

			atomic.AddInt32(&copyStats.CopyPartitionNumBytesReceived, int32(attrs.Size))
		}
		return nil
	}

	err := c.IterateObjects(context.Background(), bucket, prefix, "", nil, nil, fn)
	if err != nil {
		atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
		return err
	}

	log.Printf("s3_utils: done downloading the files for path %s", pindexPath)
	atomic.AddInt32(&copyStats.TotCopyPartitionFinished, 1)

	return nil
}

func getS3BucketSize(c objcli.Client, bucket, prefix string) (int64, error) {
	var bucketSize int64
	fn := func(attrs *objval.ObjectAttrs) error {
		bucketSize += attrs.Size
		return nil
	}

	err := c.IterateObjects(context.Background(), bucket, prefix, "", nil, nil, fn)
	if err != nil {
		return 0, fmt.Errorf("s3_utils: failed to iterate objects: %w", err)
	}

	return bucketSize, nil
}

// This function returns the S3 bucket and path for index metadata.
// The remote path is of the form: s3://<s3-bucket-name>/<key>
func getBucketAndMetadataPath(remotePath string) (string, string, error) {
	bucket, key, err := GetRemoteBucketAndPathHook(remotePath)
	if err != nil {
		return "", "", err
	}
	key = key + "/" + hibernate.INDEX_METADATA_PATH

	return bucket, key, nil
}

func DownloadIndexMetadata(client objcli.Client, remotePath string) (
	*cbgt.IndexDefs, error) {

	// Ref : https://stackoverflow.com/questions/46019484/buffer-implementing-io-writerat-in-go
	buf := aws.NewWriteAtBuffer([]byte{})
	bucket, indexMetadataPath, err := getBucketAndMetadataPath(remotePath)
	if err != nil {
		return nil, err
	}
	options := objutil.DownloadOptions{
		Client: client,
		Bucket: bucket,
		Key:    indexMetadataPath,
		Writer: buf,
	}
	err = objutil.Download(options)
	if err != nil {
		return nil, err
	}
	indexDefs := new(cbgt.IndexDefs)
	err = json.Unmarshal(buf.Bytes(), indexDefs)
	if err != nil {
		return nil, err
	}
	return indexDefs, err
}

func UploadIndexDefs(client objcli.Client, ctx context.Context, data []byte,
	remotePath string) error {

	// Upload a file without saving it - S3
	// Ref: https://stackoverflow.com/questions/47621804/upload-object-to-aws-s3-without-creating-a-file-using-aws-sdk-go
	reader := strings.NewReader(string(data))

	bucket, uploadPath, err := getBucketAndMetadataPath(remotePath)
	if err != nil {
		return err
	}

	options := objutil.UploadOptions{
		Client:  client,
		Bucket:  bucket,
		Key:     uploadPath,
		Body:    reader,
		Options: objutil.Options{Context: ctx},
	}
	log.Printf("s3_utils: uploading index metadata to path %s", uploadPath)
	err = objutil.Upload(options)
	var awsErr awserr.Error
	if err != nil {
		if errors.As(err, &awsErr) {
			log.Errorf("s3_utils: error uploading index defs: %s", awsErr.Message())
			return fmt.Errorf("s3_utils: error uploading index defs: %s", awsErr.Message())
		}
	}
	return err
}

func uploadToBucket(mgr *cbgt.Manager, c objcli.Client, bucket, pindexPath, keyPrefix string,
	copyStats *CopyPartitionStats, ctx context.Context) {

	atomic.AddInt32(&copyStats.TotCopyPartitionStart, 1)

	err := filepath.Walk(pindexPath,
		func(path string, info os.FileInfo, err error) error {

			if info != nil && !info.IsDir() &&
				// Avoid uploading PINDEX_META files.
				!strings.HasSuffix(path, cbgt.PINDEX_META_FILENAME) {
				startTime := time.Now()

				file, err := ioutil.ReadFile(path)
				if err != nil {
					log.Errorf("s3_utils: unable to read file %s:%v",
						path, err)
					// Skips the parent dir(for a non-dir)/current dir
					// For other non-nil errors, Walk will stop entirely.
					atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
					return err
				}

				dirPath := path[len(pindexPath):]
				pindexName := cbgt.PIndexNameFromPath(pindexPath)
				key := keyPrefix + "/" + pindexName + "/" + dirPath

				log.Printf("s3_utils: uploading file: %s", path)
				options := objutil.UploadOptions{
					Client:  c,
					Bucket:  bucket,
					Key:     key,
					Body:    bytes.NewReader(file),
					Options: objutil.Options{Context: ctx},
				}

				// The default AWS retry options are provided as part of
				// the client.
				// Ref: https://github.com/aws/aws-sdk-go/blob/v1.44.96/aws/config.go#L94-L108
				err = objutil.Upload(options)
				if err != nil {
					var awsErr awserr.Error
					// update upload errors here
					if errors.As(err, &awsErr) {
						log.Errorf("s3_utils: error uploading: AWS error: %s", awsErr.Message())
						atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
						return fmt.Errorf("s3_utils: error uploading: AWS error: %s", awsErr.Message())
					}
					log.Errorf("s3_utils: error uploading: %v", err)
					atomic.AddInt32(&copyStats.TotCopyPartitionErrors, 1)
					return err
				}

				uploadDuration := time.Since(startTime)

				atomic.AddInt32(&copyStats.CopyPartitionNumBytesReceived,
					int32(info.Size()))

				atomic.AddInt32(&copyStats.TotCopyPartitionTimeInMs,
					int32(uploadDuration.Milliseconds()))
			}
			return nil
		})

	if err == nil {
		atomic.AddInt32(&copyStats.TotCopyPartitionFinished, 1)
	}
}

// Uploads the pindex directory to S3 for a hibernated index.
func uploadPIndexFiles(mgr *cbgt.Manager, client objcli.Client, remotePath,
	pindexName, path string, ctx context.Context,
	cancel context.CancelFunc) {
	_, pindexes := mgr.CurrentMaps()
	pindex, exists := pindexes[pindexName]
	if !exists {
		log.Errorf("s3_utils: pindex %s not in mgr cache", pindexName)
		return
	}

	var dest *BleveDest
	if destForwarder, ok := pindex.Dest.(*cbgt.DestForwarder); ok {
		if dest, ok = destForwarder.DestProvider.(*BleveDest); ok {
			totalObjSize, err := cbgt.GetDirectorySize(path)
			if err != nil {
				log.Errorf("s3_utils: error getting directory size: %v", err)
				atomic.AddInt32(&dest.copyStats.TotCopyPartitionErrors, 1)
				return
			}
			resetCopyStats(dest.copyStats, totalObjSize)
		} else {
			log.Errorf("s3_utils: unable to find dest and upload pindex")
			return
		}
	} else {
		log.Errorf("s3_utils: unable to find dest forwarder and upload pindex")
		return
	}

	bucket, keyPrefix, err := GetRemoteBucketAndPathHook(remotePath)
	if err != nil {
		log.Errorf("s3_utils: error getting bucket and key from remote path: %v", err)
		atomic.AddInt32(&dest.copyStats.TotCopyPartitionErrors, 1)
		return
	}

	log.Printf("s3_utils: uploading path %s", path)
	uploadToBucket(mgr, client, bucket, path, keyPrefix, dest.copyStats, ctx)
}

// Resets the copy partition stats of the pindex to the expected
// values before transfer(upload/download).
func resetCopyStats(copyStats *CopyPartitionStats, totalObjSize int64) {
	atomic.StoreInt32(&copyStats.CopyPartitionNumBytesExpected, int32(totalObjSize))
	// Resetting it to 0 prior to each pindex transfer.
	atomic.StoreInt32(&copyStats.CopyPartitionNumBytesReceived, 0)
	atomic.StoreInt32(&copyStats.TotCopyPartitionErrors, 0)
}
